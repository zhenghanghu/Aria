//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "core/Partitioner.h"

#include "common/SpinLock.h"
#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Sparkle/SparkleHelper.h"

#include <chrono>
#include <thread>
#include <map>

namespace aria {

template <class Workload> class SparkleExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = SparkleTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");

  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  SparkleExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
               const ContextType &context,
               std::vector<std::unique_ptr<TransactionType>> &transactions,
               std::vector<StorageType> &storages, std::atomic<uint32_t> &epoch,
               std::atomic<uint32_t> &worker_status,
               std::atomic<uint32_t> &total_abort,
               std::atomic<uint32_t> &n_complete_workers,
               std::atomic<uint32_t> &n_started_workers,
               std::atomic<bool> &stopFlag,
               std::atomic<uint32_t> &NEXT_TX)
      : Worker(coordinator_id, id), db(db), context(context),
        transactions(transactions), storages(storages), epoch(epoch),
        worker_status(worker_status), total_abort(total_abort),
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(PartitionerFactory::create_partitioner(
            context.partitioner, coordinator_id, context.coordinator_num)),
        workload(coordinator_id, db, random, *partitioner),
        random(reinterpret_cast<uint64_t>(this)),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)),
        stopFlag(stopFlag),NEXT_TX(NEXT_TX) {

  }

  ~SparkleExecutor() = default;

  void push_message(Message *message) override {}
  
  Message *pop_message() override { return nullptr; }

  void start() override {

    LOG(INFO) << "SparkleExecutor " << id << " started. ";

    bool retry = false;
    auto i = id;

    while( !stopFlag.load() ){
      
      //LOG(INFO) << i;
      

      auto partition_id = get_partition_id();
      if( !retry ) transactions[i % transactions.size() ] = workload.next_transaction(context, partition_id, storages[i % transactions.size()]);
      else {
        retry = false;
      }
      transactions[i % transactions.size() ]->set_id(i+1);//id starts from 1

      setupHandlers(*transactions[i % transactions.size() ]);


      //LOG(INFO)<<"executor "<<id<<": enter first execute ("<<transactions[i % transactions.size()]->id<<")";
      transactions[i % transactions.size()]->execute(id);
      if( transactions[i % transactions.size()]->abort_flag ){
        localAbort(*transactions[i % transactions.size()]);
        n_abort_lock.fetch_add(1);
        retry = true;
        continue;
      }
      transactions[i % transactions.size()]->execution_phase = true; 

      //LOG(INFO)<<"executor "<<id<<": enter second execute ("<<transactions[i % transactions.size()]->id<<")";
      transactions[i % transactions.size()]->execute(id);
      if( transactions[i % transactions.size()]->abort_flag ){
        localAbort(*transactions[i % transactions.size()]);
        n_abort_lock.fetch_add(1);
        retry = true;
        continue;
      }
      //LOG(INFO)<<"executor "<<id<<": enter speculative_commit ("<<transactions[i % transactions.size()]->id<<")";
      speculative_commit(*transactions[i % transactions.size()]);
      if( transactions[i % transactions.size()]->abort_flag ){
        localAbort(*transactions[i % transactions.size()]);
        n_abort_lock.fetch_add(1);
        retry = true;
        continue;
      }
      //LOG(INFO)<<"executor "<<id<<": enter finalCommit ("<<transactions[i % transactions.size()]->id<<")";

      finalCommit(*transactions[i % transactions.size()]);
      if( transactions[i % transactions.size()]->abort_flag ){
        localAbort(*transactions[i % transactions.size()]);
        n_abort_lock.fetch_add(1);
        retry = true;
        continue;
      }
      //LOG(INFO)<<"executor "<<id<<": finish finalCommit ("<<transactions[i % transactions.size()]->id<<")";

      n_commit.fetch_add(1);
      auto latency =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - transactions[i % transactions.size()]->startTime)
            .count();
      percentile.add(latency);

      i += context.worker_num;
    }

    LOG(INFO) << "SparkleExecutor " << id << " exits. ";

  }
  
  void speculative_commit(TransactionType &txn) {

    //LOG(INFO)<<"writeSet: "<<txn.writeSet.size();
    for(auto i = 0u; i < txn.writeSet.size(); i++){
      auto &writeKey = txn.writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      
      auto table = db.find_table(tableId, partitionId);
      auto key = writeKey.get_key();
      auto value = writeKey.get_value();
      if( table->addVersion(key, value, &txn)==0 ){
        txn.abort_flag = true;
        return;
      }
    }
    
  }

  bool finalCommit(TransactionType &txn) {
    //LOG(INFO)<<txn.id<<" "<<NEXT_TX.load()<<" "<<(txn.id != NEXT_TX.load());
    while(txn.id != NEXT_TX.load() && txn.abort_flag==false ){
      //wait
      //LOG(INFO)<<"!! "<<NEXT_TX.load()<<" "<<txn.id<<" "<<txn.abort_flag;
    }
    if( txn.id == NEXT_TX.load() && txn.abort_flag==false ){
      NEXT_TX.fetch_add(1);
    }
    //LOG(INFO)<<"!! "<<txn.abort_flag;

    return true;
  }

  void localAbort(TransactionType &txn){

    for(auto i = 0u; i < txn.writeSet.size(); i++){
      auto &writeKey = txn.writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      
      auto table = db.find_table(tableId, partitionId);
      auto key = writeKey.get_key();
      auto value = writeKey.get_value();
      table->UnlockAndRemove(key, &txn);
    }
    txn.reset();
    
  }

  std::size_t get_partition_id() {

    std::size_t partition_id;

    CHECK(context.partition_num % context.coordinator_num == 0);

    auto partition_num_per_node =
        context.partition_num / context.coordinator_num;
    partition_id = random.uniform_dist(0, partition_num_per_node - 1) *
                       context.coordinator_num +
                   coordinator_id;
    CHECK(partitioner->has_master_partition(partition_id));
    return partition_id;
  }

  void setupHandlers(TransactionType &txn) {

    txn.readRequestHandler = [this, &txn](SparkleRWKey &readKey, std::size_t tid,
                                          uint32_t key_offset) {
      //LOG(INFO) << "read start";
      auto table_id = readKey.get_table_id();
      auto partition_id = readKey.get_partition_id();
      const void *key = readKey.get_key();
      void *value = readKey.get_value();

      ITable *table = db.find_table(table_id, partition_id);

      auto row = table->read(key, &txn);
      auto *ptr = std::get<0>(row);
      while( ptr == nullptr ){
    
        pthread_mutex_lock( &txn.waiting_lock );
        while( txn.waiting == 1){
          pthread_cond_wait( &txn.waiting_cond, &txn.waiting_lock);
        }
        pthread_mutex_unlock( &txn.waiting_lock );

        row = table->read(key, &txn);
        ptr = std::get<0>(row);
      }

      SparkleHelper::read(row, value, table->value_size());
      //LOG(INFO) << "read end";
      
    };

    txn.writeRequestHandler = [this, &txn](SparkleRWKey &writeKey, std::size_t tid,
                                          uint32_t key_offset) {

      //LOG(INFO) << "write start";
      auto table_id = writeKey.get_table_id();
      auto partition_id = writeKey.get_partition_id();
      const void *key = writeKey.get_key();
      void *value = writeKey.get_value();

      ITable *table = db.find_table(table_id, partition_id);
      
      while( table->lock(key, &txn)==0 ){
        
        pthread_mutex_lock( &txn.waiting_lock );
        while( txn.waiting == 1){
          pthread_cond_wait( &txn.waiting_cond, &txn.waiting_lock);
        }
        pthread_mutex_unlock( &txn.waiting_lock );

      }
      //LOG(INFO) << "write end";
    };

  }

  void onExit() override {
    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
              << " us (50%) " << percentile.nth(75) << " us (75%) "
              << percentile.nth(95) << " us (95%) " << percentile.nth(99)
              << " us (99%).";
  }

private:
  DatabaseType &db;
  const ContextType &context;
  std::vector<std::unique_ptr<TransactionType>> &transactions;
  std::vector<StorageType> &storages;
  std::atomic<uint32_t> &epoch, &worker_status, &total_abort;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  std::unique_ptr<Partitioner> partitioner;
  WorkloadType workload;
  RandomType random;
  std::unique_ptr<Delay> delay;
  Percentile<int64_t> percentile;
  std::atomic<bool> &stopFlag;
  std::atomic<uint32_t> &NEXT_TX;
};
} // namespace aria