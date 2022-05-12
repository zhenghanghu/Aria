//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "core/Partitioner.h"

#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Serial/Serial.h"
#include "protocol/Serial/SerialHelper.h"

#include <chrono>
#include <thread>

namespace aria {

template <class Workload> class SerialExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = SerialTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");

  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = Serial<DatabaseType>;

  SerialExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
               const ContextType &context,
               std::vector<std::unique_ptr<TransactionType>> &transactions,
               std::vector<StorageType> &storages, std::atomic<uint32_t> &epoch,
               std::atomic<uint32_t> &worker_status,
               std::atomic<uint32_t> &total_abort,
               std::atomic<uint32_t> &n_complete_workers,
               std::atomic<uint32_t> &n_started_workers,
               std::atomic<bool> &stopFlag)
      : Worker(coordinator_id, id), db(db), context(context),
        transactions(transactions), storages(storages), epoch(epoch),
        worker_status(worker_status), total_abort(total_abort),
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(PartitionerFactory::create_partitioner(
            context.partitioner, coordinator_id, context.coordinator_num)),
        workload(coordinator_id, db, random, *partitioner),
        random(reinterpret_cast<uint64_t>(this)),
        protocol(db, context, *partitioner),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)),
        stopFlag(stopFlag) {

  }

  ~SerialExecutor() = default;

  void push_message(Message *message) override {}
  
  Message *pop_message() override { return nullptr; }

  void start() override {

    LOG(INFO) << "SerialExecutor " << id << " started. ";

    while( !stopFlag.load() ){

      auto partition_id = get_partition_id();
      auto transaction = workload.next_transaction(context, partition_id, storages[0]);

      setupHandlers(*transaction);
      auto result = transaction->execute(id);
      n_network_size.fetch_add(transaction->network_size);
      if (result == TransactionResult::ABORT_NORETRY) {
        transaction->abort_no_retry = true;
      }

      transaction->execution_phase = true; 
      transaction->execute(id);

      if (transaction->abort_no_retry) {
        n_abort_no_retry.fetch_add(1);
        continue;
      }

      protocol.commit(*transaction);
      n_commit.fetch_add(1);
      auto latency =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - transaction->startTime)
            .count();
      percentile.add(latency);
    }

    LOG(INFO) << "SerialExecutor " << id << " exits. ";
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

    txn.readRequestHandler = [this, &txn](SerialRWKey &readKey, std::size_t tid,
                                          uint32_t key_offset) {
      auto table_id = readKey.get_table_id();
      auto partition_id = readKey.get_partition_id();
      const void *key = readKey.get_key();
      void *value = readKey.get_value();
      bool local_index_read = readKey.get_local_index_read_bit();

      bool local_read = false;

      if (this->partitioner->has_master_partition(partition_id)) {
        local_read = true;
      }

      ITable *table = db.find_table(table_id, partition_id);
      if (local_read || local_index_read) {
        // set tid meta_data
        auto row = table->search(key);
        AriaHelper::read(row, value, table->value_size());
      } 
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
  ProtocolType protocol;
  std::unique_ptr<Delay> delay;
  Percentile<int64_t> percentile;
  std::atomic<bool> &stopFlag;
};
} // namespace aria