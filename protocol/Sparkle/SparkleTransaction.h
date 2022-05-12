//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Sparkle/SparkleHelper.h"
#include "protocol/Sparkle/SparkleRWKey.h"
#include <chrono>
#include <glog/logging.h>
#include <thread>

namespace aria {

class SparkleTransaction {

public:
  using MetaDataType = std::atomic<uint64_t>;

  SparkleTransaction(std::size_t coordinator_id, std::size_t partition_id,
                  Partitioner &partitioner)
      : coordinator_id(coordinator_id), partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()), partitioner(partitioner) {
    reset();
  }

  virtual ~SparkleTransaction() = default;

  void reset() {
    abort_lock = false;
    abort_no_retry = false;
    abort_read_validation = false;
    distributed_transaction = false;
    execution_phase = false;
    pendingResponses = 0;
    network_size = 0;
    operation.clear();
    readSet.clear();
    writeSet.clear();
    abort_flag = false;
  }

  virtual TransactionResult execute(std::size_t worker_id) = 0;

  virtual void reset_query() = 0;

  template <class KeyType, class ValueType>
  void search_local_index(std::size_t table_id, std::size_t partition_id,
                          const KeyType &key, ValueType &value) {
    if (execution_phase) {
      return;
    }

    SparkleRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_local_index_read_bit();
    readKey.set_read_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_read(std::size_t table_id, std::size_t partition_id,
                       const KeyType &key, ValueType &value) {
    if (execution_phase || abort_flag ) {
      return;
    }
    SparkleRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_bit();
    readRequestHandler(readKey, 0, 0);

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value) {
    if (execution_phase || abort_flag ) {
      return;
    }
    SparkleRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_bit();
    readRequestHandler(readKey, 0, 0);

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {
    if (execution_phase || abort_flag) {
      return;
    }

    SparkleRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));

    writeRequestHandler(writeKey, 0, 0);

    add_to_write_set(writeKey);
  }

  std::size_t add_to_read_set(const SparkleRWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const SparkleRWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

  void set_id(std::size_t id) { this->id = id; }

  void set_tid_offset(std::size_t offset) { this->tid_offset = offset; }

  bool process_requests(std::size_t worker_id) { return false; }

  bool is_read_only() { return writeSet.size() == 0; }
  

    
public:
  std::size_t coordinator_id, partition_id, id, tid_offset;
  std::chrono::steady_clock::time_point startTime;
  std::size_t pendingResponses;
  std::size_t network_size;

  bool abort_lock, abort_no_retry, abort_read_validation;
  bool distributed_transaction;
  bool execution_phase;

  // read_key, id, key_offset
  std::function<void(SparkleRWKey &, std::size_t, std::size_t)> readRequestHandler;
  std::function<void(SparkleRWKey &, std::size_t, std::size_t)> writeRequestHandler;

  Partitioner &partitioner;
  Operation operation; // never used
  std::vector<SparkleRWKey> readSet, writeSet;
  bool abort_flag = false;
  pthread_mutex_t waiting_lock = PTHREAD_MUTEX_INITIALIZER;
  pthread_cond_t  waiting_cond = PTHREAD_COND_INITIALIZER;
  int waiting = 0;
};
} // namespace aria