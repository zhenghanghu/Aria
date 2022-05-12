//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "common/SpinLock.h"
#include "core/Manager.h"
#include "core/Partitioner.h"
#include "protocol/Sparkle/SparkleExecutor.h"
#include "protocol/Sparkle/SparkleHelper.h"
#include "protocol/Sparkle/SparkleTransaction.h"
#include <glog/logging.h>

#include <atomic>
#include <thread>
#include <vector>
#include <map>

namespace aria {

template <class Workload> class SparkleManager : public aria::Manager {
public:
  using base_type = aria::Manager;

  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = SparkleTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  SparkleManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
              const ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), db(db), epoch(0) {
    
    storages.resize(100001);
    transactions.resize(100001);
    NEXT_TX.store(1);

  }

public:
  RandomType random;
  DatabaseType &db;
  std::atomic<uint32_t> epoch;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
  std::atomic<uint32_t> total_abort;
  std::atomic<uint32_t> NEXT_TX;
};
} // namespace aria