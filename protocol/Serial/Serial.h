//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Serial/SerialHelper.h"
#include "protocol/Serial/SerialTransaction.h"

namespace aria {

template <class Database> class Serial {
public:
  using DatabaseType = Database;
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = typename DatabaseType::ContextType;
  using TransactionType = SerialTransaction;

  Serial(DatabaseType &db, const ContextType &context, Partitioner &partitioner)
      : db(db), context(context), partitioner(partitioner) {}

  void abort(TransactionType &txn) {
    // nothing needs to be done
  }

  bool commit(TransactionType &txn) {

    auto &writeSet = txn.writeSet;
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (partitioner.has_master_partition(partitionId)) {
        auto key = writeKey.get_key();
        auto value = writeKey.get_value();
        table->update(key, value);
      } 
    }

    return true;
  }

private:
  DatabaseType &db;
  const ContextType &context;
  Partitioner &partitioner;
};
} // namespace aria