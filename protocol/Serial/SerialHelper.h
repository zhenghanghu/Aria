//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include <atomic>
#include <cstring>
#include <tuple>

#include "core/Table.h"

#include "glog/logging.h"
#include "protocol/Serial/SerialRWKey.h"

namespace aria {

class SerialHelper {

public:
  using MetaDataType = std::atomic<uint64_t>;

  static uint64_t read(const std::tuple<MetaDataType *, void *> &row,
                       void *dest, std::size_t size) {
    MetaDataType &tid = *std::get<0>(row);
    void *src = std::get<1>(row);
    std::memcpy(dest, src, size);
    return tid.load();
  }
};

} // namespace aria