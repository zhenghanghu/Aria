//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include <atomic>
#include <cstring>
#include <tuple>

#include "core/Table.h"

#include "glog/logging.h"
#include "protocol/Sparkle/SparkleRWKey.h"

namespace aria {

class SparkleHelper {

public:
  using MetaDataType = std::atomic<uint64_t>;

  static void read(const std::tuple<MetaDataType *, void *> &row,
                       void *dest, std::size_t size) {//

    MetaDataType &tid = *std::get<0>(row);
    //LOG(INFO)<<row;
    void *src = std::get<1>(row);
    //LOG(INFO)<<src;
    std::memcpy(dest, src, size);
    return;
  }
};

} // namespace aria