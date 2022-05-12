//
// Created by Yi Lu on 2019-09-02.
//

#pragma once

#include "SpinLock.h"
#include <atomic>
#include <glog/logging.h>
#include <list>
#include <unordered_map>
#include <vector>
#include "protocol/Sparkle/SparkleTransaction.h"
#include <pthread.h>

namespace aria {

/*
 *  MVCC Hash Map -- overview --
 *
 *  KeyType -> std::list<std::tuple<uint64_t, ValueType>>,
 *  ValueType: std::tuple<MetaDataType, ycsb::value>
 *  uint64_t: version
 * 
 *  By default, the first node is a sentinel node, then comes the newest version
 * (the largest value). The upper application (e.g., worker thread) is
 * responsible for data vacuum. Given a vacuum_version, all versions less than
 * or equal to vacuum_version will be garbage collected.
 */

/* New MVCC Hash Map for Sparkle
 *
 * KeyType: ycsb:key
 * ValueType: std::tuple<MetaDataType, ycsb::value>
 *
 * ycsb:key -> std::tuple< MetaDataSparkle , std::list<std::tuple<uint64_t, ValueType>> >
 * 
 */

class MetaDataSparkle {
  public: 
    SparkleTransaction *LOCK_TX=nullptr;
    pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
    std::vector<SparkleTransaction*> WAIT_TXS;
    std::vector< std::tuple<SparkleTransaction*,uint64_t> > READ_DEPS;
};

template <std::size_t N, class KeyType, class ValueType> class MVCCHashMap {
public:
  using VersionTupleType = std::tuple<uint64_t, ValueType>;
  using MappedValueType = std::tuple<MetaDataSparkle, std::list<VersionTupleType>>;
  using HashMapType = std::unordered_map<KeyType, MappedValueType>;
  using HasherType = typename HashMapType::hasher;

  // if a particular key exists.
  bool contains_key(const KeyType &key) {
    return apply(
        [&key](HashMapType &map) {
          auto it = map.find(key);

          if (it == map.end()) {
            return false;
          }

          // check if the list is empty
          auto &l = std::get<1>(it->second);
          return !l.empty();
        },
        bucket_number(key));
  }

  // if a particular key with a specific version exists.
  bool contains_key_version(const KeyType &key, uint64_t version) {
    return apply(
        [&key, version](HashMapType &map) {
          auto it = map.find(key);

          if (it == map.end()) {
            return false;
          }

          auto &l = std::get<1>(it->second);
          for (VersionTupleType &vt : l) {
            if (get_version(vt) == version) {
              return true;
            }
          }
          return false;
        },
        bucket_number(key));
  }

  // remove a particular key.
  bool remove_key(const KeyType &key) {
    return apply(
        [&key](HashMapType &map) {
          auto it = map.find(key);

          if (it == map.end()) {
            return false;
          }
          map.erase(it);
          return true;
        },
        bucket_number(key));
  }

  // remove a particular key with a specific version.
  bool remove_key_version(const KeyType &key, uint64_t version) {
    return apply(
        [&key, version](HashMapType &map) {
          auto it = map.find(key);
          if (it == map.end()) {
            return false;
          }
          auto &l = std::get<1>(it->second);

          for (auto lit = l.begin(); lit != l.end(); lit++) {
            if (get_version(*lit) == version) {
              l.erase(lit);
              return true;
            }
          }
          return false;
        },
        bucket_number(key));
  }

  // insert a key with a specific version placeholder and return the reference
  ValueType &insert_key_version_holder(const KeyType &key, uint64_t version) {
    return apply_ref(
        [&key, version](HashMapType &map) -> ValueType & {
          auto &l = std::get<1>(map[key]);
          auto lit = l.begin();

          // always insert to the front if the list is empty
          if (l.empty()) {
            l.emplace_front();
            lit = l.begin();
          } else {
            // make sure the versions are always monotonically decreasing
            for ( ; lit != l.end(); lit++) {
              if (get_version(*lit) < version) {
                lit = l.emplace(lit);
                break;
              }
            }
          }
          // set the version
          std::get<0>(*lit) = version;
          // std::get<0> returns the version
          return std::get<1>(*lit);
        },
        bucket_number(key));
  }

  // return the number of versions of a particular key
  std::size_t version_count(const KeyType &key) {
    return apply(
        [&key](HashMapType &map) -> std::size_t {
          auto it = map.find(key);
          if (it == map.end()) {
            return 0;
          } else {
            auto &l = std::get<1>(it->second);
            return l.size();
          }
        },
        bucket_number(key));
  }

  // return the value of a particular key and a specific version
  // nullptr if not exists.
  ValueType *get_key_version(const KeyType &key, uint64_t version) {
    return apply(
        [&key, version](HashMapType &map) -> ValueType * {
          auto it = map.find(key);
          if (it == map.end()) {
            return nullptr;
          }
          auto &l = std::get<1>(it->second);
          for (VersionTupleType &vt : l) {
            if (get_version(vt) == version) {
              return &get_value(vt);
            }
          }
          return nullptr;
        },
        bucket_number(key));
  }
  // return the value of a particular key and the version older than the
  // specific version nullptr if not exists.
  VersionTupleType *get_key_version_prev(const KeyType &key, uint64_t version) {
    return apply(
        [&key, version](HashMapType &map) -> VersionTupleType * {
          auto it = map.find(key);
          if (it == map.end()) {
            return nullptr;
          }
          auto &l = std::get<1>(it->second);

          for (VersionTupleType &vt : l) {

            if (get_version(vt) < version) {

              return &vt;
            }
          }
          return nullptr;
        },
        bucket_number(key));
  }
  //Get the metadata associated with key
  MetaDataSparkle *get_metadata_sparkle(const KeyType &key) {
    return apply(
        [&key](HashMapType &map) -> MetaDataSparkle * {
          auto it = map.find(key);
          if (it == map.end()) {
            return nullptr;
          }
          auto &l = std::get<0>(it->second);

          return &l;
        },
        bucket_number(key));
  }

  // remove all versions less than vacuum_version
  std::size_t vacuum_key_versions(const KeyType &key, uint64_t vacuum_version) {
    return apply(
        [&key, vacuum_version](HashMapType &map) -> std::size_t {
          auto it = map.find(key);
          if (it == map.end()) {
            return 0;
          }

          std::size_t size = 0;
          auto &l = std::get<1>(it->second);
          auto lit = l.end();

          while (lit != l.begin()) {
            lit--;
            if (get_version(*lit) < vacuum_version) {
              lit = l.erase(lit);
              size++;
            } else {
              break;
            }
          }
          return size;
        },
        bucket_number(key));
  }

  // remove all versions except the latest one
  std::size_t vacuum_key_keep_latest(const KeyType &key) {
    return apply(
        [&key](HashMapType &map) -> std::size_t {
          auto it = map.find(key);
          if (it == map.end()) {
            return 0;
          }

          std::size_t size = 0;
          auto &l = std::get<1>(it->second);
          auto lit = l.begin();
          if (lit == l.end()) {
            return 0;
          }

          lit++;
          while (lit != l.end()) {
            lit = l.erase(lit);
            size++;
          }
          return size;
        },
        bucket_number(key));
  }

private:
  static uint64_t get_version(std::tuple<uint64_t, ValueType> &t) {
    return std::get<0>(t);
  }

  static ValueType &get_value(std::tuple<uint64_t, ValueType> &t) {
    return std::get<1>(t);
  }

private:
  auto bucket_number(const KeyType &key) { return hasher(key) % N; }

  template <class ApplyFunc>
  auto &apply_ref(ApplyFunc applyFunc, std::size_t i) {
    DCHECK(i < N) << "index " << i << " is greater than " << N;
    locks[i].lock();
    auto &result = applyFunc(maps[i]);
    locks[i].unlock();
    return result;
  }

  template <class ApplyFunc> auto apply(ApplyFunc applyFunc, std::size_t i) {
    DCHECK(i < N) << "index " << i << " is greater than " << N;
    locks[i].lock();
    auto result = applyFunc(maps[i]);
    locks[i].unlock();
    return result;
  }

private:
  HasherType hasher;
  HashMapType maps[N];
  SpinLock locks[N];
};
} // namespace aria