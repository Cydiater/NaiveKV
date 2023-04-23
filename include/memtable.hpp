#pragma once

#include <cassert>
#include <map>
#include <mutex>
#include <optional>
#include <shared_mutex>

#include "defines.h"
#include "interfaces.h"

namespace kvs {

class Memtable {

  using Map = std::map<TaggedKey, TaggedValue>;
  using Iter = Map::const_iterator;

public:
  RetCode get(const TaggedKey &key, std::string &value) {
    auto lock = std::shared_lock<std::shared_mutex>(mutex_);
    auto fetched_it = fetch_const_iter(key);
    if (fetched_it.has_value()) {
      value = fetched_it.value()->second.first;
      return RetCode::kSucc;
    }
    return RetCode::kNotFound;
  }

  void insert(const TaggedKey &key, const std::string &value) {
    auto lock = std::unique_lock<std::shared_mutex>(mutex_);
    const auto [it, success] = kv_.insert({key, {value, false}});
    assert(success);
  }

  RetCode remove(const TaggedKey &key) {
    auto lock = std::unique_lock<std::shared_mutex>(mutex_);
    auto fetched_it = fetch_const_iter(key);
    if (!fetched_it.has_value())
      return kNotFound;
    const auto [it, success] = kv_.insert({key, {"", true}});
    assert(success);
    return kSucc;
  }

private:
  std::optional<Iter> fetch_const_iter(const TaggedKey &key) {
    auto it = kv_.lower_bound(key);
    if (it == kv_.begin())
      return {};
    --it;
    const auto &[tagged_key, tagged_value] = *it;
    if (tagged_key.first != key.first)
      return {};
    if (tagged_value.second)
      return {};
    return it;
  }

  std::shared_mutex mutex_;
  Map kv_;
};

} // namespace kvs
