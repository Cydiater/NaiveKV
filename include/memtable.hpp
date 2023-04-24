#pragma once

#include <cassert>
#include <map>
#include <mutex>
#include <optional>
#include <shared_mutex>

#include "defines.h"
#include "interfaces.h"
#include "log_manager.hpp"
#include "ordered_iteratable.hpp"

namespace kvs {

class Memtable {

public:
  using Map = std::map<TaggedKey, TaggedValue>;
  using Iter = Map::const_iterator;

  Memtable() = default;
  Memtable(const std::vector<std::pair<TaggedKey, TaggedValue>> &init)
      : kv_(init.begin(), init.end()) {}

  std::optional<bool> get(const TaggedKey &key, std::string &value) {
    auto lock = std::shared_lock<std::shared_mutex>(mutex_);
    auto fetched_it = fetch_const_iter(key);
    if (fetched_it.has_value()) {
      auto [val, deleted] = fetched_it.value()->second;
      value = val;
      return !deleted;
    }
    return {};
  }

  void insert(const TaggedKey &key, const std::string &value,
              LogManager *log_mgr_) {
    auto lock = std::unique_lock<std::shared_mutex>(mutex_);
    const auto [it, success] = logged_insert({key, {value, false}}, log_mgr_);
    assert(success);
  }

  void remove(const TaggedKey &key, LogManager *log_mgr_) {
    auto lock = std::unique_lock<std::shared_mutex>(mutex_);
    const auto [it, success] = logged_insert({key, {"_", true}}, log_mgr_);
    assert(success);
  }

private:
  std::pair<Iter, bool> logged_insert(const InternalKV &kv,
                                      LogManager *log_mgr_) {
    log_mgr_->log(kv);
    return kv_.insert(kv);
  }

  std::optional<Iter> fetch_const_iter(const TaggedKey &key) {
    auto it = kv_.lower_bound(key);
    if (it == kv_.begin())
      return {};
    --it;
    const auto &[tagged_key, tagged_value] = *it;
    if (tagged_key.first != key.first)
      return {};
    return it;
  }

  std::shared_mutex mutex_;
  Map kv_;
};

class MemtableIterator : public OrderedIterater {
public:
  std::optional<InternalKV> next() override {
    if (it == end)
      return {};
    return *it++;
  }

private:
  Memtable::Map::const_iterator it;
  Memtable::Map::const_iterator end;
};

} // namespace kvs
