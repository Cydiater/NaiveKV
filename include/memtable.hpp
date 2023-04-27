#pragma once

#include <cassert>
#include <map>
#include <mutex>
#include <optional>
#include <queue>
#include <set>
#include <shared_mutex>

#include "defines.h"
#include "interfaces.h"
#include "log_manager.hpp"
#include "ordered_iteratable.hpp"

namespace kvs {

const size_t sharding_num = 16;

const uint64_t bs = 2333;

inline uint64_t hash(const std::string &s) {
  uint64_t cur = 0;
  for (auto &c : s) {
    cur = cur * bs + c;
  }
  return cur;
}

class Memtable {

public:
  using Map = std::map<TaggedKey, TaggedValue>;
  using Iter = Map::const_iterator;

  Memtable() = default;
  Memtable(const std::vector<std::pair<TaggedKey, TaggedValue>> &init) {
    for (auto &kv : init) {
      kv_[get_target(kv.first)].insert(kv);
    }
  }

  inline int get_target(const TaggedKey &key) {
    return imm ? 0 : hash(key.first) % sharding_num;
  }

  std::deque<InternalKV> lowerbound(const TaggedKey &lower,
                                    const TaggedKey &upper) {
    std::deque<InternalKV> res;
    std::set<InternalKV> ds;
    for (auto &kv : kv_) {
      auto lock = std::shared_lock<std::shared_mutex>(mutex_);
      auto it = kv.lower_bound(lower);
      while (it != kv.end()) {
        auto tmp = *it;
        if (tmp.first > upper)
          break;
        if (tmp.first.second < upper.second) {
          ds.insert(tmp);
        }
        it++;
      }
    }
    for (auto &kv : ds) {
      if (!res.empty() && res.back().first.first == kv.first.first) {
        res.pop_back();
      }
      res.push_back(kv);
    }
    return res;
  }

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
    std::ignore = it;
    std::ignore = success;
  }

  void remove(const TaggedKey &key, LogManager *log_mgr_) {
    auto lock = std::unique_lock<std::shared_mutex>(mutex_);
    const auto [it, success] = logged_insert({key, {"_", true}}, log_mgr_);
    assert(success);
    std::ignore = it;
    std::ignore = success;
  }

  OrderedIterater *get_ordered_iterator();

  void make_imm() {
    imm = true;
    for (size_t i = 1; i < sharding_num; i++) {
      kv_[0].insert(kv_[i].begin(), kv_[i].end());
      kv_[i].clear();
    }
  }

private:
  std::pair<Iter, bool> logged_insert(const InternalKV &kv,
                                      LogManager *log_mgr_) {
    log_mgr_->log(kv);
    return kv_[get_target(kv.first)].insert(kv);
  }

  std::optional<Iter> fetch_const_iter(const TaggedKey &key) {
    auto &kv_ = this->kv_[get_target(key)];
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
  Map kv_[sharding_num];
  bool imm;
};

class MemtableIterator : public OrderedIterater {
public:
  std::optional<InternalKV> next() override {
    if (it == end)
      return std::nullopt;
    auto ret = *it;
    it++;
    return ret;
  }

  MemtableIterator(Memtable::Map::const_iterator it_,
                   Memtable::Map::const_iterator end_)
      : it(it_), end(end_) {}

private:
  Memtable::Map::const_iterator it;
  Memtable::Map::const_iterator end;
};

inline OrderedIterater *Memtable::get_ordered_iterator() {
  assert(imm);
  return new MemtableIterator(kv_[0].begin(), kv_[0].end());
}

} // namespace kvs
