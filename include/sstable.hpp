#pragma once

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "conf.h"
#include "defines.h"
#include "ordered_iteratable.hpp"

namespace kvs {

inline void checked_fread(void *ptr, uint32_t len, FILE *fd) {
  auto ret = std::fread(ptr, 1, len, fd);
  if (ret != len) {
    std::cerr << "ret = " << ret << " len = " << len
              << " msg: " << std::strerror(errno) << std::endl;
    assert(false);
  }
}

inline std::optional<InternalKV> get_kv(const char *buf, uint64_t &cur,
                                        const uint64_t end) {
  uint32_t key_len;
  if (cur + 4 >= end)
    return std::nullopt;
  std::memcpy(&key_len, buf + cur, 4);
  cur += 4;
  char key_buf[key_len];
  if (cur + key_len >= end)
    return std::nullopt;
  std::memcpy(key_buf, buf + cur, key_len);
  cur += key_len;
  uint64_t lsn;
  if (cur + 8 >= end)
    return std::nullopt;
  std::memcpy(&lsn, buf + cur, 8);
  cur += 8;
  uint32_t val_len;
  if (cur + 4 >= end)
    return std::nullopt;
  std::memcpy(&val_len, buf + cur, 4);
  cur += 4;
  char val_buf[val_len];
  if (cur + val_len >= end)
    return std::nullopt;
  std::memcpy(val_buf, buf + cur, val_len);
  cur += val_len;
  bool deleted;
  if (cur + 1 > end)
    return std::nullopt;
  std::memcpy(&deleted, buf + cur, 1);
  cur += 1;
  InternalKV kv = {{std::string(key_buf, key_len), lsn},
                   {std::string(val_buf, val_len), deleted}};
  return kv;
}

class SSTableIterator : public OrderedIterater {

  void alloc() {
    buf_offset = cur;
    size_t len = std::min(kMaxKeyValueSize, static_cast<size_t>(end - cur));
    auto ret1 = std::fseek(fd, buf_offset, SEEK_SET);
    assert(ret1 == 0);
    auto ret2 = std::fread(buf, 1, len, fd);
    assert(ret2 == len);
    std::ignore = ret1;
    std::ignore = ret2;
  }

public:
  SSTableIterator(FILE *fd_, uint64_t cur_, uint64_t end_, bool owner_)
      : fd(fd_), cur(cur_), end(end_), buf_offset(-1), owner(owner_){};

  std::optional<InternalKV> get() {
    if (cur == end)
      return std::nullopt;
    if (buf_offset == -1) {
      buf_offset = cur;
      alloc();
    }
    auto _cur = cur - buf_offset;
    auto kv = get_kv(buf, _cur,
                     std::min(static_cast<uint64_t>(end - buf_offset),
                              static_cast<uint64_t>(kMaxKeyValueSize)));
    if (kv == std::nullopt) {
      alloc();
      return get();
    }
    return kv;
  }

  std::optional<InternalKV> next() override {
    auto kv = get();
    if (kv == std::nullopt)
      return std::nullopt;
    cur += 4 + kv.value().first.first.length() + 8 +
           kv.value().second.first.length() + 4 + 1;
    return kv;
  }

  ~SSTableIterator() {
    if (owner)
      std::fclose(fd);
  }

private:
  FILE *fd;
  uint64_t cur{0}, end;
  int64_t buf_offset;
  char buf[kMaxKeyValueSize];
  bool owner;
};

class SSTableBuilder {
public:
  SSTableBuilder(std::vector<std::unique_ptr<OrderedIterater>> sources)
      : sources_{std::move(sources)} {

    for (auto &source : sources_) {
      auto first = source->next();
      if (!first.has_value())
        continue;
      ds.insert({first.value(), source.get()});
    }
  }

  std::optional<std::string> build() {
    uint64_t offset = 0,
             extra_bytes =
                 sizeof(uint64_t) /* offset of index's offset array */,
             last_offset = 0;
    std::ignore = extra_bytes;
    if (ds.empty())
      return {};
    std::vector<std::tuple<TaggedKey, uint32_t, uint32_t>> block_keys;
    std::optional<TaggedKey> first_key = std::nullopt;
    std::optional<TaggedKey> last_key = std::nullopt;
    while (!ds.empty()) {
      auto it = ds.begin();
      auto [kv, source] = *it;
      ds.erase(it);
      auto next = source->next();
      if (next.has_value()) {
        ds.insert({next.value(), source});
      }
      if (first_key == std::nullopt) {
        first_key = kv.first;
      }
      last_key = kv.first;
      encode_string(buf, offset, kv.first.first);
      encode(buf, offset, kv.first.second);
      encode_string(buf, offset, kv.second.first);
      encode(buf, offset, kv.second.second);
      if (offset - last_offset >= kMaxBlockSize || ds.empty()) {
        assert(first_key.has_value());
        block_keys.push_back({first_key.value(), offset, 0});
        extra_bytes += sizeof(uint32_t) + first_key.value().first.length() +
                       sizeof(uint64_t) /* key record */ +
                       sizeof(uint32_t) /* key offset record */ +
                       sizeof(uint32_t) /* block offset record */;
        last_offset = offset;
        first_key = std::nullopt;
        if (offset >= kMaxTableSize || ds.empty()) {
          block_keys.push_back({last_key.value(), offset, 0});
          extra_bytes += sizeof(uint32_t) + last_key.value().first.length() +
                         sizeof(uint64_t) /* key record */ +
                         sizeof(uint32_t) /* key offset record */ +
                         sizeof(uint32_t) /* block offset record */;
          last_offset = offset;
          break;
        }
      }
    }
    assert(offset == last_offset);
    for (auto &[k, block_offset, key_offset] : block_keys) {
      key_offset = offset;
      encode_string(buf, offset, k.first);
      encode(buf, offset, k.second);
    }
    auto offset_offset = offset;
    for (auto &[k, block_offset, key_offset] : block_keys) {
      encode(buf, offset, block_offset);
      encode(buf, offset, key_offset);
    }
    encode(buf, offset, offset_offset);
    assert(extra_bytes == offset - last_offset);
    char tmpfile[] = "/tmp/sstable-XXXXXX";
    auto fd = mkstemp(tmpfile);
    auto ret = write(fd, buf, offset);
    assert(ret == static_cast<uint32_t>(offset));
    ret = close(fd);
    assert(ret == 0);
    std::ignore = ret;
    return std::string(tmpfile);
  }

private:
  std::vector<std::unique_ptr<OrderedIterater>> sources_;
  std::set<std::pair<InternalKV, OrderedIterater *>> ds;
  char buf[kMaxTableSize * 2];
};

class SSTable {
public:
  SSTable(const std::string &filename) : filename_(filename) {
    auto fd = get_or_create_fd(std::this_thread::get_id());
    assert(fd != NULL);
    auto ret = std::fseek(fd, 0, SEEK_END);
    assert(ret == 0);
    uint32_t end = std::ftell(fd) - 8;
    ret = std::fseek(fd, -8, SEEK_END);
    assert(ret == 0);
    uint64_t start = 0;
    checked_fread(&start, 8, fd);
    char buf[end - start];
    ret = std::fseek(fd, start, SEEK_SET);
    assert(ret == 0);
    assert((end - start) % 8 == 0);
    assert(start < end);
    checked_fread(buf, end - start, fd);
    uint32_t *offsets = reinterpret_cast<uint32_t *>(&buf);
    uint32_t len = (end - start) / 4;
    for (uint32_t i = 0; i < len - 2; i += 2) {
      this->offsets.push_back({offsets[i], offsets[i + 1]});
    }
    this->first = get_key(fd, offsets[1]);
    this->last = get_key(fd, offsets[len - 1]);
    std::ignore = ret;
  }

  ~SSTable() {
    for (auto kv : fds)
      std::fclose(kv.second);
    std::filesystem::remove(filename_);
  }

  std::string get_filename() const { return filename_; }
  uint32_t get_id() const {
    uint32_t id = 0, bs = 1;
    int i = filename_.length() - 1;
    while (i >= 0) {
      if (filename_[i] == '.')
        break;
      id += bs * (filename_[i] - '0');
      i -= 1;
      bs *= 10;
    }
    return id;
  }

  TaggedKey get_key(FILE *fd, uint32_t offset) const {
    auto ret = std::fseek(fd, offset, SEEK_SET);
    assert(ret == 0);
    std::ignore = ret;
    uint32_t key_len;
    checked_fread(&key_len, 4, fd);
    char key_buf[key_len];
    checked_fread(key_buf, key_len, fd);
    uint64_t lsn;
    checked_fread(&lsn, 8, fd);
    return {std::string(key_buf, key_len), lsn};
  }

  std::optional<bool> get(FILE *fd, uint32_t start, uint32_t end,
                          const TaggedKey &key, std::string &value,
                          uint32_t &lsn) const {
    auto iter = SSTableIterator(fd, start, end, false);
    std::optional<InternalKV> ans = std::nullopt;
    while (true) {
      auto kv = iter.next();
      if (kv == std::nullopt)
        break;
      if (kv.value().first <= key)
        ans = kv.value();
      else
        break;
    }
    if (!ans.has_value())
      return std::nullopt;
    auto target = ans.value();
    if (target.first.first == key.first) {
      lsn = target.first.second;
      if (target.second.second)
        return false;
      value = target.second.first;
      return true;
    }
    return std::nullopt;
  }

  FILE *get_or_create_fd(const std::thread::id i) {
    auto lock = std::lock_guard(mu);
    if (fds.find(i) == fds.end()) {
      auto fd = std::fopen(filename_.c_str(), "rb");
      if (fd == NULL || std::ferror(fd)) {
        std::cerr << "Failed to open sstable: " << std::strerror(errno)
                  << std::endl;
        assert(false);
      }
      fds[i] = fd;
    }
    return fds[i];
  }

  uint32_t get_target_block(const TaggedKey &key) {
    auto fd = get_or_create_fd(std::this_thread::get_id());
    int l = 0, r = offsets.size() - 1, m;
    while (l + 1 < r) {
      m = (l + r) / 2;
      auto fetched_key = get_key(fd, offsets[m].second);
      if (fetched_key <= key) {
        l = m;
      } else {
        r = m;
      }
    }
    if (get_key(fd, offsets[r].second) <= key) {
      return r;
    } else if (get_key(fd, offsets[l].second) <= key) {
      return l;
    } else {
      return offsets.size();
    }
  }

  std::optional<bool> get(const TaggedKey &key, std::string &value,
                          uint32_t &lsn) {
    auto target_block = get_target_block(key);
    if (target_block == offsets.size())
      return std::nullopt;
    uint32_t start = 0;
    if (target_block > 0) {
      start = offsets[target_block - 1].first;
    }
    uint32_t end = offsets[target_block].first;
    auto fd = get_or_create_fd(std::this_thread::get_id());
    auto ret = get(fd, start, end, key, value, lsn);
    return ret;
  }

  OrderedIterater *get_ordered_iterator(
      const std::optional<TaggedKey> &lowerbound = std::nullopt) {
    auto fd = std::fopen(filename_.c_str(), "r");
    if (lowerbound == std::nullopt) {
      return new SSTableIterator(fd, 0, offsets[0].second, true);
    }
    auto target_block = get_target_block(lowerbound.value());
    if (target_block == offsets.size())
      throw std::runtime_error("expected to be overlapped");
    auto source = new SSTableIterator(fd, offsets[target_block].first,
                                      offsets[0].second, true);
    while (true) {
      auto kv = source->get();
      if (kv == std::nullopt)
        throw std::runtime_error("expected to be overlapped");
      if (kv.value().first >= lowerbound.value())
        break;
      source->next();
    }
    return source;
  }

  const TaggedKey get_first() const { return first; }
  const TaggedKey get_last() const { return last; }

private:
  TaggedKey first, last;
  std::string filename_;
  std::mutex mu;
  std::map<std::thread::id, FILE *> fds;
  std::vector<std::pair<uint32_t, uint32_t>> offsets;
};

} // namespace kvs
