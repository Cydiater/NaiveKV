#pragma once

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <optional>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

#include "conf.h"
#include "defines.h"
#include "ordered_iteratable.hpp"

namespace kvs {

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
    if (ds.empty())
      return {};
    std::vector<std::tuple<TaggedKey, uint32_t, uint32_t>> block_keys;
    std::optional<TaggedKey> first_key = std::nullopt;
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
        if (offset >= kMaxTableSize) {
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
    write(fd, buf, offset);
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
    fd = std::fopen(filename_.c_str(), "r");
  }

  ~SSTable() {
    std::fclose(fd);
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

  TaggedKey get_key(uint32_t offset) const {
    std::fseek(fd, offset, SEEK_SET);
    uint32_t key_len;
    std::fread(&key_len, 4, 1, fd);
    char key_buf[key_len];
    std::fread(key_buf, key_len, 1, fd);
    uint64_t lsn;
    std::fread(&lsn, 8, 1, fd);
    return {std::string(key_buf, key_len), lsn};
  }

  InternalKV get_kv(uint32_t &offset) const {
    std::fseek(fd, offset, SEEK_SET);
    uint32_t key_len;
    std::fread(&key_len, 4, 1, fd);
    offset += 4;
    char key_buf[key_len];
    std::fread(key_buf, key_len, 1, fd);
    offset += key_len;
    uint64_t lsn;
    std::fread(&lsn, 8, 1, fd);
    offset += 8;
    uint32_t val_len;
    std::fread(&val_len, 4, 1, fd);
    offset += 4;
    char val_buf[val_len];
    std::fread(val_buf, val_len, 1, fd);
    offset += val_len;
    bool deleted;
    std::fread(&deleted, 1, 1, fd);
    offset += 1;
    return {{std::string(key_buf, key_len), lsn},
            {std::string(val_buf, val_len), deleted}};
  }

  std::optional<bool> get(uint32_t start, uint32_t end, const TaggedKey &key,
                          std::string &value, uint32_t &lsn) const {
    std::optional<InternalKV> ans = std::nullopt;
    while (start < end) {
      auto kv = get_kv(start);
      if (kv.first <= key) {
        ans = kv;
      } else {
        break;
      }
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

  std::optional<bool> get(const TaggedKey &key, std::string &value,
                          uint32_t &lsn) const {
    std::fseek(fd, -8, SEEK_END);
    uint64_t start;
    std::fread(&start, 8, 1, fd);
    std::fseek(fd, 0, SEEK_END);
    uint32_t end = std::ftell(fd) - 8;
    char buf[end - start];
    std::fseek(fd, start, SEEK_SET);
    std::fread(buf, end - start, 1, fd);
    uint32_t *offsets = reinterpret_cast<uint32_t *>(&buf);
    assert((end - start) % 8 == 0);
    uint32_t len = (end - start) / 4;
    int l = 0, r = (len / 2) - 1, m;
    while (l + 1 < r) {
      m = (l + r) / 2;
      auto fetched_key = get_key(offsets[m * 2 + 1]);
      if (fetched_key <= key) {
        l = m;
      } else {
        r = m;
      }
    }
    uint32_t target_block = 0;
    if (get_key(offsets[r * 2 + 1]) <= key) {
      target_block = r;
    } else if (get_key(offsets[l * 2 + 1]) <= key) {
      target_block = l;
    } else {
      auto _l = get_key(offsets[l * 2 + 1]);
      return std::nullopt;
    }
    start = 0;
    if (target_block > 0) {
      start = offsets[(target_block - 1) * 2];
    }
    end = offsets[target_block * 2];
    return get(start, end, key, value, lsn);
  }

private:
  std::string filename_;
  FILE *fd;
};

class SSTableIterator : public OrderedIterater {};

} // namespace kvs
