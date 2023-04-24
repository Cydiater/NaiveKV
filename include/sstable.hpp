#pragma once

#include <cstdlib>
#include <filesystem>
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
      : sources_{std::move(sources)} {}

  std::optional<std::string> build() {
    uint64_t offset = 0,
             extra_bytes =
                 sizeof(uint32_t) /* offset of index's offset array */,
             last_offset = 0;
    std::set<std::pair<InternalKV, OrderedIterater *>> ds;
    for (auto &source : sources_) {
      auto first = source->next();
      if (!first.has_value())
        continue;
      ds.insert({first.value(), source.get()});
    }
    if (ds.empty())
      return {};
    std::vector<std::tuple<TaggedKey, uint32_t, uint32_t>> block_keys;
    while (!ds.empty()) {
      auto it = ds.begin();
      auto [kv, source] = *it;
      ds.erase(it);
      auto next = source->next();
      if (next.has_value()) {
        ds.insert({next.value(), source});
      }
      encode_string(buf, offset, kv.first.first);
      encode(buf, offset, kv.first.second);
      encode_string(buf, offset, kv.second.first);
      encode(buf, offset, kv.second.second);
      if (offset - last_offset >= kMaxBlockSize || ds.empty()) {
        block_keys.push_back({kv.first, offset, 0});
        extra_bytes += sizeof(uint32_t) + kv.first.first.length() +
                       sizeof(uint64_t) /* key record */ +
                       sizeof(uint32_t) /* key offset record */ +
                       sizeof(uint32_t) /* block offset record */;
        last_offset = offset;
        if (offset >= kMaxTableSize) {
          break;
        }
      }
    }
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

private:
  std::string filename_;
  FILE *fd;
};

class SSTableIterator : public OrderedIterater {};

} // namespace kvs
