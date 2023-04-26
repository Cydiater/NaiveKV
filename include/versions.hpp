#pragma once

#include "defines.h"
#include "memtable.hpp"
#include "ordered_iteratable.hpp"
#include "sstable.hpp"

#include <algorithm>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>

namespace kvs {

class Versions;

class Version {
  std::optional<std::vector<uint32_t>> read_levels(FILE *fd) const {
    int num_tables;
    if (EOF == fscanf(fd, "%d", &num_tables))
      return std::nullopt;
    std::vector<uint32_t> table_ids;
    for (int i = 0; i < num_tables; i++) {
      uint32_t id = 0;
      auto ret = fscanf(fd, "%u", &id);
      assert(ret != EOF);
      table_ids.push_back(id);
      std::ignore = ret;
    }
    return table_ids;
  }

public:
  Version(const std::string &version_name, const std::string &base_dir) {
    assert(std::filesystem::exists(version_name));
    auto fd = std::fopen(version_name.c_str(), "r");
    max_id = 0;
    auto ids = read_levels(fd);
    if (ids.has_value()) {
      for (auto id : ids.value()) {
        max_id = std::max(id, max_id);
        auto table_name = base_dir + "/sst." + std::to_string(id);
        level0.emplace_back(std::make_shared<SSTable>(table_name));
        last_compaction_key.push_back(std::nullopt);
      }
      while (true) {
        ids = read_levels(fd);
        if (!ids.has_value())
          break;
        std::vector<std::shared_ptr<SSTable>> level = {};
        for (auto id : ids.value()) {
          max_id = std::max(id, max_id);
          auto table_name = base_dir + "/sst." + std::to_string(id);
          level.emplace_back(std::make_shared<SSTable>(table_name));
        }
        levels.emplace_back(level);
        last_compaction_key.push_back(std::nullopt);
      }
    }
    std::fclose(fd);
  }

  std::shared_ptr<Version> create_next_version(
      const std::string &base_dir, uint32_t &table_number,
      const std::vector<std::vector<std::string>> &new_tables) const {
    auto next_version = std::make_shared<Version>(*this);
    for (auto &filename : new_tables[0]) {
      next_version->max_id = table_number;
      std::string target_name =
          base_dir + "/sst." + std::to_string(table_number++);
      auto ret = std::rename(filename.c_str(), target_name.c_str());
      assert(ret == 0);
      std::ignore = ret;
      next_version->level0.push_back(std::make_shared<SSTable>(target_name));
    }
    return next_version;
  }

  std::shared_ptr<Version>
  create_next_version_by_compacting_level0(const std::string &base_dir,
                                           uint32_t &table_number) {
    std::set<uint32_t> source_indices;
    auto left = level0[0]->get_first(), right = level0[0]->get_last();
    while (true) {
      auto before = source_indices.size();
      for (uint32_t i = 0; i < level0.size(); i++) {
        if (level0[i]->get_last() < left || level0[i]->get_first() > right)
          continue;
        source_indices.insert(i);
        left = std::min(left, level0[i]->get_first());
        right = std::max(right, level0[i]->get_last());
      }
      if (before == source_indices.size())
        break;
    }
    if (levels.size() == 0) {
      levels.push_back({});
      last_compaction_key.push_back({});
    }
    std::pair<int, int> next_level_merge_range(0, -1);
    for (int i = 0; i < static_cast<int>(levels[0].size()); i++) {
      if (levels[0][i]->get_last() < left || levels[0][i]->get_first() > right)
        continue;
      if (i > next_level_merge_range.second)
        next_level_merge_range.second = i;
      else {
        next_level_merge_range.first = i;
        next_level_merge_range.second = i;
      }
    }
    std::vector<std::unique_ptr<OrderedIterater>> sources = {};
    for (auto i : source_indices)
      sources.push_back(
          std::unique_ptr<OrderedIterater>(level0[i]->get_ordered_iterator()));
    if (next_level_merge_range.first <= next_level_merge_range.second) {
      for (auto i = next_level_merge_range.first;
           i <= next_level_merge_range.second; i++) {
        sources.push_back(std::unique_ptr<OrderedIterater>(
            levels[0][i]->get_ordered_iterator()));
      }
    }
    auto builder = SSTableBuilder(std::move(sources));
    std::vector<std::string> tmp_sstables;
    while (true) {
      auto build = builder.build();
      if (build == std::nullopt)
        break;
      tmp_sstables.push_back(build.value());
    }
    auto next_version = std::make_shared<Version>(*this);
    for (auto it = source_indices.rbegin(); it != source_indices.rend(); it++) {
      auto i = *it;
      next_version->level0.erase(next_version->level0.begin() + i);
    }
    if (next_level_merge_range.first <= next_level_merge_range.second) {
      auto base = next_version->levels[0].begin();
      next_version->levels[0].erase(base + next_level_merge_range.first,
                                    base + next_level_merge_range.second + 1);
    }
    for (auto it = tmp_sstables.rbegin(); it != tmp_sstables.rend(); it++) {
      auto &filename = *it;
      auto target_filename =
          base_dir + "/sst." + std::to_string(table_number++);
      std::filesystem::rename(filename, target_filename);
      auto base = next_version->levels[0].begin();
      next_version->levels[0].insert(
          base + next_level_merge_range.first,
          std::make_shared<SSTable>(target_filename));
    }
    return next_version;
  }

  std::shared_ptr<Version> create_next_version_by_compacting_level_i(
      const std::string &base_dir, uint32_t lvl_idx, uint32_t &table_number) {
    if (levels.size() <= lvl_idx + 1) {
      levels.push_back({});
      last_compaction_key.push_back({});
    }
    auto start_key =
        last_compaction_key[lvl_idx].value_or(levels[lvl_idx][0]->get_first());
    uint32_t this_level_idx = 0;
    while (this_level_idx < levels[lvl_idx].size()) {
      if (levels[lvl_idx][this_level_idx]->get_first() >= start_key)
        break;
      this_level_idx += 1;
    }
    this_level_idx %= levels[lvl_idx].size();
    auto left = levels[lvl_idx][this_level_idx]->get_first(),
         right = levels[lvl_idx][this_level_idx]->get_last();
    std::pair<int, int> next_level_merge_range(0, -1);
    for (int i = 0; i < static_cast<int>(levels[lvl_idx + 1].size()); i++) {
      if (levels[lvl_idx + 1][i]->get_last() < left ||
          levels[lvl_idx + 1][i]->get_first() > right)
        continue;
      if (i > next_level_merge_range.second)
        next_level_merge_range.second = i;
      else {
        next_level_merge_range.first = i;
        next_level_merge_range.second = i;
      }
    }
    std::vector<std::unique_ptr<OrderedIterater>> sources;
    sources.push_back(std::unique_ptr<OrderedIterater>(
        levels[lvl_idx][this_level_idx]->get_ordered_iterator()));
    for (auto i = next_level_merge_range.first;
         i <= next_level_merge_range.second; i++) {
      sources.push_back(std::unique_ptr<OrderedIterater>(
          levels[lvl_idx + 1][i]->get_ordered_iterator()));
    }
    auto builder = SSTableBuilder(std::move(sources));
    std::vector<std::string> tmp_sstables;
    while (true) {
      auto build = builder.build();
      if (build == std::nullopt)
        break;
      tmp_sstables.push_back(build.value());
    }
    auto next_version = std::make_shared<Version>(*this);
    next_version->last_compaction_key[lvl_idx] = right;
    next_version->levels[lvl_idx].erase(next_version->levels[lvl_idx].begin() +
                                        this_level_idx);
    for (auto it = tmp_sstables.rbegin(); it != tmp_sstables.rend(); it++) {
      auto &filename = *it;
      auto target_filename =
          base_dir + "/sst." + std::to_string(table_number++);
      std::filesystem::rename(filename, target_filename);
      auto base = next_version->levels[lvl_idx + 1].begin();
      next_version->levels[lvl_idx + 1].insert(
          base + next_level_merge_range.first,
          std::make_shared<SSTable>(target_filename));
    }
    return next_version;
  }

  void dump(const std::string &filename) const {
    auto fd = std::fopen(filename.c_str(), "w");
    fprintf(fd, "%lu ", level0.size());
    for (auto &s : level0) {
      fprintf(fd, "%u ", s->get_id());
    }
    fprintf(fd, "\n");
    for (auto &lvl : levels) {
      fprintf(fd, "%lu ", lvl.size());
      for (auto &s : lvl) {
        fprintf(fd, "%u ", s->get_id());
      }
      fprintf(fd, "\n");
    }
    std::fclose(fd);
  }

  uint32_t get_max_id() const { return max_id; }

  std::optional<bool> get(const std::vector<std::shared_ptr<SSTable>> &lvl,
                          const bool overlapping, const TaggedKey &key,
                          std::string &value) const {
    std::optional<InternalKV> ans = std::nullopt;
    std::string val;
    uint32_t lsn;
    if (overlapping) {
      for (auto &s : lvl) {
        auto ret = s->get(key, val, lsn);
        if (ret.has_value()) {
          InternalKV tmp = {{key.first, lsn}, {val, !ret.value()}};
          if (ans == std::nullopt || tmp > ans.value()) {
            ans = tmp;
          }
        }
      }
    } else {
      auto it = std::lower_bound(
          lvl.begin(), lvl.end(), key,
          [](const std::shared_ptr<SSTable> &lhs, const TaggedKey &key) {
            return lhs->get_last() < key;
          });
      if (it == lvl.end())
        return std::nullopt;
      auto ret = (*it)->get(key, val, lsn);
      if (ret.has_value()) {
        ans = {{key.first, lsn}, {val, !ret.value()}};
      }
    }
    if (ans == std::nullopt)
      return std::nullopt;
    if (ans.value().second.second) {
      return false;
    }
    value = ans.value().second.first;
    return true;
  }

  std::optional<bool> get(const TaggedKey &key, std::string &value) const {
    auto ret = get(level0, true, key, value);
    if (ret != std::nullopt) {
      return ret;
    }
    for (auto &lvl : levels) {
      auto ret = get(lvl, false, key, value);
      if (ret != std::nullopt) {
        return ret;
      }
    }
    return std::nullopt;
  }

  friend Versions;

private:
  std::vector<std::shared_ptr<SSTable>> level0;
  std::vector<std::vector<std::shared_ptr<SSTable>>> levels;
  std::vector<std::optional<TaggedKey>> last_compaction_key;
  uint32_t max_id;
};

class Versions {
  struct PhantomStorage {
    std::shared_ptr<Version> latest;
  };

public:
  Versions(const std::string &base_dir) : base_dir(base_dir) {
    auto current_file = base_dir + "/current";
    if (!std::filesystem::exists(current_file.c_str())) {
      version_number = 0;
      table_number = 0;
      auto fd = std::fopen(current_file.c_str(), "w");
      fprintf(fd, "%u", version_number);
      std::fclose(fd);
      fd = std::fopen((base_dir + "/version.0").c_str(), "w");
      std::fclose(fd);
    }
    auto fd = std::fopen(current_file.c_str(), "r");
    uint32_t num;
    fscanf(fd, "%u", &num);
    std::fclose(fd);
    version_number = num;
    auto version_name = base_dir + "/version." + std::to_string(num);
    latest = std::make_shared<Version>(version_name, base_dir);
    table_number = latest->get_max_id() + 1;
    phantom = new PhantomStorage{latest};
  }

  void add_version(std::shared_ptr<Version> next_version) {
    auto version_filename =
        base_dir + "/version." + std::to_string(++version_number);
    next_version->dump(version_filename);
    auto fd = std::fopen((base_dir + "/current").c_str(), "w");
    fprintf(fd, "%u", version_number);
    std::fclose(fd);
    latest = next_version;
    phantom->latest = latest;
  }

  void store_immtable(Memtable *imm) {
    auto it = std::unique_ptr<OrderedIterater>(imm->get_ordered_iterator());
    std::vector<std::unique_ptr<OrderedIterater>> sources;
    sources.push_back(std::move(it));
    auto builder = SSTableBuilder(std::move(sources));
    std::vector<std::string> new_tables;
    while (true) {
      auto sstable_tmp = builder.build();
      if (!sstable_tmp.has_value())
        break;
      new_tables.push_back(sstable_tmp.value());
    }
    auto lock = std::lock_guard<std::mutex>(mutex);
    auto next_version =
        latest->create_next_version(base_dir, table_number, {new_tables});
    add_version(next_version);
  }

  void schedule_compaction() {
    auto lock = std::lock_guard(mutex);
    if (latest == nullptr)
      return;
    std::shared_ptr<Version> next = nullptr;
    if (latest->level0.size() > 4) {
      next = latest->create_next_version_by_compacting_level0(base_dir,
                                                              table_number);
    } else {
      uint32_t bs = 10, i = 0;
      for (auto &lvl : latest->levels) {
        if (lvl.size() > bs) {
          next = latest->create_next_version_by_compacting_level_i(
              base_dir, i, table_number);
          break;
        }
        bs *= 10;
        i += 1;
      }
    }
    if (next == nullptr) {
      return;
    }
    add_version(next);
  }

  ~Versions() {
    // PhantomStorage is leaked here intentionally to prevent latest version
    // from being deleted
  }

  std::shared_ptr<Version> get_latest() {
    auto ret = latest;
    return ret;
  }

private:
  std::shared_ptr<Version> latest;
  uint32_t version_number, table_number;
  std::string base_dir;
  std::mutex mutex;
  PhantomStorage *phantom;
};
} // namespace kvs
