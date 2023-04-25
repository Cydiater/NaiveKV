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
  std::optional<std::vector<uint32_t>> read_levels(FILE *fd) {
    int num_tables;
    if (EOF == fscanf(fd, "%d", &num_tables))
      return std::nullopt;
    std::vector<uint32_t> table_ids;
    for (int i = 0; i < num_tables; i++) {
      uint32_t id = 0;
      assert(fscanf(fd, "%u", &id) != EOF);
      table_ids.push_back(id);
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

  std::shared_ptr<Version>
  create_next_version(const std::string &base_dir, uint32_t &table_number,
                      const std::vector<std::vector<std::string>> &new_tables,
                      const std::vector<std::vector<std::string>> &rm_tables) {
    std::ignore = rm_tables;
    auto next_version = std::make_shared<Version>(*this);
    for (auto &filename : new_tables[0]) {
      next_version->max_id = table_number;
      std::string target_name =
          base_dir + "/sst." + std::to_string(table_number++);
      assert(std::rename(filename.c_str(), target_name.c_str()) == 0);
      next_version->level0.push_back(std::make_shared<SSTable>(target_name));
    }
    printf("new version with %lu level-0 tables\n",
           next_version->level0.size());
    return next_version;
  }

  std::shared_ptr<Version>
  create_next_version_by_compacting_level0(const std::string &base_dir,
                                           uint32_t &table_number) {
    std::vector<uint32_t> source_indices;
    auto left = level0[0]->get_first(), right = level0[0]->get_last();
    for (uint32_t i = 0; i < level0.size(); i++) {
      if (level0[i]->get_last() < left || level0[i]->get_first() > right)
        continue;
      source_indices.push_back(i);
      left = std::min(left, level0[i]->get_first());
      right = std::max(right, level0[i]->get_last());
    }
    if (levels.size() == 0) {
      levels.push_back({});
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
    printf("got builder\n");
    std::vector<std::string> tmp_sstables;
    while (true) {
      auto build = builder.build();
      printf("built one\n");
      if (build == std::nullopt)
        break;
      tmp_sstables.push_back(build.value());
    }
    printf("tables built of total %lu\n", tmp_sstables.size());
    auto next_version = std::make_shared<Version>(*this);
    std::sort(source_indices.begin(), source_indices.end());
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

  std::shared_ptr<Version>
  create_next_version_by_compacting_level_i(const std::string &base_dir,
                                            uint32_t lvl_idx) {
    std::ignore = base_dir;
    std::ignore = lvl_idx;
    return nullptr;
  }

  void dump(const std::string &filename) {
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
                          const TaggedKey &key, std::string &value) {
    std::optional<InternalKV> ans = std::nullopt;
    for (auto &s : lvl) {
      std::string val;
      uint32_t lsn;
      auto ret = s->get(key, val, lsn);
      if (ret.has_value()) {
        InternalKV tmp = {{key.first, lsn}, {val, !ret.value()}};
        if (ans == std::nullopt || tmp > ans.value()) {
          ans = tmp;
        }
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

  std::optional<bool> get(const TaggedKey &key, std::string &value) {
    auto ret = get(level0, key, value);
    if (ret != std::nullopt)
      return ret;
    for (auto &lvl : levels) {
      auto ret = get(lvl, key, value);
      if (ret != std::nullopt)
        return ret;
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
        latest->create_next_version(base_dir, table_number, {new_tables}, {});
    add_version(next_version);
  }

  void schedule_compaction() {
    printf("compaction scheduled\n");
    auto lock = std::lock_guard(mutex);
    if (latest == nullptr)
      return;
    std::shared_ptr<Version> next = nullptr;
    if (latest->level0.size() > 4) {
      printf("do l0 compaction\n");
      next = latest->create_next_version_by_compacting_level0(base_dir,
                                                              table_number);
      printf("next version created");
    } else {
      uint32_t bs = 10, i = 0;
      for (auto &lvl : latest->levels) {
        if (lvl.size() > bs) {
          next = latest->create_next_version_by_compacting_level_i(base_dir, i);
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

  std::shared_ptr<Version> get_latest() { return latest; }

private:
  std::shared_ptr<Version> latest;
  uint32_t version_number, table_number;
  std::string base_dir;
  std::mutex mutex;
  PhantomStorage *phantom;
};
} // namespace kvs
