#pragma once

#include "defines.h"
#include "memtable.hpp"
#include "ordered_iteratable.hpp"
#include "sstable.hpp"

#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>

namespace kvs {

class Version {
  std::optional<std::vector<uint32_t>> read_levels(FILE *fd) {
    int num_tables;
    if (EOF == fscanf(fd, "%d", &num_tables))
      return std::nullopt;
    std::vector<uint32_t> table_ids;
    for (int i = 0; i < num_tables; i++) {
      uint32_t id;
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
      }
    }
    std::fclose(fd);
  }

  std::shared_ptr<Version>
  create_next_version(const std::string &base_dir, uint32_t table_number,
                      const std::vector<std::vector<std::string>> &new_tables,
                      const std::vector<std::vector<std::string>> &rm_tables) {
    std::ignore = rm_tables;
    auto next_version = std::make_shared<Version>(*this);
    for (auto &filename : new_tables[0]) {
      next_version->max_id = table_number;
      std::string target_name =
          base_dir + "/sst." + std::to_string(table_number++);
      std::filesystem::rename(filename, target_name);
      next_version->level0.push_back(std::make_shared<SSTable>(target_name));
    }
    return next_version;
  }

  void dump(const std::string &filename) {
    auto fd = std::fopen(filename.c_str(), "w");
    for (auto &s : level0) {
      fprintf(fd, "%u ", s->get_id());
    }
    fprintf(fd, "\n");
    for (auto &lvl : levels) {
      for (auto &s : lvl) {
        fprintf(fd, "%u ", s->get_id());
      }
      fprintf(fd, "\n");
    }
    std::fclose(fd);
  }

  uint32_t get_max_id() const { return max_id; }

  std::optional<bool> get(const TaggedKey &key, std::string &value) {
    std::optional<InternalKV> ans = std::nullopt;
    for (auto &s : level0) {
      std::string val;
      uint32_t lsn;
      auto ret = s->get(key, val, lsn);
      if (ret.has_value()) {
        InternalKV tmp = {{key.first, lsn}, {val, !ret.value()}};
        if (ans == std::nullopt || tmp > ans.value())
          ans = tmp;
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

private:
  std::vector<std::shared_ptr<SSTable>> level0;
  std::vector<std::vector<std::shared_ptr<SSTable>>> levels;
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

  void StoreImmtable(Memtable *imm) {
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
    table_number += new_tables.size();
    auto version_filename =
        base_dir + "/version." + std::to_string(++version_number);
    next_version->dump(version_filename);
    auto fd = std::fopen((base_dir + "/current").c_str(), "w");
    fprintf(fd, "%u", version_number);
    std::fclose(fd);
    latest = next_version;
    phantom->latest = latest;
  }

  void DoCompaction() {}

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
