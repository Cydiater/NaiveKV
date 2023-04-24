#pragma once

#include "memtable.hpp"
#include "sstable.hpp"

#include <filesystem>
#include <memory>

namespace kvs {

class Version {
  std::optional<std::vector<uint32_t>> read_levels(FILE *fd) {
    int num_tables;
    if (EOF == scanf("%d", &num_tables))
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
    auto ids = read_levels(fd);
    if (ids.has_value()) {
      for (auto id : ids.value()) {
        auto table_name = base_dir + "/sst." + std::to_string(id);
        level0.emplace_back(std::make_shared<SSTable>(table_name));
      }
      while (true) {
        ids = read_levels(fd);
        if (!ids.has_value())
          break;
        std::vector<std::shared_ptr<SSTable>> level = {};
        for (auto id : ids.value()) {
          auto table_name = base_dir + "/sst." + std::to_string(id);
          level.emplace_back(std::make_shared<SSTable>(table_name));
        }
        levels.emplace_back(level);
      }
    }
    std::fclose(fd);
  }

private:
  std::vector<std::shared_ptr<SSTable>> level0;
  std::vector<std::vector<std::shared_ptr<SSTable>>> levels;
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
      auto fd = std::fopen(current_file.c_str(), "w");
      fprintf(fd, "%u", version_number.load());
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
    phantom = new PhantomStorage{latest};
  }

  void StoreImmtable(Memtable *) {}

  void DoCompaction() {}

  ~Versions() {
    // PhantomStorage is leaked here intentionally to prevent latest version
    // from being deleted
  }

  std::shared_ptr<Version> get_latest() { return latest; }

private:
  std::shared_ptr<Version> latest;
  std::atomic<uint32_t> version_number;
  std::string base_dir;
  PhantomStorage *phantom;
};
} // namespace kvs
