#pragma once

#include <cassert>
#include <cstdio>
#include <filesystem>
#include <mutex>
#include <ostream>
#include <string>

#include "conf.h"
#include "defines.h"

namespace kvs {

class LogManager {
public:
  LogManager(const std::string &path)
      : mode(Mode::Recovering), filename_imm_log(path + "/imm.log"),
        filename_mem_log(path + "/mem.log"), log_size{0} {}

  ~LogManager() {
    if (mode == Mode::Logging) {
      std::fflush(mem_fd);
      std::fclose(mem_fd);
    }
  }

  std::pair<std::vector<InternalKV>, std::vector<InternalKV>>
  dump_for_recovering() {
    std::vector<InternalKV> imm_init = {}, mem_init = {};
    char key_buf[kMaxKeySize + 1], val_buf[kMaxValueSize + 1];
    uint64_t lsn;
    int rm;
    auto *imm_fd = std::fopen(filename_imm_log.c_str(), "r");
    if (imm_fd != NULL) {
      while (fscanf(imm_fd, "%s %llu %s %d", key_buf, &lsn, val_buf, &rm) !=
             EOF) {
        TaggedKey tagged_key = {std::string(key_buf), lsn};
        TaggedValue tagged_val = {std::string{val_buf}, rm};
        imm_init.push_back({tagged_key, tagged_val});
      }
      std::fclose(imm_fd);
    }
    auto *mem_fd = std::fopen(filename_mem_log.c_str(), "r");
    if (mem_fd != NULL) {
      while (fscanf(mem_fd, "%s %llu %s %d", key_buf, &lsn, val_buf, &rm) !=
             EOF) {
        TaggedKey tagged_key = {std::string(key_buf), lsn};
        TaggedValue tagged_val = {std::string{val_buf}, rm};
        mem_init.push_back({tagged_key, tagged_val});
      }
      std::fclose(mem_fd);
    }
    mode = Mode::Logging;
    this->mem_fd = std::fopen(filename_mem_log.c_str(), "a");
    return {imm_init, mem_init};
  }

  void log(const InternalKV &kv) {
    auto lock = std::lock_guard<std::mutex>(mutex_);
    assert(mem_fd != NULL);
    assert(mode == Mode::Logging);
    const auto &[tagged_key, tagged_val] = kv;
    log_size +=
        fprintf(mem_fd, "%s %llu %s %d\n", tagged_key.first.c_str(),
                tagged_key.second, tagged_val.first.c_str(), tagged_val.second);
  }

  uint32_t get_log_size() const { return log_size; }

  void flush() {
    assert(mode == Mode::Logging);
    std::fflush(mem_fd);
  }

  void flush_and_reset() {
    flush();
    std::fclose(mem_fd);
    assert(!std::filesystem::exists(filename_imm_log));
    std::rename(filename_mem_log.c_str(), filename_imm_log.c_str());
    mem_fd = std::fopen(filename_mem_log.c_str(), "a");
  }

private:
  enum class Mode {
    Recovering,
    Logging,
  } mode;
  const std::string filename_imm_log;
  const std::string filename_mem_log;
  FILE *mem_fd;
  std::mutex mutex_;
  uint32_t log_size;
};

} // namespace kvs
