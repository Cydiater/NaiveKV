#pragma once
#include <thread>
#ifndef INCLUDE_ENGINE_H_
#define INCLUDE_ENGINE_H_
#include <functional>
#include <semaphore>
#include <string>
#include <vector>

#include "conf.h"
#include "interfaces.h"
#include "log_manager.hpp"
#include "memtable.hpp"
#include "options.h"
#include "versions.hpp"

namespace kvs {

class Engine : public IEngine {
public:
  Engine(const std::string &path, EngineOptions options);

  static Pointer new_instance(const std::string &path, EngineOptions options) {
    return std::make_shared<Engine>(path, options);
  }

  virtual ~Engine();

  RetCode put(const Key &key, const Value &value) override;
  RetCode remove(const Key &key) override;
  RetCode get(const Key &key, Value &value) override;

  RetCode sync() override;

  RetCode visit(const Key &lower, const Key &upper,
                const Visitor &visitor) override;
  std::shared_ptr<IROEngine> snapshot() override;

  RetCode garbage_collect() override;

private:
  void background();
  void schedule_bg();
  void check_mem();

  std::atomic<uint64_t> current_lsn_;
  std::unique_ptr<Memtable> mut_;
  std::unique_ptr<Memtable> imm_;
  std::unique_ptr<LogManager> log_mgr_;
  std::unique_ptr<Versions> versions_;

  std::shared_mutex
      checking_mem /* make sure that mut_ and imm_ stay unchanged */;
  std::binary_semaphore do_compaction{0};
  bool bg_scheduled, killed;
  std::thread bg_work;
};

} // namespace kvs

#endif // INCLUDE_ENGINE_H_
