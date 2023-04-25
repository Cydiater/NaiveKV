#include "engine.h"
#include "interfaces.h"
#include "memtable.hpp"
#include <cstdio>
#include <memory>
#include <mutex>
#include <ostream>
#include <thread>

namespace kvs {
Engine::Engine(const std::string &path, EngineOptions options)
    : log_mgr_(std::make_unique<LogManager>(path)),
      versions_(std::make_unique<Versions>(path)) {
  std::ignore = options;
  mut_ = std::make_unique<Memtable>();
  current_lsn_ = 0;
  const auto &[imm_init, mem_init] = log_mgr_->dump_for_recovering();
  imm_ = nullptr;
  mut_ = std::make_unique<Memtable>(mem_init);
  if (!imm_init.empty())
    imm_ = std::make_unique<Memtable>(imm_init);
  for (auto &kv : imm_init)
    current_lsn_ = std::max(kv.first.second, current_lsn_.load());
  for (auto &kv : mem_init)
    current_lsn_ = std::max(kv.first.second, current_lsn_.load());
  bg_work = std::thread(&Engine::background, this);
  bg_scheduled = false;
  killed = false;
}

void Engine::background() {
  while (true) {
    do_compaction.acquire();
    if (killed)
      break;
    {
      auto lock = std::unique_lock<std::shared_mutex>(checking_mem);
      if (imm_ != nullptr) {
        versions_->store_immtable(imm_.get());
        log_mgr_->rm_imm_log();
        imm_ = nullptr;
      }
    }
    versions_->schedule_compaction();
    bg_scheduled = false;
  }
}

void Engine::schedule_bg() {
  if (bg_scheduled == false) {
    bg_scheduled = true;
    do_compaction.release();
  }
}

void Engine::check_mem() {
  auto lock = std::unique_lock<std::shared_mutex>(checking_mem);
  if (log_mgr_->get_log_size() >= kMaxLogSize) {
    if (imm_.get() != nullptr) {
      return;
    }
    imm_.swap(mut_);
    mut_ = std::make_unique<Memtable>();
    log_mgr_->flush_and_reset();
    schedule_bg();
  }
}

Engine::~Engine() {
  killed = true;
  schedule_bg();
  bg_work.join();
}

RetCode Engine::put(const Key &key, const Value &value) {
  {
    auto lock = std::shared_lock<std::shared_mutex>(checking_mem);
    auto lsn = current_lsn_.fetch_add(1);
    mut_->insert({key, lsn}, value, log_mgr_.get());
  }
  check_mem();
  return RetCode::kSucc;
}

RetCode Engine::remove(const Key &key) {
  {
    auto lock = std::shared_lock<std::shared_mutex>(checking_mem);
    auto version = versions_->get_latest();
    auto lsn = current_lsn_.fetch_add(1);
    std::string _val;
    auto ret = mut_->get({key, lsn}, _val);
    if (ret == true)
      goto do_remove;
    if (ret == false)
      return kNotFound;
    if (imm_ != nullptr) {
      ret = imm_->get({key, lsn}, _val);
      if (ret == true)
        goto do_remove;
      if (ret == false)
        return kNotFound;
    }
    if (version != nullptr) {
      ret = version->get({key, lsn}, _val);
      if (ret == true) {
        goto do_remove;
      }
      if (ret == false)
        return kNotFound;
    }
    return kNotFound;
  do_remove:
    mut_->remove({key, lsn}, log_mgr_.get());
  }
  check_mem();
  return kSucc;
}

RetCode Engine::get(const Key &key, Value &value) {
  auto lock = std::shared_lock<std::shared_mutex>(checking_mem);
  auto version = versions_->get_latest();
  auto lsn = current_lsn_.fetch_add(1);
  std::string _val;
  auto ret = mut_->get({key, lsn}, _val);
  if (ret == true) {
    value = _val;
    return kSucc;
  }
  if (ret == false) {
    return kNotFound;
  }
  if (imm_ != nullptr) {
    auto ret = imm_->get({key, lsn}, _val);
    if (ret == true) {
      value = _val;
      return kSucc;
    }
    if (ret == false) {
      return kNotFound;
    }
  }
  if (version != nullptr) {
    auto ret = version->get({key, lsn}, _val);
    if (ret == true) {
      value = _val;
      return kSucc;
    }
    if (ret == false) {
      return kNotFound;
    }
  }
  return RetCode::kNotFound;
}

RetCode Engine::sync() {
  auto lock = std::unique_lock<std::shared_mutex>(checking_mem);
  log_mgr_->flush();
  return RetCode::kSucc;
}

RetCode Engine::visit(const Key &lower, const Key &upper,
                      const Visitor &visitor) {
  std::ignore = lower;
  std::ignore = upper;
  std::ignore = visitor;
  return kNotSupported;
}

RetCode Engine::garbage_collect() { return kNotSupported; }

std::shared_ptr<IROEngine> Engine::snapshot() { return nullptr; }

} // namespace kvs
