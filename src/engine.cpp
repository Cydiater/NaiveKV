#include "engine.h"
#include "interfaces.h"
#include "memtable.hpp"
#include <memory>

namespace kvs {
Engine::Engine(const std::string &path, EngineOptions options)
    : log_mgr_(std::make_unique<LogManager>(path)) {
  std::ignore = path;
  std::ignore = options;
  mut_ = std::make_unique<Memtable>();
  current_lsn_ = 0;
  const auto &[imm_init, mem_init] = log_mgr_->dump_for_recovering();
  imm_ = std::make_unique<Memtable>(imm_init);
  mut_ = std::make_unique<Memtable>(mem_init);
  for (auto &kv : imm_init)
    current_lsn_ = std::max(kv.first.second, current_lsn_.load());
  for (auto &kv : mem_init)
    current_lsn_ = std::max(kv.first.second, current_lsn_.load());
}

Engine::~Engine() {}

RetCode Engine::put(const Key &key, const Value &value) {
  auto lsn = current_lsn_.fetch_add(1);
  mut_->insert({key, lsn}, value, log_mgr_.get());
  return RetCode::kSucc;
}
RetCode Engine::remove(const Key &key) {
  auto lsn = current_lsn_.fetch_add(1);
  return mut_->remove({key, lsn}, log_mgr_.get());
}

RetCode Engine::get(const Key &key, Value &value) {
  return mut_->get({key, current_lsn_.fetch_add(1)}, value);
}

RetCode Engine::sync() {
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
