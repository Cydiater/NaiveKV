#include "engine.h"
#include "interfaces.h"
#include "memtable.hpp"
#include <memory>

namespace kvs {
Engine::Engine(const std::string &path, EngineOptions options) {
  std::ignore = path;
  std::ignore = options;
  mut_ = std::make_unique<Memtable>();
  current_lsn_ = 0;
}

Engine::~Engine() {}

RetCode Engine::put(const Key &key, const Value &value) {
  mut_->insert({key, current_lsn_++}, value);
  return RetCode::kSucc;
}
RetCode Engine::remove(const Key &key) {
  return mut_->remove({key, current_lsn_++});
}

RetCode Engine::get(const Key &key, Value &value) {
  return mut_->get({key, current_lsn_++}, value);
}

RetCode Engine::sync() { return kNotSupported; }

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
