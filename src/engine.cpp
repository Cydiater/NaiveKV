#include "engine.h"

namespace kvs {
Engine::Engine(const std::string &path, EngineOptions options) {
  std::ignore = path;
  std::ignore = options;
}

Engine::~Engine() {}

RetCode Engine::put(const Key &key, const Value &value) {
  std::ignore = key;
  std::ignore = value;
  return kNotSupported;
}
RetCode Engine::remove(const Key &key) {
  std::ignore = key;
  return kNotSupported;
}

RetCode Engine::get(const Key &key, Value &value) {
  std::ignore = key;
  std::ignore = value;
  return kNotSupported;
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
