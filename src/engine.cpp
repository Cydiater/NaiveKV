#include "engine.h"
#include "interfaces.h"
#include "ordered_iteratable.hpp"

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
  if (!imm_init.empty()) {
    imm_ = std::make_unique<Memtable>(imm_init);
    imm_->make_imm();
  }
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
    if (killed)
      break;
    do_compaction.acquire();
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
  if (log_mgr_->get_log_size() >= kMaxLogSize) {
    if (imm_.get() != nullptr) {
      return;
    }
    log_mgr_->flush_and_reset();
    auto lock = std::unique_lock<std::shared_mutex>(checking_mem);
    imm_.swap(mut_);
    imm_->make_imm();
    mut_ = std::make_unique<Memtable>();
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
    std::shared_ptr<Version> version = nullptr;
    auto lock = std::shared_lock<std::shared_mutex>(checking_mem);
    auto lsn = current_lsn_.fetch_add(1);
    std::string _val;
    auto ret = mut_->get({key, lsn}, _val);
    if (ret == true)
      goto do_remove;
    if (ret == false) {
      return kNotFound;
    }
    if (imm_ != nullptr) {
      ret = imm_->get({key, lsn}, _val);
      if (ret == true)
        goto do_remove;
      if (ret == false) {
        return kNotFound;
      }
    }
    version = versions_->get_latest();
    if (version != nullptr) {
      ret = version->get({key, lsn}, _val);
      if (ret == true) {
        goto do_remove;
      }
      if (ret == false) {
        return kNotFound;
      }
    }
    return kNotFound;
  do_remove:
    mut_->remove({key, lsn}, log_mgr_.get());
  }
  check_mem();
  return kSucc;
}

RetCode Engine::get(const Key &key, Value &value) {
  uint64_t lsn;
  std::shared_ptr<Version> version;
  std::string _val;
  {
    auto lock = std::shared_lock<std::shared_mutex>(checking_mem);
    lsn = current_lsn_.fetch_add(1);
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
  }
  version = versions_->get_latest();
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
  auto lock = std::shared_lock<std::shared_mutex>(checking_mem);
  log_mgr_->flush();
  return RetCode::kSucc;
}

RetCode Engine::visit(const Key &lower, const Key &upper,
                      const Visitor &visitor) {
  auto lock = std::shared_lock<std::shared_mutex>(checking_mem);
  auto lsn = current_lsn_.fetch_add(1);
  auto latest = versions_->get_latest();
  auto key = std::make_pair(lower, 0);
  std::set<std::pair<InternalKV, OrderedIterater *>> ds;
  std::deque<InternalKV> imm_res, mut_res;
  if (imm_ != nullptr) {
    imm_res = imm_->lowerbound(key, {upper, lsn});
  }
  if (mut_ != nullptr) {
    mut_res = mut_->lowerbound(key, {upper, lsn});
  }
  std::vector<std::unique_ptr<OrderedIterater>> sources;
  std::map<std::string, std::string> check_kv;
  if (latest != nullptr) {
    sources = latest->fetch_sources({lower, 0}, {upper, lsn});
    for (auto &source : sources) {
      auto kv = source->next();
      if (kv == std::nullopt)
        continue;
      ds.insert({kv.value(), source.get()});
    }
  }
  std::map<std::string, std::string> checking_kv;
  while (true) {
    std::optional<InternalKV> m = std::nullopt;
    if (!imm_res.empty()) {
      m = std::min(m.value_or(imm_res.front()), imm_res.front());
    }
    if (!mut_res.empty()) {
      m = std::min(m.value_or(mut_res.front()), mut_res.front());
    }
    if (!ds.empty()) {
      auto it = ds.begin();
      auto kv = it->first;
      m = std::min(m.value_or(kv), kv);
    }
    if (m == std::nullopt) {
      break;
    }
    assert(m.value().first.second < lsn);
    if (m.value().second.second) {
      if (checking_kv.find(m.value().first.first) != checking_kv.end())
        checking_kv.erase(checking_kv.find(m.value().first.first));
    } else
      checking_kv[m.value().first.first] = m.value().second.first;
    assert(checking_kv.size() <= 2);
    if (checking_kv.size() == 2) {
      auto it = checking_kv.begin();
      auto kv = *it;
      visitor(kv.first, kv.second);
      checking_kv.erase(it);
    }
    TaggedKey next_key = {m.value().first.first, m.value().first.second + 1};
    if (!imm_res.empty() && m == imm_res.front()) {
      imm_res.pop_front();
    } else if (!mut_res.empty() && m == mut_res.front()) {
      mut_res.pop_front();
    } else {
      auto it = ds.begin();
      auto source = it->second;
      ds.erase(it);
      while (true) {
        auto kv = source->next();
        if (kv == std::nullopt || kv.value().first > std::make_pair(upper, lsn))
          break;
        if (kv.value().first.second <= lsn) {
          ds.insert({kv.value(), source});
          break;
        }
      }
    }
  }
  if (!checking_kv.empty()) {
    assert(checking_kv.size() == 1);
    auto it = checking_kv.begin();
    auto kv = *it;
    visitor(kv.first, kv.second);
  }
  return kSucc;
}

RetCode Engine::garbage_collect() { return kSucc; }

std::shared_ptr<IROEngine> Engine::snapshot() {
  auto lock = std::unique_lock(checking_mem);
  auto lsn = current_lsn_.fetch_add(1);
  if (imm_ != nullptr) {
    versions_->store_immtable(imm_.get());
    log_mgr_->rm_imm_log();
    imm_ = nullptr;
  }
  if (mut_ != nullptr) {
    log_mgr_->flush_and_reset();
    mut_->make_imm();
    versions_->store_immtable(mut_.get());
    mut_ = std::make_unique<Memtable>();
  }
  auto snapshot = std::make_shared<ROEngine>(lsn, versions_->get_latest());
  return snapshot;
}

RetCode ROEngine::get(const Key &key, Value &value) {
  auto ret = version_->get({key, lsn_}, value);
  if (ret == true)
    return kSucc;
  return kNotFound;
}

RetCode ROEngine::visit(const Key &lower, const Key &upper,
                        const Visitor &visitor) {
  auto sources = version_->fetch_sources({lower, 0}, {upper, lsn_});
  std::set<std::pair<InternalKV, OrderedIterater *>> ds;
  for (auto &source : sources) {
    auto kv = source->next();
    if (kv == std::nullopt)
      continue;
    ds.insert({kv.value(), source.get()});
  }
  std::map<std::string, std::string> checking_kv;
  while (true) {
    std::optional<InternalKV> m = std::nullopt;
    if (!ds.empty()) {
      auto it = ds.begin();
      auto kv = it->first;
      m = std::min(m.value_or(kv), kv);
    }
    if (m == std::nullopt) {
      break;
    }
    assert(m.value().first.second < lsn_);
    if (m.value().second.second) {
      if (checking_kv.find(m.value().first.first) != checking_kv.end())
        checking_kv.erase(checking_kv.find(m.value().first.first));
    } else
      checking_kv[m.value().first.first] = m.value().second.first;
    assert(checking_kv.size() <= 2);
    if (checking_kv.size() == 2) {
      auto it = checking_kv.begin();
      auto kv = *it;
      visitor(kv.first, kv.second);
      checking_kv.erase(it);
    }
    TaggedKey next_key = {m.value().first.first, m.value().first.second + 1};
    auto it = ds.begin();
    auto source = it->second;
    ds.erase(it);
    while (true) {
      auto kv = source->next();
      if (kv == std::nullopt || kv.value().first > std::make_pair(upper, lsn_))
        break;
      if (kv.value().first.second <= lsn_) {
        ds.insert({kv.value(), source});
        break;
      }
    }
  }
  if (!checking_kv.empty()) {
    assert(checking_kv.size() == 1);
    auto it = checking_kv.begin();
    auto kv = *it;
    visitor(kv.first, kv.second);
  }
  return kSucc;
}

} // namespace kvs
