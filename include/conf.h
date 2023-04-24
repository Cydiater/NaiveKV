#pragma once

#include <algorithm>

namespace kvs {
constexpr static const char *kDefaultTestDir = "./engine_test/";
constexpr static const char *kDefaultBenchDir = "./engine_bench/";
constexpr static size_t kMaxKeySize = 4 * 1024;    // 4KB
constexpr static size_t kMaxValueSize = 16 * 1024; // 16MB
constexpr static size_t kMaxKeyValueSize = std::max(kMaxKeySize, kMaxValueSize);
constexpr static size_t kMaxLogSize = 4 * 1024 * 1024; // 4MB
constexpr static size_t kMaxTableSize = 2 * 1024 * 1024;
constexpr static size_t kMaxBlockSize = 4 * 1024;
} // namespace kvs
