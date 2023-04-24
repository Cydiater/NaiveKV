#pragma once

#include "defines.h"
#include <optional>

namespace kvs {

class OrderedIterater {
public:
  virtual std::optional<InternalKV> next() = 0;
  virtual ~OrderedIterater() = default;
};

} // namespace kvs
