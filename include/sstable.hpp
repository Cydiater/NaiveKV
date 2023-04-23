#pragma once

#include <vector>

#include "ordered_iteratable.hpp"

namespace kvs {

class SSTableBuilder {
public:
  SSTableBuilder(std::vector<std::unique_ptr<OrderedIterater>> sources)
      : sources_{sources} {}

  void build() {}

private:
  std::vector<std::unique_ptr<OrderedIterater>> sources_;
};

class SSTable {};

class SSTableIterator : public OrderedIterater {};

} // namespace kvs
