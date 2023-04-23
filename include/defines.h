#pragma once

#include <utility>

typedef std::pair<std::string, uint64_t /* LSN */> TaggedKey;
typedef std::pair<std::string, bool /* deleted */> TaggedValue;
