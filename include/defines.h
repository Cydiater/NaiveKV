#pragma once

#include <string>
#include <utility>

typedef std::pair<std::string, uint64_t /* LSN */> TaggedKey;
typedef std::pair<std::string, bool /* deleted */> TaggedValue;
typedef std::pair<TaggedKey, TaggedValue> InternalKV;

template <typename scalar>
inline void encode(char *ptr, uint64_t &offset, const scalar &s) {
  memcpy(ptr + offset, &s, sizeof(s));
  offset += sizeof(s);
}

inline void encode_string(char *ptr, uint64_t &offset, const std::string &s) {
  uint32_t len = s.length();
  encode(ptr, offset, len);
  memcpy(ptr + offset, s.c_str(), len);
  offset += len;
}
