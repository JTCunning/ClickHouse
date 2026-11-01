#pragma once
// Minimal stand-ins for the ClickHouse headers the ALP encoder and FFOR need, so the encoder can be compiled standalone.
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>
#include <unordered_map>
#include <concepts>
#include <cassert>
#include <array>
#include <utility>
#include <bit>
using UInt8 = uint8_t; using UInt16 = uint16_t; using UInt32 = uint32_t; using UInt64 = uint64_t;
using Int8 = int8_t; using Int16 = int16_t; using Int32 = int32_t; using Int64 = int64_t;
using Float32 = float; using Float64 = double;
#define ALWAYS_INLINE __attribute__((always_inline))
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#define chassert(x) assert(x)
template <typename T> concept UnsignedInteger = std::is_unsigned_v<T> && std::is_integral_v<T>;
template <typename T> inline T unalignedLoadLittleEndian(const void * p) { T v; std::memcpy(&v, p, sizeof(T)); return v; }
template <typename T> inline void unalignedStoreLittleEndian(void * p, const T & v) { std::memcpy(p, &v, sizeof(T)); }
template <typename T> using VectorWithMemoryTracking = std::vector<T>;
template <typename K, typename V> using UnorderedMapWithMemoryTracking = std::unordered_map<K, V>;
namespace DB
{
namespace ErrorCodes { inline const int BAD_ARGUMENTS = 36; inline const int CANNOT_COMPRESS = 431; inline const int LOGICAL_ERROR = 49; inline const int CANNOT_DECOMPRESS = 432; }
struct Exception : std::runtime_error
{
    template <typename... Args> Exception(int, const char * msg, Args &&...) : std::runtime_error(msg) {}
};
}
