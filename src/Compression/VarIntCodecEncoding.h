#pragma once

#include <Common/Exception.h>
#include <base/types.h>

#include <cstring>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_DECOMPRESS;
}

namespace VarIntCodecEncoding
{

/// Encode a signed 64-bit integer using variable-length encoding (for DoubleDelta + VarInt chain).
/// Returns number of bytes written.
inline size_t encodeSignedInt64(UInt8 * dest, Int64 value)
{
    /// Range [-63, 64] -> [0, 127] -> 1 byte
    if (value >= -63 && value <= 64)
    {
        dest[0] = static_cast<UInt8>(value + 63); /// 0x00-0x7F
        return 1;
    }

    /// Range [-8191, 8192] -> 2 bytes
    if (value >= -8191 && value <= 8192)
    {
        UInt16 encoded = static_cast<UInt16>(value + 8191);
        dest[0] = static_cast<UInt8>(0x80 | (encoded >> 8)); /// 10xxxxxx
        dest[1] = static_cast<UInt8>(encoded & 0xFF);
        return 2;
    }

    /// Range [-1048575, 1048576] -> 3 bytes
    if (value >= -1048575 && value <= 1048576)
    {
        UInt32 encoded = static_cast<UInt32>(value + 1048575);
        dest[0] = static_cast<UInt8>(0xC0 | (encoded >> 16)); /// 110xxxxx
        dest[1] = static_cast<UInt8>((encoded >> 8) & 0xFF);
        dest[2] = static_cast<UInt8>(encoded & 0xFF);
        return 3;
    }

    /// Range [-2^31, 2^31] -> 5 bytes
    if (value >= std::numeric_limits<Int32>::min() && value <= std::numeric_limits<Int32>::max())
    {
        dest[0] = 0xE0; /// 1110xxxx
        Int32 narrow = static_cast<Int32>(value);
        memcpy(dest + 1, &narrow, 4);
        return 5;
    }

    /// Full 64-bit value -> 9 bytes
    dest[0] = 0xF0; /// 1111xxxx
    memcpy(dest + 1, &value, 8);
    return 9;
}

/// Decode a variable-length integer. Returns number of bytes consumed.
inline size_t decodeSignedInt64(const UInt8 * src, const UInt8 * src_end, Int64 & value)
{
    if (src >= src_end)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt codec: unexpected end of data");

    UInt8 header = src[0];

    if ((header & 0x80) == 0) /// 0xxxxxxx
    {
        value = static_cast<Int64>(header) - 63;
        return 1;
    }

    if ((header & 0xC0) == 0x80) /// 10xxxxxx
    {
        if (src + 2 > src_end)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt codec: unexpected end of data");
        UInt16 encoded = static_cast<UInt16>((static_cast<UInt16>(header & 0x3F) << 8) | static_cast<UInt8>(src[1]));
        value = static_cast<Int64>(encoded) - 8191;
        return 2;
    }

    if ((header & 0xE0) == 0xC0) /// 110xxxxx
    {
        if (src + 3 > src_end)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt codec: unexpected end of data");
        UInt32 encoded = (static_cast<UInt32>(header & 0x1F) << 16)
            | (static_cast<UInt32>(src[1]) << 8)
            | src[2];
        value = static_cast<Int64>(encoded) - 1048575;
        return 3;
    }

    if ((header & 0xF0) == 0xE0) /// 1110xxxx
    {
        if (src + 5 > src_end)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt codec: unexpected end of data");
        Int32 narrow;
        memcpy(&narrow, src + 1, 4);
        value = narrow;
        return 5;
    }

    /// 1111xxxx - full 64-bit
    if (src + 9 > src_end)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt codec: unexpected end of data");
    memcpy(&value, src + 1, 8);
    return 9;
}

}

}
