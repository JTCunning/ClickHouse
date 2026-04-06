#pragma clang diagnostic ignored "-Wreserved-identifier"

#include <Common/SipHash.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Compression/VarIntCodecEncoding.h>
#include <DataTypes/IDataType.h>
#include <base/unaligned.h>

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTLiteral.h>

#include <cstring>
#include <limits>
#include <type_traits>


namespace DB
{

/** VarInt codec: re-encodes the Int64 delta-of-delta tail produced by DoubleDeltaWide (method DoubleDeltaWide).
 *  Only valid in CODEC(DoubleDelta, VarInt, ...) — enforced in CompressionCodecFactory.
 */
class CompressionCodecVarInt : public ICompressionCodec
{
public:
    explicit CompressionCodecVarInt(UInt8 data_bytes_size_);

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }
    bool isDeltaCompression() const override { return false; }

    String getDescription() const override
    {
        return "Variable-length encoding of Int64 tails from DoubleDelta (chain with DoubleDelta).";
    }

private:
    UInt8 data_bytes_size;
};


namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
    extern const int BAD_ARGUMENTS;
}

UInt8 getVarIntCodecDataBytesSize(const IDataType * column_type)
{
    if (!column_type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec VarInt is not applicable for {} because the data type is not numeric", column_type->getName());

    const size_t max_size = column_type->getSizeOfValueInMemory();
    if (max_size == 1 || max_size == 2 || max_size == 4 || max_size == 8)
        return static_cast<UInt8>(max_size);

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Codec VarInt is only applicable for data types of size 1, 2, 4, 8 bytes. Given type {}",
        column_type->getName());
}

namespace
{

template <typename ValueType>
UInt32 compressInnerWideToVarInt(const UInt8 * in, UInt32 inner_size, UInt8 * out)
{
    static_assert(std::is_unsigned_v<ValueType>, "ValueType must be unsigned");
    using SignedType = std::make_signed_t<ValueType>;
    const UInt8 * in_end = in + inner_size;

    if (inner_size < sizeof(UInt32))
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with VarInt codec: inner data too short");

    UInt32 count = unalignedLoadLittleEndian<UInt32>(in);
    in += sizeof(UInt32);

    UInt8 * out_start = out;
    unalignedStoreLittleEndian<UInt32>(out, count);
    out += sizeof(UInt32);

    if (count == 0)
        return static_cast<UInt32>(out - out_start);

    if (in + sizeof(ValueType) > in_end)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with VarInt codec: unexpected end of inner data");

    ValueType first_value = unalignedLoadLittleEndian<ValueType>(in);
    unalignedStoreLittleEndian<ValueType>(out, first_value);
    in += sizeof(ValueType);
    out += sizeof(ValueType);

    if (count == 1)
        return static_cast<UInt32>(out - out_start);

    if (in + sizeof(SignedType) > in_end)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with VarInt codec: unexpected end of inner data");

    SignedType first_delta = unalignedLoadLittleEndian<SignedType>(in);
    unalignedStoreLittleEndian<SignedType>(out, first_delta);
    in += sizeof(SignedType);
    out += sizeof(SignedType);

    if (count == 2)
        return static_cast<UInt32>(out - out_start);

    for (UInt32 i = 2; i < count; ++i)
    {
        if (in + sizeof(Int64) > in_end)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with VarInt codec: unexpected end of inner data");
        Int64 double_delta = unalignedLoadLittleEndian<Int64>(in);
        in += sizeof(Int64);
        out += VarIntCodecEncoding::encodeSignedInt64(out, double_delta);
    }

    return static_cast<UInt32>(out - out_start);
}

template <typename ValueType>
UInt32 decompressInnerVarIntToWide(const UInt8 * in, UInt32 inner_size, UInt8 * out, UInt32 expected_output_size)
{
    static_assert(std::is_unsigned_v<ValueType>, "ValueType must be unsigned");
    using SignedType = std::make_signed_t<ValueType>;
    const UInt8 * in_end = in + inner_size;

    if (inner_size < sizeof(UInt32))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt codec: inner data too short");

    UInt32 count = unalignedLoadLittleEndian<UInt32>(in);
    in += sizeof(UInt32);

    if (count * sizeof(ValueType) != expected_output_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt codec: size mismatch");

    UInt8 * out_start = out;
    unalignedStoreLittleEndian<UInt32>(out, count);
    out += sizeof(UInt32);

    if (count == 0)
        return static_cast<UInt32>(out - out_start);

    if (in + sizeof(ValueType) > in_end)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt codec: unexpected end of inner data");

    ValueType first_value = unalignedLoadLittleEndian<ValueType>(in);
    unalignedStoreLittleEndian<ValueType>(out, first_value);
    in += sizeof(ValueType);
    out += sizeof(ValueType);

    if (count == 1)
        return static_cast<UInt32>(out - out_start);

    if (in + sizeof(SignedType) > in_end)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt codec: unexpected end of inner data");

    SignedType first_delta = unalignedLoadLittleEndian<SignedType>(in);
    unalignedStoreLittleEndian<SignedType>(out, first_delta);
    in += sizeof(SignedType);
    out += sizeof(SignedType);

    if (count == 2)
        return static_cast<UInt32>(out - out_start);

    for (UInt32 i = 2; i < count; ++i)
    {
        Int64 double_delta = 0;
        in += VarIntCodecEncoding::decodeSignedInt64(in, in_end, double_delta);
        unalignedStoreLittleEndian<Int64>(out, double_delta);
        out += sizeof(Int64);
    }

    return static_cast<UInt32>(out - out_start);
}

}


CompressionCodecVarInt::CompressionCodecVarInt(UInt8 data_bytes_size_)
    : data_bytes_size(data_bytes_size_)
{
    setCodecDescription("VarInt", {make_intrusive<ASTLiteral>(static_cast<UInt64>(data_bytes_size))});
}

uint8_t CompressionCodecVarInt::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::VarInt);
}

void CompressionCodecVarInt::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
    hash.update(data_bytes_size);
}

UInt32 CompressionCodecVarInt::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    /// Worst case: each Int64 tail byte becomes up to 9 bytes of varint; keep a simple bound.
    return uncompressed_size * 2;
}

UInt32 CompressionCodecVarInt::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    if (source_size < 2)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with VarInt codec: data too short");

    UInt8 bytes_size = static_cast<UInt8>(source[0]);
    if (bytes_size != data_bytes_size)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with VarInt codec: data width mismatch");

    UInt8 bytes_to_skip = static_cast<UInt8>(source[1]);
    if (static_cast<UInt32>(2 + bytes_to_skip) > source_size)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with VarInt codec: invalid header");

    memcpy(dest, source, 2 + bytes_to_skip);
    const UInt8 * inner_in = reinterpret_cast<const UInt8 *>(source + 2 + bytes_to_skip);
    UInt32 inner_size = source_size - 2 - bytes_to_skip;
    UInt8 * inner_out = reinterpret_cast<UInt8 *>(dest + 2 + bytes_to_skip);

    UInt32 inner_compressed = 0;
    switch (data_bytes_size)
    {
        case 1:
            inner_compressed = compressInnerWideToVarInt<UInt8>(inner_in, inner_size, inner_out);
            break;
        case 2:
            inner_compressed = compressInnerWideToVarInt<UInt16>(inner_in, inner_size, inner_out);
            break;
        case 4:
            inner_compressed = compressInnerWideToVarInt<UInt32>(inner_in, inner_size, inner_out);
            break;
        case 8:
            inner_compressed = compressInnerWideToVarInt<UInt64>(inner_in, inner_size, inner_out);
            break;
        default:
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Unsupported data size {} for VarInt codec", static_cast<int>(data_bytes_size));
    }

    return 2 + bytes_to_skip + inner_compressed;
}

UInt32 CompressionCodecVarInt::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < 2)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt-encoded data: wrong header");

    UInt8 bytes_size = static_cast<UInt8>(source[0]);
    if (bytes_size != data_bytes_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt-encoded data: width mismatch");

    UInt8 bytes_to_skip = uncompressed_size % bytes_size;
    if (static_cast<UInt32>(2 + bytes_to_skip) > source_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt-encoded data: wrong header");

    memcpy(dest, source, 2 + bytes_to_skip);
    const UInt8 * inner_in = reinterpret_cast<const UInt8 *>(source + 2 + bytes_to_skip);
    UInt32 inner_size = source_size - 2 - bytes_to_skip;
    UInt8 * inner_out = reinterpret_cast<UInt8 *>(dest + 2 + bytes_to_skip);
    UInt32 output_size = uncompressed_size - bytes_to_skip;

    switch (bytes_size)
    {
        case 1:
            decompressInnerVarIntToWide<UInt8>(inner_in, inner_size, inner_out, output_size);
            break;
        case 2:
            decompressInnerVarIntToWide<UInt16>(inner_in, inner_size, inner_out, output_size);
            break;
        case 4:
            decompressInnerVarIntToWide<UInt32>(inner_in, inner_size, inner_out, output_size);
            break;
        case 8:
            decompressInnerVarIntToWide<UInt64>(inner_in, inner_size, inner_out, output_size);
            break;
        default:
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress VarInt-encoded data: unsupported width {}", static_cast<int>(bytes_size));
    }

    return uncompressed_size;
}

CompressionCodecPtr createCompressionCodecVarInt(const ASTPtr & arguments, const IDataType * column_type)
{
    UInt8 data_bytes_size = 1;
    if (column_type != nullptr)
        data_bytes_size = getVarIntCodecDataBytesSize(column_type);

    if (arguments && !arguments->children.empty())
    {
        if (arguments->children.size() > 1)
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "VarInt codec must have at most 1 parameter, given {}", arguments->children.size());

        const auto * literal = arguments->children[0]->as<ASTLiteral>();
        if (!literal || literal->value.getType() != Field::Types::Which::UInt64)
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "VarInt codec argument must be unsigned integer");

        const size_t user_bytes_size = literal->value.safeGet<UInt64>();
        if (user_bytes_size != 1 && user_bytes_size != 2 && user_bytes_size != 4 && user_bytes_size != 8)
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Argument value for VarInt codec can be 1, 2, 4 or 8, given {}", user_bytes_size);

        data_bytes_size = static_cast<UInt8>(user_bytes_size);
    }

    return std::make_shared<CompressionCodecVarInt>(data_bytes_size);
}

void registerCodecVarInt(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::VarInt);

    factory.registerCompressionCodecCodeOnly(
        method_code,
        [](const ASTPtr &, const IDataType *) { return std::make_shared<CompressionCodecVarInt>(8); });
}

}
