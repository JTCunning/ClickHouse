// Standalone A/B of the ALP encoder: the encoder half of src/Compression/CompressionCodecALP.cpp at master and at the branch,
// extracted verbatim by extract.sh, run over a value stream cut into compress blocks of N values (one granule of a compact part).
// Reports the bytes after ALP, after ALP + ZSTD(3) (the TimeSeries default chain), and the ALP encode CPU time.
#include <Common/Exception.h>
#include <Compression/FFOR.h>
#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <fstream>
#include <limits>
#include <string>
extern "C" size_t ZSTD_compress(void *, size_t, const void *, size_t, int);
extern "C" size_t ZSTD_compressBound(size_t);

namespace M { using namespace DB;
#include "enc_master.h"
}
namespace B { using namespace DB;
#ifndef BRANCH_HEADER
#define BRANCH_HEADER "enc_branch.h"
#endif
#include BRANCH_HEADER
}
namespace Dm { using namespace DB;
#include "enc_master.h"
#include "dec_master.h"
}

template <typename STD, typename RD>
static UInt32 compressAuto(const char * src, UInt32 size, char * dst, bool & is_rd)
{
    // Mirrors CompressionCodecALP::doCompressData for the DEFAULT / AUTO variant: try STD, fall back to RD.
    char * d = dst + 4;
    UInt32 n = 0;
    is_rd = false;
    if (!STD().tryEncodeWithAuto(src, size, d, n)) { n = RD().encode(src, size, d); is_rd = true; }
    return n + 4;
}

int main(int argc, char ** argv)
{
    if (argc < 4) { fprintf(stderr, "harness <file.f64> <block values> <reps> [f32]\n"); return 1; }
    std::ifstream f(argv[1], std::ios::binary);
    std::vector<char> data((std::istreambuf_iterator<char>(f)), {});
    // Optional 4th argument "f32": run the Float32 encoder on the values converted to Float32.
    const bool f32 = argc > 4 && std::string(argv[4]) == "f32";
    if (f32)
    {
        std::vector<char> narrow(data.size() / 2);
        for (size_t i = 0; i < data.size() / 8; ++i)
        {
            double d; std::memcpy(&d, &data[i * 8], 8);
            const float x = static_cast<float>(d); std::memcpy(&narrow[i * 4], &x, 4);
        }
        data.swap(narrow);
    }
    const size_t width = f32 ? sizeof(float) : sizeof(double);
    const size_t block = std::stoul(argv[2]) * width;
    const int reps = std::stoi(argv[3]);
    std::vector<char> out(block * 2 + 4096), z(ZSTD_compressBound(block * 2 + 4096));

    auto run = [&](auto tag, const char * name)
    {
        using T = decltype(tag);
        std::vector<double> times;
        size_t alp_bytes = 0, zstd_bytes = 0, rd_blocks = 0, blocks = 0;
        for (int r = 0; r < reps; ++r)
        {
            alp_bytes = zstd_bytes = rd_blocks = blocks = 0;
            double t = 0;
            for (size_t off = 0; off < data.size(); off += block)
            {
                const UInt32 sz = static_cast<UInt32>(std::min(block, data.size() - off));
                bool is_rd;
                auto t0 = std::chrono::steady_clock::now();
                UInt32 n;
                if (f32)
                {
                    if constexpr (std::is_same_v<T, int>) n = compressAuto<M::ALPCodecEncoder<float>, M::ALPRDCodecEncoder<float>>(&data[off], sz, out.data(), is_rd);
                    else n = compressAuto<B::ALPCodecEncoder<float>, B::ALPRDCodecEncoder<float>>(&data[off], sz, out.data(), is_rd);
                }
                else
                {
                    if constexpr (std::is_same_v<T, int>) n = compressAuto<M::ALPCodecEncoder<double>, M::ALPRDCodecEncoder<double>>(&data[off], sz, out.data(), is_rd);
                    else n = compressAuto<B::ALPCodecEncoder<double>, B::ALPRDCodecEncoder<double>>(&data[off], sz, out.data(), is_rd);
                }
                t += std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
                alp_bytes += n; rd_blocks += is_rd; ++blocks;
                zstd_bytes += ZSTD_compress(z.data(), z.size(), out.data(), n, 3);
                // Round trip through the (unchanged) master decoder, as CompressionCodecALP::doDecompressData does.
                std::vector<char> back(sz + 64);
                UInt32 m = f32 ? (is_rd ? Dm::ALPRDCodecDecoder<float>().decode(out.data() + 4, n - 4, back.data(), sz)
                                        : Dm::ALPCodecDecoder<float>().decode(out.data() + 4, n - 4, back.data(), sz))
                               : (is_rd ? Dm::ALPRDCodecDecoder<double>().decode(out.data() + 4, n - 4, back.data(), sz)
                                        : Dm::ALPCodecDecoder<double>().decode(out.data() + 4, n - 4, back.data(), sz));
                if (m != sz || std::memcmp(back.data(), &data[off], sz) != 0) { fprintf(stderr, "%s: ROUND TRIP MISMATCH at block %zu\n", name, blocks); return; }
            }
            times.push_back(t);
        }
        std::sort(times.begin(), times.end());
        printf("%-6s blocks=%zu rd_blocks=%zu alp_bytes=%zu alp_zstd_bytes=%zu alp_ms_median=%.1f min=%.1f max=%.1f\n", name, blocks,
               rd_blocks, alp_bytes, zstd_bytes, times[times.size() / 2], times.front(), times.back());
    };
    for (int i = 0; i < 1; ++i) { run(int{}, "master"); run(char{}, "branch"); }
    size_t zonly = 0;
    for (size_t off = 0; off < data.size(); off += block)
        zonly += ZSTD_compress(z.data(), z.size(), &data[off], std::min(block, data.size() - off), 3);
    printf("zstd_only_bytes=%zu raw_bytes=%zu\n", zonly, data.size());
}
