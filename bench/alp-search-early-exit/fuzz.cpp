// Randomized exactness check: master's and the branch's ALP encoders (AUTO and STD) must write the same bytes for every block.
// Blocks of 1..2,100 values mix decimals of 0-12 digits, integers, full-precision doubles, repeated values, NaN, +-inf, -0,
// subnormals and values near the Int64 limits of the encoder, in runs, so that consecutive samples share or change their best pair.
#include <Common/Exception.h>
#include <Compression/FFOR.h>
#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <limits>
#include <random>
#include <string>
#include <vector>

namespace M { using namespace DB;
#include "enc_master.h"
}
namespace B { using namespace DB;
#ifndef BRANCH_HEADER
#define BRANCH_HEADER "enc_branch.h"
#endif
#include BRANCH_HEADER
}

template <typename F>
static F value(std::mt19937_64 & rng, int kind, F base)
{
    std::uniform_real_distribution<double> u(0, 1);
    static const double p10[] = {1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12};
    switch (kind)
    {
        case 0: { const int d = static_cast<int>(rng() % 13); return static_cast<F>(std::round((base + u(rng) * 100) * p10[d]) / p10[d]); }
        case 1: return static_cast<F>(static_cast<double>(static_cast<int64_t>(rng() % 2000000) - 1000000));
        case 2: return static_cast<F>(base + u(rng));
        case 3: return base;
        case 4:
        {
            switch (rng() % 8)
            {
                case 0: return std::numeric_limits<F>::quiet_NaN();
                case 1: return std::numeric_limits<F>::infinity();
                case 2: return -std::numeric_limits<F>::infinity();
                case 3: return static_cast<F>(-0.0);
                case 4: return std::numeric_limits<F>::denorm_min() * static_cast<F>(rng() % 1000);
                case 5: return static_cast<F>(9.2e18 * (u(rng) - 0.5) * 2);
                case 6: return static_cast<F>(std::ldexp(1.0, static_cast<int>(rng() % 120) - 60));
                default: return static_cast<F>((u(rng) - 0.5) * 1e300);
            }
        }
        default: return static_cast<F>(std::round(u(rng) * 1e6) / 1e3 + base);
    }
}

template <typename F>
static bool check(std::mt19937_64 & rng, size_t n, std::vector<char> & a, std::vector<char> & b)
{
    std::vector<F> v(n);
    size_t i = 0;
    while (i < n)
    {
        const int kind = static_cast<int>(rng() % 6);
        const F base = static_cast<F>(static_cast<double>(rng() % 100000) / (1 + rng() % 1000));
        const size_t run = 1 + rng() % 300;
        for (size_t j = 0; j < run && i < n; ++j, ++i)
            v[i] = (rng() % 50 == 0) ? value<F>(rng, 4, base) : value<F>(rng, kind, base);
    }
    const char * src = reinterpret_cast<const char *>(v.data());
    const UInt32 size = static_cast<UInt32>(n * sizeof(F));
    // AUTO: STD if the search says so, else RD (as CompressionCodecALP::doCompressData).
    UInt32 na = 0, nb = 0;
    const bool sa = M::ALPCodecEncoder<F>().tryEncodeWithAuto(src, size, a.data(), na);
    const bool sb = B::ALPCodecEncoder<F>().tryEncodeWithAuto(src, size, b.data(), nb);
    if (sa != sb || (sa && (na != nb || std::memcmp(a.data(), b.data(), na) != 0)))
        return false;
    // STD only (ALP(STD)).
    na = M::ALPCodecEncoder<F>().encode(src, size, a.data());
    nb = B::ALPCodecEncoder<F>().encode(src, size, b.data());
    return na == nb && std::memcmp(a.data(), b.data(), na) == 0;
}

int main(int argc, char ** argv)
{
    const size_t iterations = argc > 1 ? std::stoul(argv[1]) : 100000;
    std::mt19937_64 rng(argc > 2 ? std::stoull(argv[2]) : 42);
    std::vector<char> a(1 << 20), b(1 << 20);
    size_t blocks = 0, mismatches = 0;
    for (size_t it = 0; it < iterations; ++it)
    {
        const size_t n = 1 + rng() % 2100;
        const bool ok = (it % 2) ? check<float>(rng, n, a, b) : check<double>(rng, n, a, b);
        ++blocks;
        if (!ok)
        {
            ++mismatches;
            if (mismatches <= 5)
                fprintf(stderr, "MISMATCH iteration %zu n=%zu type=%s\n", it, n, (it % 2) ? "f32" : "f64");
        }
    }
    printf("blocks=%zu mismatches=%zu\n", blocks, mismatches);
    return mismatches != 0;
}
