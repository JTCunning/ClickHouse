// Model of the timeSeries*ToGrid bucket map (HashMap<UInt64, Bucket, TrivialHash, TimeSeriesBucketsHashTableGrower>)
// and of the insert rules of master and of lilred-perf-grid-gap-wraparound, to find where run 2's
// timeseries_to_grid_bucketing slowdown comes from without a ClickHouse build.
//
// The map copies HashTable.h's linear probing: identity hash, place = key & mask, next = +1, zero key stored
// outside the buffer, grow by realloc + in-place reinsert (with the tail pass), 3/4 fill limit, start at 16 cells.
//
// Build (release flags of ClickHouse):
//   clang++-20 -std=c++20 -O3 -march=x86-64-v3 -ffp-contract=off -fno-math-errno -falign-functions=64 \
//     -falign-loops=32 -mbranches-within-32B-boundaries model.cpp -o model
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#define ALWAYS_INLINE __attribute__((always_inline)) inline
#define NO_INLINE __attribute__((noinline))

struct Bucket
{
    int64_t ts = INT64_MIN;
    double v = 0;
    ALWAYS_INLINE void addMany(const int64_t * t, const double * x, size_t n)
    {
        for (size_t i = 0; i < n; ++i)
            if (t[i] >= ts) { ts = t[i]; v = x[i]; }
    }
    ALWAYS_INLINE void merge(const Bucket & o) { if (o.ts >= ts) { ts = o.ts; v = o.v; } }
};

struct Cell
{
    uint64_t key;
    Bucket m;
};

uint64_t g_resizes = 0;
uint64_t g_probes = 0;

struct Map
{
    Cell * buf;
    uint8_t deg = 4;
    size_t m_size = 0;
    bool has_zero = false;
    Cell zero{0, {}};

    Map() { buf = static_cast<Cell *>(calloc(16, sizeof(Cell))); }
    ~Map() { free(buf); }
    Map(const Map &) = delete;
    Map(Map && o) noexcept : buf(o.buf), deg(o.deg), m_size(o.m_size), has_zero(o.has_zero), zero(o.zero) { o.buf = nullptr; }

    ALWAYS_INLINE size_t bufSize() const { return 1ULL << deg; }
    ALWAYS_INLINE size_t mask() const { return bufSize() - 1; }
    ALWAYS_INLINE size_t maxFill() const { return bufSize() / 4 * 3; }
    ALWAYS_INLINE size_t size() const { return m_size; }
    ALWAYS_INLINE bool isEmptyCell(size_t h) const { return buf[h & mask()].key == 0; }
    ALWAYS_INLINE uint64_t keyInCell(size_t h) const { return buf[h & mask()].key; }

    ALWAYS_INLINE size_t findCell(uint64_t key, size_t place) const
    {
        while (buf[place].key != 0 && buf[place].key != key)
        {
            place = (place + 1) & mask();
#ifdef COUNT_PROBES
            ++g_probes;
#endif
        }
        return place;
    }

    void reinsert(Cell & x)
    {
        size_t place = x.key & mask();
        if (&x == &buf[place])
            return;
        place = findCell(x.key, place);
        if (buf[place].key != 0)
            return;
        memcpy(static_cast<void *>(&buf[place]), &x, sizeof(x));
        x.key = 0;
    }

    NO_INLINE void resize(size_t for_num_elems = 0)
    {
        const size_t old_size = bufSize();
        uint8_t nd;
        if (for_num_elems)
        {
            nd = 4;
            while (((1ULL << nd) / 4 * 3) < for_num_elems)
                ++nd;
            if ((1ULL << nd) <= old_size)
                return;
        }
        else
            nd = deg + 1;
        ++g_resizes;
        const size_t new_size = 1ULL << nd;
        buf = static_cast<Cell *>(realloc(buf, new_size * sizeof(Cell)));
        memset(static_cast<void *>(buf + old_size), 0, (new_size - old_size) * sizeof(Cell));
        deg = nd;
        if (m_size - has_zero)
        {
            size_t i = 0;
            for (; i < old_size; ++i)
                if (buf[i].key != 0)
                    reinsert(buf[i]);
            for (; i < new_size && buf[i].key != 0; ++i)
                reinsert(buf[i]);
        }
    }

    void reserve(size_t n) { if (n) resize(n); }

    ALWAYS_INLINE Bucket & operator[](uint64_t key)
    {
        if (key == 0)
        {
            if (!has_zero) { has_zero = true; zero = Cell{0, {}}; ++m_size; }
            return zero.m;
        }
        size_t place = findCell(key, key & mask());
        if (buf[place].key != 0)
            return buf[place].m;
        buf[place].key = key;
        buf[place].m = Bucket{};
        ++m_size;
        if (__builtin_expect(m_size > maxFill(), 0))
        {
            resize();
            place = findCell(key, key & mask());
        }
        return buf[place].m;
    }

    /// Like operator[], also reports whether the key was inserted and the cell it went to (before any resize).
    ALWAYS_INLINE Bucket & emplace(uint64_t key, bool & inserted, size_t & distance)
    {
        inserted = false;
        distance = 0;
        if (key == 0)
        {
            if (!has_zero) { has_zero = true; zero = Cell{0, {}}; ++m_size; inserted = true; }
            return zero.m;
        }
        const size_t home = key & mask();
        size_t place = findCell(key, home);
        if (buf[place].key != 0)
            return buf[place].m;
        inserted = true;
        distance = (place - home) & mask();
        buf[place].key = key;
        buf[place].m = Bucket{};
        ++m_size;
        if (__builtin_expect(m_size > maxFill(), 0))
        {
            resize();
            place = findCell(key, key & mask());
        }
        return buf[place].m;
    }

    ALWAYS_INLINE Cell * find(uint64_t key)
    {
        if (key == 0)
            return has_zero ? &zero : nullptr;
        size_t place = findCell(key, key & mask());
        return buf[place].key ? &buf[place] : nullptr;
    }

    template <typename F>
    void forEach(F && f) const
    {
        if (has_zero)
            f(zero);
        for (size_t i = 0, n = bufSize(); i < n; ++i)
            if (buf[i].key != 0)
                f(buf[i]);
    }
};

enum Policy { MASTER = 0, RUN1 = 1, RUN2 = 2, POST = 3, POST1 = 4, OCC = 5, SEP = 6 };
const char * policy_name[] = {"master", "run1", "run2", "post", "post1", "occ", "sep"};

template <int P>
ALWAYS_INLINE Bucket & getOrInsert(Map & b, uint64_t k)
{
    if constexpr (P == MASTER)
        return b[k];
    else if constexpr (P == RUN1)
    {
        const size_t bs = b.bufSize();
        if (k >= bs && b.size() * 2 > bs && !b.isEmptyCell(k))
        {
            if (auto * it = b.find(k))
                return it->m;
            b.reserve(bs);
        }
        return b[k];
    }
    else if constexpr (P == RUN2)
    {
        const size_t bs = b.bufSize();
        if (b.size() * 2 > bs && k < 2 * bs && !b.isEmptyCell(k) && !b.isEmptyCell(k + 1))
        {
            if (auto * it = b.find(k))
                return it->m;
            b.reserve(bs);
        }
        return b[k];
    }
    else if constexpr (P == SEP)
    {
        /// RUN2 plus: only if the doubling moves the key and the key in its home cell to different halves.
        const size_t bs = b.bufSize();
        if (b.size() * 2 > bs && k < 2 * bs && k != 0)
        {
            const uint64_t occupant = b.keyInCell(k);
            if (occupant != 0 && ((occupant ^ k) & bs) != 0 && !b.isEmptyCell(k + 1))
            {
                if (auto * it = b.find(k))
                    return it->m;
                b.reserve(bs);
            }
        }
        return b[k];
    }
    else if constexpr (P == OCC)
    {
        /// Grow a more-than-half-full map before inserting a missing key whose home cell and the next cell are taken,
        /// if the doubling moves the key and the key in its home cell to different halves.
        const size_t bs = b.bufSize();
        if (b.size() * 2 > bs && k != 0)
        {
            const uint64_t occupant = b.keyInCell(k);
            if (occupant != 0 && ((occupant ^ k) & bs) != 0 && !b.isEmptyCell(k + 1))
            {
                if (auto * it = b.find(k))
                    return it->m;
                b.reserve(bs);
            }
        }
        return b[k];
    }
    else
    {
        /// POST: insert first (one probe walk, same as master), then grow if the new key landed more than
        /// one cell past its home in a more-than-half-full map and would not wrap after doubling.
        /// POST1: the same with distance >= 1.
        bool inserted;
        size_t distance;
        Bucket & ref = b.emplace(k, inserted, distance);
        if (__builtin_expect(inserted && distance > (P == POST ? 1 : 0), 0))
        {
            const size_t bs = b.bufSize();
            if (b.size() * 2 > bs && k < 2 * bs)
            {
                b.reserve(bs);
                return b.find(k)->m;
            }
        }
        return ref;
    }
}

/// A call of the batch add path: one bucket key and the samples of its run.
struct Run
{
    uint32_t id;
    uint32_t key;
    uint32_t begin;  /// offset into the sample arrays
    uint32_t count;
};

struct Stream
{
    std::vector<Run> runs;
    std::vector<int64_t> ts;
    std::vector<double> val;
};

static double now_ms()
{
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

struct PhaseTimes
{
    double add = 0, merge = 0, fin = 0;
    uint64_t resizes = 0;
    size_t cells_partial = 0, cells_final = 0;
};

/// threads[t] is the run stream of reader thread t; states are per (thread, id); merge folds thread 1.. into 0 as
/// Aggregator::mergeDataImpl does (the first state of an id is adopted); final walks [0, bucket_count).
bool g_merge_into_empty = false;

template <int P>
NO_INLINE PhaseTimes runWorkload(const std::vector<Stream> & threads, size_t num_ids, size_t bucket_count)
{
    PhaseTimes pt;
    g_resizes = 0;
    std::vector<std::vector<Map>> states(threads.size());
    std::vector<std::vector<uint8_t>> present(threads.size());
    double t0 = now_ms();
    for (size_t t = 0; t < threads.size(); ++t)
    {
        states[t].resize(num_ids);
        present[t].assign(num_ids, 0);
        const Stream & s = threads[t];
        for (const Run & r : s.runs)
        {
            present[t][r.id] = 1;
            getOrInsert<P>(states[t][r.id], r.key).addMany(s.ts.data() + r.begin, s.val.data() + r.begin, r.count);
        }
    }
    double t1 = now_ms();
    for (size_t t = 0; t < threads.size(); ++t)
        for (size_t id = 0; id < num_ids; ++id)
            if (present[t][id])
                pt.cells_partial += states[t][id].bufSize();
    double t2 = now_ms();
    std::vector<Map *> result(num_ids, nullptr);
    std::vector<Map> empties(g_merge_into_empty ? num_ids : 0);
    if (g_merge_into_empty)
        for (size_t id = 0; id < num_ids; ++id)
            result[id] = &empties[id];
    for (size_t t = 0; t < threads.size(); ++t)
        for (size_t id = 0; id < num_ids; ++id)
        {
            if (!present[t][id])
                continue;
            if (!result[id])
            {
                result[id] = &states[t][id];
                continue;
            }
            Map & lhs = *result[id];
            const Map & rhs = states[t][id];
            lhs.reserve(rhs.size());
            rhs.forEach([&](const Cell & c) { getOrInsert<P>(lhs, c.key).merge(c.m); });
        }
    double t3 = now_ms();
    double checksum = 0;
    for (size_t id = 0; id < num_ids; ++id)
    {
        if (!result[id])
            continue;
        Map & m = *result[id];
        pt.cells_final += m.bufSize();
        if (m.bufSize() >= bucket_count)
        {
            for (size_t k = 0; k < bucket_count; ++k)
                if (auto * it = m.find(k))
                    checksum += it->m.v;
        }
        else
        {
            std::vector<std::pair<uint64_t, const Bucket *>> v;
            v.reserve(m.size());
            m.forEach([&](const Cell & c) { v.emplace_back(c.key, &c.m); });
            std::sort(v.begin(), v.end(), [](auto & a, auto & b) { return a.first < b.first; });
            for (auto & e : v)
                checksum += e.second->v;
        }
    }
    double t4 = now_ms();
    if (checksum == 12345.678)
        printf("x\n");
    pt.add = t1 - t0;
    pt.merge = t3 - t2;
    pt.fin = t4 - t3;
    pt.resizes = g_resizes;
    return pt;
}

/// Append the samples of one (id, sorted timestamps) to a stream as runs of equal bucket keys.
/// key_of returns UINT32_MAX for a rejected sample.
template <typename KeyOf>
void appendSeries(Stream & s, uint32_t id, const std::vector<int64_t> & tss, KeyOf && key_of)
{
    size_t i = 0;
    while (i < tss.size())
    {
        uint32_t k = key_of(tss[i]);
        size_t j = i + 1;
        while (j < tss.size() && key_of(tss[j]) == k)
            ++j;
        if (k != UINT32_MAX)
        {
            Run r{id, k, static_cast<uint32_t>(s.ts.size()), static_cast<uint32_t>(j - i)};
            for (size_t q = i; q < j; ++q)
            {
                s.ts.push_back(tss[q]);
                s.val.push_back(static_cast<double>(tss[q] % 1000));
            }
            s.runs.push_back(r);
        }
        i = j;
    }
}

/// The ts_bucketing table of tests/performance/timeseries_to_grid_bucketing.xml: number -> id = n % 2500,
/// ts = (n / 2500) * 15 s, 50M rows. numbers_mt hands blocks of 65409 numbers to 8 insert threads round-robin; each
/// insert thread squashes 17 blocks into a part (min_insert_block_size_rows 1048449). `merge_factor` parts in a row
/// (by part order) are merged into one part (1 = no merges, 0 = everything in one part). Parts are sorted by
/// (id, ts) and their rows split into `num_threads` equal contiguous ranges, one per reader thread.
/// `scale` divides the series count and the block sizes alike, which keeps every per-series shape (time slices per
/// part, keys per partial state) and cuts the model's memory.
constexpr uint64_t SCALE = 5;
std::vector<Stream> bucketingLayout(size_t merge_factor, size_t num_threads, uint32_t step_s, uint32_t window_s, bool unsorted, uint64_t draw = 0)
{
    const uint64_t ids = 2500 / SCALE, per_series = unsorted ? 8000 : 20000, total = ids * per_series;
    const uint64_t blk = 65409 / SCALE, blocks_per_part = 17, insert_threads = 8;
    const uint64_t nblocks = (total + blk - 1) / blk;
    std::vector<std::vector<uint64_t>> thread_blocks(insert_threads);
    /// draw 0: blocks go round-robin; draw > 0: each block goes to a pseudo-random insert thread (the scheduling
    /// draw the comment in timeseries_to_grid_aggregation_functions.xml describes).
    uint64_t x = draw * 0x9E3779B97F4A7C15ULL + 1;
    for (uint64_t b = 0; b < nblocks; ++b)
    {
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        thread_blocks[draw ? x % insert_threads : b % insert_threads].push_back(b);
    }
    /// Part order = commit order: interleave the insert threads' parts by their first block.
    std::vector<std::vector<uint64_t>> parts;
    for (uint64_t t = 0; t < insert_threads; ++t)
        for (size_t i = 0; i < thread_blocks[t].size(); i += blocks_per_part)
            parts.emplace_back(thread_blocks[t].begin() + i,
                thread_blocks[t].begin() + std::min(thread_blocks[t].size(), i + blocks_per_part));
    std::sort(parts.begin(), parts.end(), [](auto & a, auto & b) { return a.front() < b.front(); });
    std::vector<std::vector<uint64_t>> merged;
    const size_t mf = merge_factor == 0 ? parts.size() : merge_factor;
    for (size_t i = 0; i < parts.size(); i += mf)
    {
        std::vector<uint64_t> m;
        for (size_t j = i; j < std::min(parts.size(), i + mf); ++j)
            m.insert(m.end(), parts[j].begin(), parts[j].end());
        merged.push_back(std::move(m));
    }
    /// Rows of all parts, in part order then (id, ts) -- or (id, hash) for the unsorted table.
    struct Row { uint32_t id; int64_t ts; };
    std::vector<Row> rows;
    rows.reserve(total);
    for (auto & p : merged)
    {
        size_t start = rows.size();
        for (uint64_t b : p)
            for (uint64_t n = b * blk; n < std::min(total, (b + 1) * blk); ++n)
                rows.push_back(Row{static_cast<uint32_t>(n % ids), static_cast<int64_t>((n / ids) * 15)});
        if (unsorted)
        {
            auto h = [](const Row & r) { uint64_t x = r.ts * 0x9E3779B97F4A7C15ULL + r.id; x ^= x >> 31; x *= 0xBF58476D1CE4E5B9ULL; x ^= x >> 29; return x; };
            std::sort(rows.begin() + start, rows.end(), [&](const Row & a, const Row & b) { return a.id != b.id ? a.id < b.id : h(a) < h(b); });
        }
        else
            std::sort(rows.begin() + start, rows.end(), [](const Row & a, const Row & b) { return a.id != b.id ? a.id < b.id : a.ts < b.ts; });
    }
    const int64_t step = step_s, window = window_s;
    auto key_of = [&](int64_t t) -> uint32_t
    {
        /// Grid points 0, step, ..., the bucket of a grid point g is (g - window, g]; for window >= step split
        /// buckets at multiples of gcd-ish boundaries are approximated by (t + step - 1) / step.
        if (window >= step)
            return static_cast<uint32_t>((t + step - 1) / step);
        int64_t g = (t + step - 1) / step * step;
        return (g - t < window) ? static_cast<uint32_t>(g / step) : UINT32_MAX;
    };
    std::vector<Stream> out(num_threads);
    const size_t per_thread = (rows.size() + num_threads - 1) / num_threads;
    const size_t read_block = 65409 / SCALE;
    for (size_t t = 0; t < num_threads; ++t)
    {
        size_t b = t * per_thread, e = std::min(rows.size(), b + per_thread);
        /// Runs are cut at read block boundaries and at id changes.
        for (size_t blk_begin = b; blk_begin < e; blk_begin += read_block)
        {
            size_t blk_end = std::min(e, blk_begin + read_block);
            size_t i = blk_begin;
            while (i < blk_end)
            {
                size_t j = i;
                std::vector<int64_t> tss;
                while (j < blk_end && rows[j].id == rows[i].id)
                    tss.push_back(rows[j++].ts);
                if (unsorted)
                {
                    /// Each sample is its own call: appendSeries merges equal neighbours only.
                    appendSeries(out[t], rows[i].id, tss, key_of);
                }
                else
                    appendSeries(out[t], rows[i].id, tss, key_of);
                i = j;
            }
        }
    }
    return out;
}

/// The single-threaded shapes of tests/performance/timeseries_to_grid_gap.xml: 200 series, one sample per minute,
/// 60 s grid of 7 days (10081 buckets), one state per series; `merge_later_first` builds two states per series
/// (minutes >= split first, then < split) and merges the earlier into the later.
struct GapCase { const char * name; std::vector<std::pair<int64_t, int64_t>> ranges; int every; int per_minute; bool merge_later_first; int order = 0; int64_t grid = 10081; };

std::vector<Stream> gapLayout(const GapCase & c, size_t & num_ids)
{
    num_ids = 200;
    std::vector<Stream> out((c.merge_later_first || c.order >= 2) ? 2 : 1);
    auto key_of = [](int64_t t) -> uint32_t { return static_cast<uint32_t>((t + 59) / 60); };
    for (uint32_t id = 0; id < 200; ++id)
    {
        std::vector<int64_t> all;
        for (auto [b, e] : c.ranges)
            for (int64_t m = b; m < e; m += c.every)
                for (int q = 0; q < c.per_minute; ++q)
                    all.push_back(m * 60 - (c.per_minute - 1 - q) * (60 / c.per_minute));
        if (c.order == 1)
        {
            std::reverse(all.begin(), all.end());
            appendSeries(out[0], id, all, key_of);
        }
        else if (c.order >= 2)
        {
            std::vector<int64_t> early, late;
            for (int64_t t : all)
                (t < c.ranges[1].first * 60 ? early : late).push_back(t);
            appendSeries(out[0], id, c.order == 2 ? late : early, key_of);
            appendSeries(out[1], id, c.order == 2 ? early : late, key_of);
        }
        else if (!c.merge_later_first)
            appendSeries(out[0], id, all, key_of);
        else
        {
            std::vector<int64_t> early, late;
            for (int64_t t : all)
                (t < c.ranges[1].first * 60 ? early : late).push_back(t);
            appendSeries(out[0], id, late, key_of);
            appendSeries(out[1], id, early, key_of);
        }
    }
    return out;
}

std::vector<Stream> aggFunctionsLayout(size_t num_threads)
{
    const uint64_t ids = 400, per_series = 10000;
    std::vector<Stream> out(num_threads);
    /// Reader threads get whole granules (8192 rows), so their ranges end inside a series.
    const uint64_t total = ids * per_series, granule = 8192;
    const uint64_t per_thread = ((total + num_threads - 1) / num_threads + granule - 1) / granule * granule;
    auto key_of = [](int64_t t) -> uint32_t { return static_cast<uint32_t>((t + 9) / 10); };
    for (size_t t = 0; t < num_threads; ++t)
    {
        uint64_t b = std::min(total, t * per_thread), e = std::min(total, b + per_thread);
        uint64_t r = b;
        while (r < e)
        {
            uint32_t id = static_cast<uint32_t>(r / per_series);
            uint64_t end = std::min(e, (r / per_series + 1) * per_series);
            std::vector<int64_t> tss;
            for (uint64_t q = r; q < end; ++q)
                tss.push_back(static_cast<int64_t>((q % per_series) * 10));
            appendSeries(out[t], id, tss, key_of);
            r = end;
        }
    }
    return out;
}

template <int P>
PhaseTimes best(const std::vector<Stream> & s, size_t ids, size_t bc, int reps)
{
    std::vector<PhaseTimes> v;
    for (int r = 0; r < reps; ++r)
        v.push_back(runWorkload<P>(s, ids, bc));
    std::sort(v.begin(), v.end(), [](auto & a, auto & b) { return a.add + a.merge + a.fin < b.add + b.merge + b.fin; });
    return v[v.size() / 2];
}

void report(const char * name, const std::vector<Stream> & s, size_t ids, size_t bc, int reps)
{
    PhaseTimes r[7] = {best<MASTER>(s, ids, bc, reps), best<RUN1>(s, ids, bc, reps), best<RUN2>(s, ids, bc, reps),
                       best<POST>(s, ids, bc, reps), best<POST1>(s, ids, bc, reps), best<OCC>(s, ids, bc, reps),
                       best<SEP>(s, ids, bc, reps)};
    for (int p = 0; p < 7; ++p)
    {
        if (p == RUN1 || p == POST || p == POST1)
            continue;
    {
        double tot = r[p].add + r[p].merge + r[p].fin;
        double base = r[0].add + r[0].merge + r[0].fin;
        printf("%-34s %-6s add %8.2f merge %8.2f final %7.2f total %8.2f ms (%+6.1f%%) resizes %8lu partial_cells %9zu final_cells %9zu\n",
            name, policy_name[p], r[p].add, r[p].merge, r[p].fin, tot, (tot / base - 1) * 100, r[p].resizes, r[p].cells_partial, r[p].cells_final);
    }
    }
}

int main(int argc, char ** argv)
{
    std::string what = argc > 1 ? argv[1] : "all";
    int reps = argc > 2 ? atoi(argv[2]) : 5;
    if (what == "all" || what == "gap")
    {
        std::vector<GapCase> cases = {
            {"T1 gap", {{0, 3100}, {6981, 10081}}, 1, 1, false},
            {"T3 merge later first", {{0, 3100}, {6981, 10081}}, 1, 1, true},
            {"G1 contiguous", {{0, 6200}}, 1, 1, false},
            {"G3 late_start", {{4000, 10081}}, 1, 1, false},
            {"G4 gap_sparse", {{0, 1500}, {8581, 10081}}, 1, 1, false},
            {"G6 every_2nd", {{0, 10081}}, 2, 1, false},
            {"G7 dense", {{0, 6200}}, 1, 4, false},
        };
        for (auto & c : cases)
        {
            size_t ids;
            auto s = gapLayout(c, ids);
            report(c.name, s, ids, 10081, reps);
        }
    }
    if (what == "all" || what == "bucketing")
    {
        for (size_t mf : {size_t(1), size_t(8), size_t(0)})
        {
            auto s = bucketingLayout(mf, 16, 300, 300, false);
            std::string n = "bucketing 300/300 mf=" + std::to_string(mf);
            report(n.c_str(), s, 2500 / SCALE, 1001, reps);
            auto s2 = bucketingLayout(mf, 16, 900, 60, false);
            n = "bucketing 900/60 mf=" + std::to_string(mf);
            report(n.c_str(), s2, 2500 / SCALE, 334, reps);
        }
        auto u = bucketingLayout(1, 16, 300, 300, true);
        report("bucketing unsorted mf=1", u, 2500 / SCALE, 1001, reps);
    }
    if (what == "orders")
    {
        std::vector<GapCase> cases = {
            {"T1 gap fwd", {{0, 3100}, {6981, 10081}}, 1, 1, false, 0},
            {"T1 gap rev", {{0, 3100}, {6981, 10081}}, 1, 1, false, 1},
            {"T3a -Merge late first", {{0, 3100}, {6981, 10081}}, 1, 1, false, 2},
            {"T3b -Merge early first", {{0, 3100}, {6981, 10081}}, 1, 1, false, 3},
            {"G4 sparse fwd", {{0, 1500}, {8581, 10081}}, 1, 1, false, 0},
            {"G4 sparse rev", {{0, 1500}, {8581, 10081}}, 1, 1, false, 1},
            {"G4 sparse -Merge late first", {{0, 1500}, {8581, 10081}}, 1, 1, false, 2},
            {"G6 every_2nd fwd", {{0, 10081}}, 2, 1, false, 0},
            {"G6 every_2nd rev", {{0, 10081}}, 2, 1, false, 1},
            {"G3 late_start rev", {{4000, 10081}}, 1, 1, false, 1},
            {"G1 contiguous rev", {{0, 6200}}, 1, 1, false, 1},
            {"L1 30d grid gap fwd", {{0, 3100}, {40101, 43201}}, 1, 1, false, 0, 43201},
            {"L1 30d grid gap rev", {{0, 3100}, {40101, 43201}}, 1, 1, false, 1, 43201},
            {"L1 30d -Merge late first", {{0, 3100}, {40101, 43201}}, 1, 1, false, 2, 43201},
            {"L2 30d sparse fwd", {{0, 1500}, {41701, 43201}}, 1, 1, false, 0, 43201},
        };
        for (auto & c : cases)
        {
            size_t ids;
            auto s = gapLayout(c, ids);
            g_merge_into_empty = c.order >= 2;
            report(c.name, s, ids, c.grid, reps);
            g_merge_into_empty = false;
        }
    }
    if (what == "agg")
    {
        auto s = aggFunctionsLayout(16);
        report("aggfn 16 threads one part", s, 400, 10001, reps);
    }
    if (what == "draws")
    {
        for (uint64_t d = 1; d <= 6; ++d)
        {
            auto s = bucketingLayout(1, 16, 300, 300, false, d);
            std::string n = "bucketing 300/300 draw=" + std::to_string(d);
            report(n.c_str(), s, 2500 / SCALE, 1001, reps);
        }
    }
    return 0;
}
