// Shape check only: the branch's control flow (lambda, if / else if constexpr) and the fill loop.
#include <cstdint>
#include <cstddef>
#include <vector>
struct DateTime64 { int64_t value; DateTime64() = default; DateTime64(int64_t v) : value(v) {} };
template <bool with_values>
struct F
{
    template <typename TimestampType>
    size_t run(std::vector<TimestampType> & data, const uint8_t * null_map, int64_t start_timestamp, uint64_t step, size_t num_steps, bool row_has_no_nulls) const
    {
        auto timestamp_at = [&](size_t j)
        {
            return static_cast<TimestampType>(static_cast<int64_t>(static_cast<uint64_t>(start_timestamp) + j * step));
        };
        if (row_has_no_nulls)
        {
            const size_t timestamps_offset = data.size();
            data.resize(timestamps_offset + num_steps);
            for (size_t j = 0; j != num_steps; ++j)
                data[timestamps_offset + j] = timestamp_at(j);
        }
        else if constexpr (with_values)
        {
            for (size_t j = 0; j != num_steps; ++j)
            {
                if (null_map[j])
                    continue;
                data.push_back(timestamp_at(j));
            }
        }
        return data.size();
    }
};
size_t a(std::vector<DateTime64> & d, const uint8_t * n, int64_t s, uint64_t st, size_t k, bool b) { return F<true>{}.run<DateTime64>(d, n, s, st, k, b); }
size_t c(std::vector<uint32_t> & d, int64_t s, uint64_t st, size_t k) { return F<false>{}.run<uint32_t>(d, nullptr, s, st, k, true); }
