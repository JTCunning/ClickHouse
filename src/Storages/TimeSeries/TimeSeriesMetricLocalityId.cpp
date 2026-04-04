#include <Storages/TimeSeries/TimeSeriesMetricLocalityId.h>

#include <array>


namespace DB
{
namespace
{
/// Letters a-z are encoded as 1..26; 0 means “no letter” (trailing padding). Base is 27 so 0 does not alias `a`.
static constexpr size_t MAX_PACKED_LETTERS = 6;
static constexpr UInt32 PACK_BASE = 27;

bool isAsciiUpper(UInt8 c)
{
    return c >= 'A' && c <= 'Z';
}

bool isAsciiLower(UInt8 c)
{
    return c >= 'a' && c <= 'z';
}

bool isAsciiDigit(UInt8 c)
{
    return c >= '0' && c <= '9';
}

/// 1..26 for a-z, 0 if not a letter (ignored for packing).
UInt8 letterDigit(UInt8 c)
{
    if (isAsciiUpper(c))
        c = static_cast<UInt8>(c - 'A' + 'a');
    if (c >= 'a' && c <= 'z')
        return static_cast<UInt8>(c - 'a' + 1);
    return 0;
}
}

UInt32 computeTimeSeriesMetricLocalityId(std::string_view metric_name)
{
    /// Collect initials from snake_case segments and camelCase / PascalCase words (ASCII only).
    /// Non-letters in the initial position of a word are skipped (digits / punctuation ignored).
    std::array<UInt8, MAX_PACKED_LETTERS> letters{};
    size_t letter_count = 0;

    const size_t n = metric_name.size();
    size_t seg_start = 0;

    auto push_initial = [&](UInt8 c)
    {
        const UInt8 d = letterDigit(c);
        if (!d)
            return;
        if (letter_count >= MAX_PACKED_LETTERS)
            return;
        letters[letter_count] = d;
        ++letter_count;
    };

    for (size_t i = 0; i <= n && letter_count < MAX_PACKED_LETTERS; ++i)
    {
        if (i == n || static_cast<UInt8>(metric_name[i]) == '_')
        {
            const size_t seg_end = i;
            if (seg_start < seg_end)
            {
                size_t wstart = seg_start;
                for (size_t j = seg_start + 1; j < seg_end && letter_count < MAX_PACKED_LETTERS; ++j)
                {
                    const UInt8 c = static_cast<UInt8>(metric_name[j]);
                    const UInt8 prev = static_cast<UInt8>(metric_name[j - 1]);
                    if (isAsciiUpper(c) && (isAsciiLower(prev) || isAsciiDigit(prev)))
                    {
                        push_initial(static_cast<UInt8>(metric_name[wstart]));
                        wstart = j;
                    }
                }
                if (letter_count < MAX_PACKED_LETTERS)
                    push_initial(static_cast<UInt8>(metric_name[wstart]));
            }
            seg_start = i + 1;
        }
    }

    UInt32 v = 0;
    for (size_t i = 0; i < MAX_PACKED_LETTERS; ++i)
    {
        const UInt32 digit = (i < letter_count) ? letters[i] : 0;
        v = v * PACK_BASE + digit;
    }
    return v;
}

}
