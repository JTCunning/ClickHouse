#include <Storages/TimeSeries/tryGetLiteralAlternatives.h>


namespace DB
{

namespace
{
    constexpr size_t max_group_depth = 16;
    constexpr size_t max_terms_of_sequence = 1024;

    /// Parses `sequence ('|' sequence)*` from `pos` up to the end of `regex` or an unmatched ')', which is left to the caller.
    /// A sequence is plain characters and groups `(alternatives)` or `(?:alternatives)`, optionally followed by `?`.
    std::optional<Strings> parseAlternatives(std::string_view regex, size_t & pos, size_t depth)
    {
        if (depth > max_group_depth)
            return {};

        Strings result;
        Strings sequence(1);

        auto end_sequence = [&]
        {
            for (auto & term : sequence)
                result.push_back(std::move(term));
            sequence.assign(1, String{});
        };

        while (pos < regex.size())
        {
            char c = regex[pos];
            if (c == '|')
            {
                ++pos;
                end_sequence();
            }
            else if (c == ')')
            {
                break;
            }
            else if (c == '(')
            {
                ++pos;
                if (regex.substr(pos).starts_with("?:"))
                    pos += 2;

                auto group = parseAlternatives(regex, pos, depth + 1);
                if (!group || pos >= regex.size() || regex[pos] != ')')
                    return {};
                ++pos;

                if (pos < regex.size() && regex[pos] == '?')
                {
                    ++pos;
                    group->push_back(String{});
                }

                if (sequence.size() > 1 && sequence.size() * group->size() > max_terms_of_sequence)
                    return {};

                Strings product;
                product.reserve(sequence.size() * group->size());
                for (const auto & prefix : sequence)
                {
                    for (const auto & suffix : *group)
                        product.push_back(prefix + suffix);
                }
                sequence = std::move(product);
            }
            else if (std::string_view("\\.^$?*+[]{}").contains(c))
            {
                return {};
            }
            else
            {
                for (auto & term : sequence)
                    term += c;
                ++pos;
            }
        }

        end_sequence();
        return result;
    }
}

std::optional<Strings> tryGetLiteralAlternatives(std::string_view regex)
{
    size_t pos = 0;
    auto terms = parseAlternatives(regex, pos, 0);
    if (!terms || pos != regex.size())
        return {};
    return terms;
}

}
