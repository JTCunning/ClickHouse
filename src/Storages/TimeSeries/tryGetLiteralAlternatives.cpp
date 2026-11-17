#include <Storages/TimeSeries/tryGetLiteralAlternatives.h>

#include <fmt/format.h>


namespace DB
{

std::optional<Strings> tryGetLiteralAlternatives(std::string_view regex)
{
    Strings terms(1);
    size_t depth = 0;
    bool term_ended = false;
    for (size_t pos = 0; pos < regex.size(); ++pos)
    {
        char c = regex[pos];
        if (c == '|')
        {
            terms.emplace_back();
            term_ended = false;
        }
        else if (c == '(')
        {
            if (term_ended || !terms.back().empty())
                return {};
            if (regex.substr(pos + 1).starts_with("?:"))
                pos += 2;
            ++depth;
        }
        else if (c == ')')
        {
            if (depth == 0)
                return {};
            --depth;
            term_ended = true;
        }
        else if (term_ended || std::string_view("\\.^$?*+[]{}").contains(c))
            return {};
        else
            terms.back() += c;
    }
    if (depth != 0)
        return {};
    return terms;
}

String makeFullMatchRegexp(std::string_view regex)
{
    bool has_alternatives = false;
    size_t depth = 0;
    bool in_character_class = false;
    for (size_t pos = 0; pos < regex.size(); ++pos)
    {
        char c = regex[pos];
        if (c == '\\')
            ++pos;
        else if (in_character_class)
            in_character_class = (c != ']');
        else if (c == '[')
            in_character_class = true;
        else if (c == '(')
            ++depth;
        else if ((c == ')') && depth)
            --depth;
        else if ((c == '|') && !depth)
            has_alternatives = true;
    }

    if (has_alternatives)
        return fmt::format("^(?:{})$", regex);

    String res{regex};
    if (!res.starts_with('^'))
        res = '^' + res;
    if (!res.ends_with('$'))
        res += '$';
    return res;
}

}
