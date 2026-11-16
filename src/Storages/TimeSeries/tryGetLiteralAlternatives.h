#pragma once

#include <Core/Types.h>

#include <optional>


namespace DB
{

/// Returns the strings matched by a regex like `a|b`, `(a|b)|(?:c)`, `x(a|b)y` or `x(_total)?` if the regex has no other syntax.
/// Returns nullopt if the regex is not such a list, nests groups too deeply, or expands to too many strings.
std::optional<Strings> tryGetLiteralAlternatives(std::string_view regex);

}
