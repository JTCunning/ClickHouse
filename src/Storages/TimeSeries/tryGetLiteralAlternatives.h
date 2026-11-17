#pragma once

#include <Core/Types.h>

#include <optional>


namespace DB
{

/// Returns the terms of a regex like `a|b` or `(a|b)|(?:c)` if every term is a plain literal.
/// A group is allowed only as a whole term, so `(a|b)c` is not a list.
std::optional<Strings> tryGetLiteralAlternatives(std::string_view regex);

/// Adds anchors to a regex of a PromQL matcher, which must match the whole value: `a.*` gives `^a.*$`.
/// A regex with alternatives at the top level is put into a group, because `^a|b$` matches any value starting with `a`:
/// `a.|b` gives `^(?:a.|b)$`. Other regexes are not put into a group, so the primary key can use their prefixes.
String makeFullMatchRegexp(std::string_view regex);

}
