#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{

/// Registers built-in SQL user-defined functions that replace what would otherwise be
/// one-line native functions (e.g. `timeSeriesMetricLocalityId` as `toUInt32(sipHash64(x))`).
/// Call after `UserDefinedSQLObjectsStorage::loadObjects()` so persisted UDFs take precedence
/// when `registerBuiltinSQLUserDefinedFunctions` skips names that already exist.
void registerBuiltinSQLUserDefinedFunctions(ContextMutablePtr context);

}
