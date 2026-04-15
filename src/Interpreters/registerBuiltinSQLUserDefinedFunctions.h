#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{

/// Registers built-in SQL user-defined functions that must load after persisted user functions from disk
/// (so stored UDFs take precedence when names already exist).
/// Call after `UserDefinedSQLObjectsStorage::loadObjects()`.
void registerBuiltinSQLUserDefinedFunctions(ContextMutablePtr context);

/// Ensures the built-in `timeSeriesMetricLocalityId` UDF matches `x -> toUInt32(sipHash64(x))` when TimeSeries
/// query paths run (e.g. `timeSeriesSelector`). Call from those paths only — not at global startup, so a
/// conflicting user-owned UDF does not block server boot until TimeSeries actually uses the name.
void ensureTimeSeriesMetricLocalityIdUserDefinedFunction(ContextMutablePtr context);

}
