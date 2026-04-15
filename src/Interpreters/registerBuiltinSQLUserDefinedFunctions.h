#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{

/// Registers built-in SQL user-defined functions that must load after persisted user functions from disk.
/// Call after `UserDefinedSQLObjectsStorage::loadObjects()`.
///
/// For `timeSeriesMetricLocalityId`: registers the canonical definition if absent; if a SQL UDF with that name
/// already exists (e.g. from disk), validates normalized function body matches `x -> toUInt32(sipHash64(x))`
/// exactly and throws on mismatch — server startup fails until resolved.
void registerBuiltinSQLUserDefinedFunctions(ContextMutablePtr context);

/// Re-validates or registers the same canonical `timeSeriesMetricLocalityId` UDF (e.g. when `timeSeriesSelector`
/// runs) so a definition replaced at runtime without process restart is checked before query execution.
void ensureTimeSeriesMetricLocalityIdUserDefinedFunction(ContextMutablePtr context);

}
