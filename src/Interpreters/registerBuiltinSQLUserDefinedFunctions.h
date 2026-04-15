#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{

/// Registers built-in SQL user-defined functions that must load after persisted user functions from disk.
/// Call after `UserDefinedSQLObjectsStorage::loadObjects()`.
///
/// For `timeSeriesMetricLocalityId`: registers the canonical definition **only if the name is unused**, so
/// existing persisted UDFs keep precedence and server startup stays backward-compatible. Strict validation of a
/// conflicting definition is deferred to `ensureTimeSeriesMetricLocalityIdUserDefinedFunction` (TimeSeries paths).
void registerBuiltinSQLUserDefinedFunctions(ContextMutablePtr context);

/// Ensures `timeSeriesMetricLocalityId` matches the canonical body when TimeSeries runs (e.g. `timeSeriesSelector`):
/// register if missing; if present, require normalized AST to match `x -> toUInt32(sipHash64(x))` or throw.
void ensureTimeSeriesMetricLocalityIdUserDefinedFunction(ContextMutablePtr context);

}
