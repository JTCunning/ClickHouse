#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{

/// Hook for optional built-in SQL UDFs that must register after persisted user functions from disk.
/// Native functions use `REGISTER_FUNCTION` instead. Call after `UserDefinedSQLObjectsStorage::loadObjects()`.
void registerBuiltinSQLUserDefinedFunctions(ContextMutablePtr context);

}
