#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{

/// Registers built-in SQL user-defined functions that must load after persisted user functions from disk
/// (so stored UDFs take precedence when names already exist).
/// Call after `UserDefinedSQLObjectsStorage::loadObjects()`.
void registerBuiltinSQLUserDefinedFunctions(ContextMutablePtr context);

}
