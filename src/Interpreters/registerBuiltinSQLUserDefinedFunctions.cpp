#include <Interpreters/registerBuiltinSQLUserDefinedFunctions.h>

#include <Interpreters/Context.h>


namespace DB
{

void registerBuiltinSQLUserDefinedFunctions(ContextMutablePtr /* context */)
{
    /// Reserved for SQL UDFs that must load after persisted user functions.
    /// (Built-in scalar functions are registered via `REGISTER_FUNCTION` in `src/Functions/`.)
}

}
