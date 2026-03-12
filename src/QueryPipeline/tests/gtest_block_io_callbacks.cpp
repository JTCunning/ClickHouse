#include <gtest/gtest.h>

#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/QueryPipeline.h>

using namespace DB;

/// Test that BlockIO::executeWithCallbacks invokes onFinish (and thus finish_callbacks)
/// after the lambda completes successfully. This is the pattern used by PrometheusHTTPProtocolAPI
/// so that PromQL queries are fully logged to system.query_log (QueryStart and QueryFinish).
TEST(BlockIO, ExecuteWithCallbacksInvokesFinishCallbacks)
{
    BlockIO io;
    io.finalize_query_pipeline = [](QueryPipeline &&) { return QueryPipelineFinalizedInfo{}; };

    bool finish_called = false;
    io.finish_callbacks.push_back(
        [&finish_called](const QueryPipelineFinalizedInfo &, std::chrono::system_clock::time_point)
        {
            finish_called = true;
        });

    io.executeWithCallbacks([]() {
        /// Simulate doing work (e.g. PullingPipelineExecutor::pull).
    });

    EXPECT_TRUE(finish_called) << "executeWithCallbacks must call onFinish() and thus finish_callbacks";
}

/// Test that BlockIO::executeWithCallbacks invokes onException (and thus exception_callbacks)
/// when the lambda throws, then rethrows.
TEST(BlockIO, ExecuteWithCallbacksInvokesExceptionCallbacksOnThrow)
{
    BlockIO io;
    bool exception_called = false;
    io.exception_callbacks.push_back([&exception_called](bool) { exception_called = true; });

    EXPECT_THROW(
        io.executeWithCallbacks([]() { throw std::runtime_error("test"); }),
        std::runtime_error);

    EXPECT_TRUE(exception_called) << "executeWithCallbacks must call onException() when the lambda throws";
}
