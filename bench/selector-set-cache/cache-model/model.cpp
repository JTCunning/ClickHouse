#include <future>
#include <mutex>
#include <unordered_map>
#include <variant>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <chrono>
#include <cstdio>
using String = std::string;
struct Set {};
using SetPtr = std::shared_ptr<const Set>;
using SharedSet = std::shared_future<SetPtr>;
static int failures = 0;
struct Sink { template <class T> Sink & operator<<(const T &) { return *this; } };
#define ASSERT_EQ(a,b) if (!((a)==(b))) { std::printf("ASSERT_EQ failed %s:%d\n", __func__, __LINE__); ++failures; return; } else Sink()
#define EXPECT_EQ(a,b) if (!((a)==(b))) { std::printf("EXPECT_EQ failed %s:%d\n", __func__, __LINE__); ++failures; } else Sink()
#define EXPECT_THROW(e,x) do { bool t=false; try { e; } catch (const x &) { t=true; } if(!t){std::printf("EXPECT_THROW failed %s:%d\n", __func__, __LINE__); ++failures;} } while(0); Sink()
#define ASSERT_NO_THROW(e) try { e; } catch (...) { std::printf("ASSERT_NO_THROW failed %s\n", __func__); ++failures; return; }
class PreparedSetsCache
{
public:
    explicit PreparedSetsCache(bool wait_for_sets_being_built_ = true) : wait_for_sets_being_built(wait_for_sets_being_built_) {}
    std::variant<std::promise<SetPtr>, SharedSet> findOrPromiseToBuild(const String & key);
    SetPtr findBuilt(const String & key);
private:
    struct Entry { SharedSet future; };
    const bool wait_for_sets_being_built;
    std::mutex cache_mutex;
    std::unordered_map<String, Entry> cache;
};
std::variant<std::promise<SetPtr>, SharedSet> PreparedSetsCache::findOrPromiseToBuild(const String & key)
{
    std::lock_guard lock(cache_mutex);

    auto it = cache.find(key);
    if (it != cache.end() && it->second.future.valid())
    {
        /// If the set is still being built, return its future so the caller can wait for it,
        /// or a promise to build it once more, which leaves the entry to the task building it.
        if (it->second.future.wait_for(std::chrono::seconds(0)) != std::future_status::ready)
        {
            if (!wait_for_sets_being_built)
                return std::promise<SetPtr>{};
            return it->second.future;
        }

        /// The build has finished. Reuse it only if it produced a set: a null set (an abandoned build)
        /// and a stored exception are both dropped below, handing this caller a fresh promise.
        try
        {
            if (it->second.future.get() != nullptr)
                return it->second.future;
        }
        catch (...) // Ok: a failed cached build is intentionally dropped and rebuilt below // NOLINT(bugprone-empty-catch)
        {
            /// The cached build failed; fall through to rebuild it.
        }
    }

    /// Insert the entry into the cache so that other threads can find it and start waiting for the set.
    std::promise<SetPtr> promise_to_fill_set;
    Entry & entry = cache[key];
    entry.future = promise_to_fill_set.get_future();
    return promise_to_fill_set;
}

SetPtr PreparedSetsCache::findBuilt(const String & key)
{
    std::lock_guard lock(cache_mutex);

    auto it = cache.find(key);
    if (it == cache.end() || !it->second.future.valid()
        || it->second.future.wait_for(std::chrono::seconds(0)) != std::future_status::ready)
        return nullptr;

    try
    {
        return it->second.future.get();
    }
    catch (...) // Ok: a failed build is not reused, the caller builds the set itself
    {
        return nullptr;
    }
}

SetPtr makeDummySet() { return std::make_shared<Set>(); }
/// A successfully built set is reused by later callers instead of being rebuilt.
void ReusesSuccessfullyBuiltSet()
{
    PreparedSetsCache cache;

    auto first = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(first.index(), 0u) << "The first caller must be asked to build the set";

    auto set = makeDummySet();
    std::get<0>(first).set_value(set);

    auto second = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(second.index(), 1u) << "A later caller must reuse the built set";
    EXPECT_EQ(std::get<1>(second).get(), set);
}

/// A cached null result is deliberately retryable: a later caller rebuilds it.
void RebuildsNullResult()
{
    PreparedSetsCache cache;

    auto first = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(first.index(), 0u);
    std::get<0>(first).set_value(nullptr);

    auto second = cache.findOrPromiseToBuild("key");
    EXPECT_EQ(second.index(), 0u) << "A null cached result must be rebuilt, not reused";
}

/// A build that threw stores an exception into the shared entry (see `CreatingSetsTransform::work`).
/// `findOrPromiseToBuild` must not rethrow it to a later, unrelated caller; it must drop the poisoned
/// entry and let that caller rebuild the set.
void RebuildsAfterBuilderException()
{
    PreparedSetsCache cache;

    auto builder = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(builder.index(), 0u);
    std::get<0>(builder).set_exception(
        std::make_exception_ptr(std::runtime_error("Limit for IN-set exceeded")));

    /// The next caller must neither throw nor inherit the failure - it must be asked to rebuild.
    std::variant<std::promise<SetPtr>, SharedSet> retry;
    ASSERT_NO_THROW(retry = cache.findOrPromiseToBuild("key"));
    ASSERT_EQ(retry.index(), 0u) << "A poisoned cache entry must be handed out for rebuilding";

    /// The rebuild succeeds and is then reused normally.
    auto set = makeDummySet();
    std::get<0>(retry).set_value(set);

    auto reuse = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(reuse.index(), 1u);
    EXPECT_EQ(std::get<1>(reuse).get(), set);
}

/// An abandoned build (the pipeline was stopped before the set was created) publishes a null set
/// rather than an exception, so a waiter rebuilds it instead of inheriting the failure. A build that
/// really failed still stores an exception, which every waiter must observe rather than retry: the
/// subquery would otherwise be rerun once per waiting part task.
void AbandonedBuildIsRetryableButRealFailureIsNot()
{
    PreparedSetsCache cache;

    auto abandoned = cache.findOrPromiseToBuild("abandoned");
    ASSERT_EQ(abandoned.index(), 0u);
    auto abandoned_waiter = cache.findOrPromiseToBuild("abandoned");
    ASSERT_EQ(abandoned_waiter.index(), 1u);

    /// What `CreatingSetsTransform`'s destructor publishes for a build that never produced a set.
    std::get<0>(abandoned).set_value(nullptr);
    EXPECT_EQ(std::get<1>(abandoned_waiter).get(), nullptr) << "An abandoned build must be retryable, not a failure";

    auto failed = cache.findOrPromiseToBuild("failed");
    ASSERT_EQ(failed.index(), 0u);
    auto failed_waiter = cache.findOrPromiseToBuild("failed");
    ASSERT_EQ(failed_waiter.index(), 1u);

    /// What `CreatingSetsTransform::work` publishes for a real build error, e.g. SET_SIZE_LIMIT_EXCEEDED.
    std::get<0>(failed).set_exception(std::make_exception_ptr(std::runtime_error("Limit for IN-set exceeded")));
    EXPECT_THROW((void)std::get<1>(failed_waiter).get(), std::runtime_error)
        << "A real build failure must reach the waiter instead of being retried";
}

/// A concurrent waiter observes a poisoned build (its `SharedSet::get` throws) and its mutation part
/// task fails once; a later attempt of the same mutation calls `findOrPromiseToBuild` again and is
/// handed a fresh promise to rebuild the set instead of inheriting the poisoned entry forever.
void WaiterFailureIsRebuildableOnRetry()
{
    PreparedSetsCache cache;

    auto builder = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(builder.index(), 0u);

    auto waiter = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(waiter.index(), 1u) << "A second concurrent caller must wait for the builder";

    std::get<0>(builder).set_exception(
        std::make_exception_ptr(std::runtime_error("Limit for IN-set exceeded")));

    /// The waiter sees the builder's failure (the mutation part task fails this attempt).
    EXPECT_THROW((void)std::get<1>(waiter).get(), std::runtime_error);

    /// A later attempt is asked to rebuild rather than getting the poisoned entry again.
    auto retry = cache.findOrPromiseToBuild("key");
    EXPECT_EQ(retry.index(), 0u);
}

/// A cache that doesn't wait for sets being built hands a second caller a promise to build the set once more while
/// the first build is in flight, leaves the entry to the first builder, and hands out the set once it is built.
void NonWaitingCacheBuildsAgainWhileInFlight()
{
    PreparedSetsCache cache(/* wait_for_sets_being_built_ = */ false);

    auto first = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(first.index(), 0u);
    EXPECT_EQ(cache.findBuilt("key"), nullptr) << "A set in flight is not built yet";

    auto second = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(second.index(), 0u) << "A second caller must not wait for a set in flight";

    /// The second build finishes first: its promise is not registered, so the entry still waits for the first.
    std::get<0>(second).set_value(makeDummySet());
    EXPECT_EQ(cache.findBuilt("key"), nullptr);

    auto set = makeDummySet();
    std::get<0>(first).set_value(set);
    EXPECT_EQ(cache.findBuilt("key"), set);

    auto third = cache.findOrPromiseToBuild("key");
    ASSERT_EQ(third.index(), 1u) << "A later caller must reuse the built set";
    EXPECT_EQ(std::get<1>(third).get(), set);
}

/// `findBuilt` never hands out an abandoned or failed build.
void FindBuiltSkipsAbandonedAndFailedBuilds()
{
    PreparedSetsCache cache;
    EXPECT_EQ(cache.findBuilt("missing"), nullptr);

    auto abandoned = cache.findOrPromiseToBuild("abandoned");
    ASSERT_EQ(abandoned.index(), 0u);
    std::get<0>(abandoned).set_value(nullptr);
    EXPECT_EQ(cache.findBuilt("abandoned"), nullptr);

    auto failed = cache.findOrPromiseToBuild("failed");
    ASSERT_EQ(failed.index(), 0u);
    std::get<0>(failed).set_exception(std::make_exception_ptr(std::runtime_error("Limit for IN-set exceeded")));
    std::optional<SetPtr> found;
    ASSERT_NO_THROW(found = cache.findBuilt("failed"));
    EXPECT_EQ(*found, nullptr);
}

int main() { ReusesSuccessfullyBuiltSet(); RebuildsNullResult(); RebuildsAfterBuilderException(); AbandonedBuildIsRetryableButRealFailureIsNot(); WaiterFailureIsRebuildableOnRetry(); NonWaitingCacheBuildsAgainWhileInFlight(); FindBuiltSkipsAbandonedAndFailedBuilds(); std::printf("%d tests, %d failures\n", 7, failures); return failures != 0; }
