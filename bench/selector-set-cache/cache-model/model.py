# Builds model.cpp: PreparedSetsCache's member bodies and the gtest cases, verbatim from the branch, with Set stubbed
# and the class declaration written out here (mirrors src/Interpreters/PreparedSets.h). Run: python3 model.py && g++ -std=c++20 -o model model.cpp && ./model
import re
R='/root/nerve-workspace/repos/ClickHouse/'
src=open(R+'src/Interpreters/PreparedSets.cpp').read()
i=src.index('std::variant<std::promise<SetPtr>, SharedSet> PreparedSetsCache::findOrPromiseToBuild')
bodies=src[i:src.index('\n};', i)]
t=open(R+'src/Interpreters/tests/gtest_prepared_sets_cache.cpp').read()
tests=re.sub(r'TEST\(PreparedSetsCache, (\w+)\)', r'void \1()', t[t.index('/// A successfully built set'):])
names=re.findall(r'void (\w+)\(\)', tests)
hdr=open(R+'src/Interpreters/PreparedSets.h').read()
ctor=re.search(r'    explicit PreparedSetsCache\(.*?\n', hdr).group(0)
prog='''#include <future>
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
#define ASSERT_EQ(a,b) if (!((a)==(b))) { std::printf("ASSERT_EQ failed %s:%d\\n", __func__, __LINE__); ++failures; return; } else Sink()
#define EXPECT_EQ(a,b) if (!((a)==(b))) { std::printf("EXPECT_EQ failed %s:%d\\n", __func__, __LINE__); ++failures; } else Sink()
#define EXPECT_THROW(e,x) do { bool t=false; try { e; } catch (const x &) { t=true; } if(!t){std::printf("EXPECT_THROW failed %s:%d\\n", __func__, __LINE__); ++failures;} } while(0); Sink()
#define ASSERT_NO_THROW(e) try { e; } catch (...) { std::printf("ASSERT_NO_THROW failed %s\\n", __func__); ++failures; return; }
class PreparedSetsCache
{
public:
''' + ctor + '''    std::variant<std::promise<SetPtr>, SharedSet> findOrPromiseToBuild(const String & key);
    SetPtr findBuilt(const String & key);
private:
    struct Entry { SharedSet future; };
    const bool wait_for_sets_being_built;
    std::mutex cache_mutex;
    std::unordered_map<String, Entry> cache;
};
''' + bodies + '''
SetPtr makeDummySet() { return std::make_shared<Set>(); }
''' + tests + '\nint main() { ' + ' '.join(n + '();' for n in names) + f' std::printf("%d tests, %d failures\\n", {len(names)}, failures); return failures != 0; }}\n'
open('model.cpp','w').write(prog)
