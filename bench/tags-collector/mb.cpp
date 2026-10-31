// Microbenchmark of the ContextTimeSeriesTagsCollector storage pattern (not ClickHouse code).
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <cstdint>
using namespace std;
using Tags = vector<pair<string,string>>; using TagsPtr = shared_ptr<const Tags>;
static uint64_t h64(const char* p, size_t n, uint64_t seed){ uint64_t h=seed^1469598103934665603ULL; for(size_t i=0;i<n;++i){h^=(unsigned char)p[i]; h*=1099511628211ULL;} return h; }
struct Key{ TagsPtr t; uint64_t h; };
struct KH{ size_t operator()(const Key&k)const{return k.h;} };
struct KE{ bool operator()(const Key&a,const Key&b)const{return *a.t==*b.t;} };
static uint64_t mix(uint64_t x){x^=x>>33;x*=0xff51afd7ed558ccdULL;x^=x>>33;x*=0xc4ceb9fe1a85ec53ULL;x^=x>>33;return x;}
struct Row{ string v[7]; };
static vector<Row> gen(size_t n){ static const char* modes[]={"user","system","idle","iowait","nice","irq","softirq","steal"}; vector<Row> r(n);
 for(size_t i=0;i<n;++i){uint64_t h=mix(i); r[i].v[0]="c"+to_string((h/5000)%5); r[i].v[1]="host-"+to_string((h/10)%500)+":9100"; r[i].v[2]="job"+to_string(h%10);
  r[i].v[3]=modes[(h/500000)%8]; r[i].v[4]="ns"+to_string((h/25000)%20); r[i].v[5]="pod-"+to_string(i); r[i].v[6]="metric_"+to_string(i<100000?0:(i%199)+1);} return r; }
static const char* names[7]={"cluster","instance","job","mode","namespace","pod","__name__"};
static double now(){return chrono::duration<double,milli>(chrono::steady_clock::now().time_since_epoch()).count();}
int main(int argc,char**argv){ size_t n=400000, block=65536; int variant=atoi(argv[1]); auto rows=gen(n);
 double t0=now(), tb=0, ti=0, td=0;
 { vector<TagsPtr> groups; unordered_map<Key,uint64_t,KH,KE> gft; unordered_map<uint64_t,uint64_t> idmap;
   if(variant==1){ gft.reserve(n); idmap.reserve(n); groups.reserve(n);} if(variant==2){ gft.reserve(n);} if(variant==3){ idmap.reserve(n);} // upper bound, as if reserved per batch geometrically
   for(size_t b=0;b<n;b+=block){ size_t e=min(n,b+block);
     double a=now(); vector<TagsPtr> tv; tv.reserve(e-b);
     for(size_t i=b;i<e;++i){ Tags t; t.reserve(7); for(int j=0;j<6;++j) t.emplace_back(names[j], rows[i].v[j]); t.emplace_back(names[6], rows[i].v[6]);
       sort(t.begin(),t.end(),[](auto&x,auto&y){return x.first<y.first;}); tv.emplace_back(make_shared<Tags>(std::move(t))); }
     double c=now(); tb+=c-a;
     for(size_t i=b;i<e;++i){ uint64_t h=0; for(auto&[k,v]:*tv[i-b]){h=h64(k.data(),k.size(),h);h=h64(v.data(),v.size(),h);}
       auto [it,ins]=gft.try_emplace(Key{tv[i-b],h},groups.size()); if(ins) groups.push_back(tv[i-b]); idmap.emplace(mix(i),it->second);} 
     ti+=now()-c; }
   t0=now()-t0; double d=now();
   // destructor runs at scope exit
   groups.clear(); groups.shrink_to_fit(); gft.clear(); idmap.clear(); td=now()-d; }
 printf("variant=%d build_tags=%.0fms insert=%.0fms destroy=%.0fms\n",variant,tb,ti,td); }
