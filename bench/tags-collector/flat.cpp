// Flat layout estimate: one arena for strings, groups as offset ranges, open-addressing map of group indices.
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>
#include <algorithm>
#include <cstdint>
#include <memory>
using namespace std;
static uint64_t h64(const char* p, size_t n, uint64_t seed){ uint64_t h=seed^1469598103934665603ULL; for(size_t i=0;i<n;++i){h^=(unsigned char)p[i]; h*=1099511628211ULL;} return h; }
static uint64_t mix(uint64_t x){x^=x>>33;x*=0xff51afd7ed558ccdULL;x^=x>>33;x*=0xc4ceb9fe1a85ec53ULL;x^=x>>33;return x;}
struct Row{ string v[7]; };
static vector<Row> gen(size_t n){ static const char* modes[]={"user","system","idle","iowait","nice","irq","softirq","steal"}; vector<Row> r(n);
 for(size_t i=0;i<n;++i){uint64_t h=mix(i); r[i].v[0]="c"+to_string((h/5000)%5); r[i].v[1]="host-"+to_string((h/10)%500)+":9100"; r[i].v[2]="job"+to_string(h%10);
  r[i].v[3]=modes[(h/500000)%8]; r[i].v[4]="ns"+to_string((h/25000)%20); r[i].v[5]="pod-"+to_string(i); r[i].v[6]="metric_"+to_string(i<100000?0:(i%199)+1);} return r; }
static const char* names[7]={"cluster","instance","job","mode","namespace","pod","__name__"};
static double now(){return chrono::duration<double,milli>(chrono::steady_clock::now().time_since_epoch()).count();}
struct Arena{ vector<unique_ptr<char[]>> chunks; char* p=nullptr; size_t left=0; string_view put(string_view s){ if(s.size()>left){ size_t sz=max<size_t>(1<<20,s.size()); chunks.emplace_back(new char[sz]); p=chunks.back().get(); left=sz;} memcpy(p,s.data(),s.size()); string_view r(p,s.size()); p+=s.size(); left-=s.size(); return r; } };
struct OA{ vector<uint64_t> slots; vector<uint64_t> hs; size_t cnt=0; // slot = group+1
  void grow(){ size_t n=slots.empty()?1024:slots.size()*2; vector<uint64_t> s(n,0),h(n); for(size_t i=0;i<slots.size();++i) if(slots[i]){size_t j=hs[i]&(n-1); while(s[j]) j=(j+1)&(n-1); s[j]=slots[i]; h[j]=hs[i];} slots.swap(s); hs.swap(h);} };
int main(){ size_t n=400000, block=65536; auto rows=gen(n); double tb=0,ti=0,td;
 { Arena arena; vector<pair<string_view,string_view>> tags; vector<pair<uint32_t,uint32_t>> groups; OA m; vector<pair<uint64_t,uint64_t>> idmap_flat; OA idm;
   vector<pair<string_view,string_view>> row;
   for(size_t b=0;b<n;b+=block){ size_t e=min(n,b+block);
     for(size_t i=b;i<e;++i){ double a=now(); row.clear(); for(int j=0;j<6;++j) row.emplace_back(names[j], rows[i].v[j]); row.emplace_back(names[6], rows[i].v[6]);
       sort(row.begin(),row.end(),[](auto&x,auto&y){return x.first<y.first;}); uint64_t h=0; for(auto&[k,v]:row){h=h64(k.data(),k.size(),h);h=h64(v.data(),v.size(),h);} double c=now(); tb+=c-a;
       if((m.cnt+1)*2>m.slots.size()) m.grow(); size_t mask=m.slots.size()-1, j=h&mask; uint64_t g=0;
       while(m.slots[j]){ if(m.hs[j]==h){ auto [o,l]=groups[m.slots[j]-1]; if(l==row.size() && equal(row.begin(),row.end(),tags.begin()+o)){g=m.slots[j];break;} } j=(j+1)&mask; }
       if(!g){ uint32_t o=tags.size(); for(auto&[k,v]:row) tags.emplace_back(arena.put(k),arena.put(v)); groups.emplace_back(o,row.size()); g=groups.size(); m.slots[j]=g; m.hs[j]=h; ++m.cnt; }
       if((idm.cnt+1)*2>idm.slots.size()) idm.grow(); uint64_t id=mix(i); size_t k=id&(idm.slots.size()-1); while(idm.slots[k]) k=(k+1)&(idm.slots.size()-1); idm.slots[k]=g; idm.hs[k]=id; ++idm.cnt;
       ti+=now()-c; } }
   td=now(); }
 td=now()-td; printf("flat build_tags=%.0fms insert=%.0fms destroy=%.0fms\n",tb,ti,td); }
