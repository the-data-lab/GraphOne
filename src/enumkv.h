#pragma once
#include <map>
#include <string>
#include "num.h"
#include "cf_info.h"
#include "type.h"

using std::map;
using std::string;


//generic enum class
class enumkv_t : public cfinfo_t  {
  private:
    //mapping between enum and string
    map<string, uint8_t> str2enum;
    char**         enum2str;
    uint8_t        ecount;
    uint8_t        max_ecount;
  
    kvT_t<uint8_t>** numkv_out;

    FILE* vtf;
  
 public:
    enumkv_t();
    void init_enum(int enumcount);
    void populate_enum(const char* e);

    status_t batch_edge(edgeT_t<char*>& edge);
    status_t batch_update(const string& src, const string& dst, propid_t pid = 0);
    
    void make_graph_baseline();
    void store_graph_baseline(bool clean =false);
    void read_graph_baseline();
    void file_open(const string& odir, bool trunc); 
    void prep_graph_baseline();
};
