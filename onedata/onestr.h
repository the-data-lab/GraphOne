
#pragma once
#include "str2sid.h"
#include "type.h"
#include "str.h"
using  std::map;

class strkv_t {
 public:
    sid_t* kv;
    tid_t  tid;
    vid_t  max_vcount;
    
    str_t  mem;
    //vertex table file related log
    /*disk_strkv_t* dvt;
    vid_t    dvt_count; 
    vid_t    dvt_max_count;
    */

    int    vtf;   //vertex table file
    int    etf;   //edge table file
    
 public:
    strkv_t(); 
 
 public: 
    void setup(tid_t tid, vid_t v_count); 
    void set_value(vid_t vid, const char* value); 
    const char* get_value(vid_t vid);
    void handle_write();
    void read_vtable();
    void read_etable();
    // void prep_str2sid(map<string, sid_t>& str2sid);
    void prep_str2sid(str2intmap& str2sid);
    void file_open(const string& filename, bool trunc);
    
    /*
    inline char* alloc_mem(size_t sz) {
        char* ptr = log_beg +  log_head;
        log_head += sz;
        return ptr;
    }*/
};
