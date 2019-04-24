#pragma once

#include "type.h"
#include "str.h"

class stringkv_t : public cfinfo_t {
 protected:
     strkv_t** strkv_out;
 public:
    inline stringkv_t() {
        strkv_out = 0;
    }
    static cfinfo_t* create_instance();
    status_t batch_edge(edgeT_t<char*>& edge);
    status_t batch_update(const string& src, const string& dst, propid_t pid = 0);
    
    void prep_graph_baseline();
    void make_graph_baseline();
    void store_graph_baseline(bool clean = false);
    void read_graph_baseline();
    void file_open(const string& dir, bool trunc);

    //inline const char* get_value(tid_t tid, vid_t vid) 
    inline const char* get_value(sid_t sid) {
        vid_t vid = TO_VID(sid);
        tid_t tid = TO_TID(sid);
        return strkv_out[tid]->get_value(vid);
    }

    inline void print_raw_dst(tid_t tid, vid_t vid, propid_t pid = 0) {
        cout << strkv_out[tid]->kv[vid];
    }
};

