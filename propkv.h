#pragma once

#include "type.h"

class disk_propkv_t {
    public:
    vid_t vid;
    univ_t univ;
};

class propkv_t {
 public:
    univ_t* kv;
    sid_t super_id;
    vid_t max_vcount;
    
    //vertex table file related log
    disk_propkv_t* dvt;
    vid_t    dvt_count; 
    vid_t    dvt_max_count;

    FILE*    vtf;   //vertex table file
    
    friend class labelkv_t;

 public:
    propkv_t(); 
 
 public: 
    void setup(tid_t tid); 
    void set_value(vid_t vid, univ_t value); 
    void persist_kvlog(const string& vtfile);
    void read_vtable(const string& vtfile);
    
};

class labelkv_t : public cfinfo_t {
 protected:
     propkv_t** kv_out;
     prop_encoder_t* encoder;

 public:
    inline labelkv_t() {
        kv_out = 0;
        encoder = 0;
    }

    static cfinfo_t* create_instance();
   
    //used for encoder only
    void add_edge_property(const char* longname, prop_encoder_t* prop_encoder);
    status_t batch_update(const string& src, const string& dst, propid_t pid = 0);
    void make_graph_baseline();
    void store_graph_baseline(string dir);
    void read_graph_baseline(const string& dir);

    propkv_t** prep_propkv();
    void fill_kv_out();

    inline void print_raw_dst(tid_t tid, vid_t vid, propid_t pid = 0) {
        encoder->print(kv_out[tid]->kv[vid]);
    }
};
