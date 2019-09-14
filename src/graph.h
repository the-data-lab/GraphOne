#pragma once

#include "type.h"
#include "cf_info.h"

class typekv_t;

//For many purpose. Will be removed ultimately
extern index_t residue;
extern vid_t _global_vcount;
//Required for exiting from the streaming computation
extern index_t _edge_count;
extern int _dir, _persist, _source;

////////////main class/////////////////////
class graph {
 public:
    //graphs and labels store.
    map <string, propid_t> str2pid;
    cfinfo_t** cf_info;
    pinfo_t *  p_info;
    
    int          cf_count;
    propid_t     p_count;

    //Other information
    typekv_t* typekv;
    string    odirname;
    
 public:
    graph();
    inline void set_odir(const string& odir) {
        odirname = odir;
        //snapfile = odir + "graph.snap";
    }
    void register_instances();
    void create_schema(propid_t count, const string& conf_file);
    
    inline cfinfo_t* get_sgraph(propid_t cfid) { return cf_info[cfid];}
    inline typekv_t* get_typekv() { return (typekv_t*)cf_info[0]; }
    vid_t get_type_scount(tid_t type = 0);
    vid_t get_type_vcount(tid_t type);
    tid_t get_total_types();
	tid_t get_tid(const char* type);
    sid_t type_update(const string& src, const string& dst);
    sid_t type_update(const string& src, tid_t tid = 0);
    sid_t get_sid(const char* src);
    void  type_store(const string& odir);


    void add_columnfamily(cfinfo_t* cf, propid_t p_count = 1);
    status_t add_property(const char* longname);
    propid_t get_cfid(propid_t pid);
    propid_t get_cfid(const char* property);
    propid_t get_pid(const char* property);
    
    //queries
    //void run_query(query_clause* q);
    
    status_t batch_update(const string& src, const string& dst, const string& predicate);
    //useful or properties
    status_t batch_update(sid_t src_id, const string& dst, const string& predicate);
    //For edges with properties.
    status_t batch_update(const string& src, const string& dst, const string& predicate, 
                          const char* property_str);
    status_t batch_update(const string& src, const string& dst, propid_t pid, 
                          propid_t count, prop_pair_t* prop_pair);
    
    void prep_graph_baseline();
    void swap_log_buffer();
    void waitfor_archive();
    void make_graph_baseline();
    void create_snapshot();
    void write_edgelog();
    void store_graph_baseline(bool clean = false);
    void read_graph_baseline();
    void file_open(bool trunc);
    
    void create_threads(bool snap_thd, bool w_thd);
};

extern graph* g;

