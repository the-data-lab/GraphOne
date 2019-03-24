#pragma once

#include <map>
#include <string>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>

#include "type.h"
#include "cf_info.h"
#include "graph_base.h"
#include "prop_encoder.h"

//#include "rset.h"
//#include "//query_clause.h"

using std::map;
using std::cout;
using std::endl;
using std::string;
using std::swap;
using std::pair;

    
void* alloc_buf();


//class pkv_t;
class graph;
extern graph* g;

class typekv_t;

typedef cfinfo_t* (*get_graph_instance)();
typedef prop_encoder_t* (*get_encoder_instance)();

extern map<string, get_graph_instance>  graph_instance;
extern map<string, get_encoder_instance>  encoder_instance;

////////////main class/////////////////////
class graph {
 public:
    //graphs and labels store.
    map <string, propid_t> str2pid;
    cfinfo_t** cf_info;
    pinfo_t *  p_info;
    
    batchinfo_t* batch_info;
    batchinfo_t* batch_info1;
    
    uint8_t      batch_count;
    uint8_t      batch_count1;
    int          cf_count;
    propid_t     p_count;

    //Other information
    vid_t     vert_count;
    typekv_t* typekv;
    string    odirname;
    
    //threads
    pthread_t snap_thread;
    pthread_mutex_t snap_mutex;
    pthread_cond_t  snap_condition;
    
    pthread_t w_thread;
    pthread_mutex_t w_mutex;
    pthread_cond_t  w_condition;

    index_t snap_id;

 public:
    graph();
    inline void set_odir(const string& odir) {
        odirname = odir;
        //snapfile = odir + "graph.snap";
    }
    void register_instances();
    void create_schema(propid_t count, const string& conf_file);
    
    snapid_t get_snapid();
    void incr_snapid();
    inline cfinfo_t* get_sgraph(propid_t cfid) { return cf_info[cfid];}
    inline typekv_t* get_typekv() { return (typekv_t*)cf_info[0]; }
    vid_t get_type_scount(tid_t type = 0);
    vid_t get_type_vcount(tid_t type);
    tid_t get_total_types();
	tid_t get_tid(const char* type);
    sid_t type_update(const string& src, const string& dst);
    sid_t type_update(const string& src, tid_t tid = 0);
    void  type_done();
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
    void calc_degree();
    void make_graph_baseline();
    void create_snapshot();
    void write_edgelog();
    void store_graph_baseline(bool clean = false);
    void read_graph_baseline();
    void file_open(bool trunc);
    
    void create_snapthread();
    static void* snap_func(void* arg);

    void create_wthread();
    static void* w_func(void* arg);
};


