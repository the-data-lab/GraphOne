#pragma once

#include "graph_base.h"
#include "log.h"

template <class T>
class gview_t {
 public:
    pgraph_t<T>*    pgraph;  
    snapshot_t*     snapshot;
    pthread_t       thread;
    void*           algo_meta;//algorithm specific data
    vid_t           v_count;
    int             flag;
    typename callback<T>::sfunc   sstream_func; 
    
    virtual degree_t get_nebrs_out(vid_t vid, T* ptr) {assert(0); return 0;}
    virtual degree_t get_nebrs_in (vid_t vid, T* ptr) {assert(0); return 0;}
    virtual degree_t get_degree_out(vid_t vid) {assert(0); return 0;}
    virtual degree_t get_degree_in (vid_t vid) {assert(0); return 0;}
    
    virtual delta_adjlist_t<T>* get_nebrs_archived_out(vid_t) {assert(0); return 0;}
    virtual delta_adjlist_t<T>* get_nebrs_archived_in(vid_t) {assert(0); return 0;}
    virtual index_t get_nonarchived_edges(edgeT_t<T>*& ptr) {assert(0); return 0;}
    
    virtual status_t    update_view() {assert(0); return eOK;}
    virtual bool has_vertex_changed_out(vid_t v) { assert(0); return false;}
    virtual bool has_vertex_changed_in(vid_t v) { assert(0); return false;}
    virtual vid_t    get_vcount() { return v_count; }
    virtual int      get_snapid() { assert(0); return 0; }
    
    inline virtual ~gview_t() {} 
};
