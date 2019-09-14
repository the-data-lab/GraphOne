#pragma once

#include <omp.h>
#include <sys/mman.h>
#include <asm/mman.h>
#include <unistd.h>
#include <fcntl.h>


#include "type.h"
#include "graph.h"
#include "onekv.h"
#include "wtime.h"

template <class T>
class many2one: public pgraph_t<T> {
 public:
    using pgraph_t<T>::sgraph_in;
    using pgraph_t<T>::skv_out;
    using pgraph_t<T>::flag1;
    using pgraph_t<T>::flag2;
    using pgraph_t<T>::flag1_count;
    using pgraph_t<T>::flag2_count;
    using pgraph_t<T>::blog;
    
    using pgraph_t<T>::read_sgraph;
    using pgraph_t<T>::read_skv;
    using pgraph_t<T>::prep_sgraph;
    using pgraph_t<T>::prep_skv;
    using pgraph_t<T>::file_open_sgraph;
    using pgraph_t<T>::file_open_skv;
    using pgraph_t<T>::prep_sgraph_internal;
    using pgraph_t<T>::store_sgraph;
    using pgraph_t<T>::store_skv;

 public:
    static cfinfo_t* create_instance();
    
    void incr_count(sid_t src, sid_t dst, int del = 0);
    void add_nebr(sid_t src, sid_t dst, int del = 0);
    void prep_graph_baseline();
    void make_graph_baseline();
    void create_snapshot();
    void store_graph_baseline(bool clean = false);
    void read_graph_baseline();
    void file_open(const string& odir,  bool trunc);
    
    //status_t transform(srset_t* iset, srset_t* oset, direction_t direction);
    //virtual status_t extend(srset_t* iset, srset_t* oset, direction_t direction);
};

template <class T>
class one2one: public pgraph_t<T> {
 public:
    using pgraph_t<T>::skv_in;
    using pgraph_t<T>::skv_out;
    using pgraph_t<T>::flag1;
    using pgraph_t<T>::flag2;
    using pgraph_t<T>::flag1_count;
    using pgraph_t<T>::flag2_count;
    using pgraph_t<T>::blog;
    
    using pgraph_t<T>::read_skv;
    using pgraph_t<T>::prep_skv;
    using pgraph_t<T>::file_open_skv;
    using pgraph_t<T>::store_skv;
    using pgraph_t<T>::fill_skv;

 public:
    static cfinfo_t* create_instance();
    
    void incr_count(sid_t src, sid_t dst, int del = 0);
    void add_nebr(sid_t src, sid_t dst, int del = 0);
    void prep_graph_baseline();
    void make_graph_baseline();
    void create_snapshot();
    void store_graph_baseline(bool clean = false);
    void read_graph_baseline();
    void file_open(const string& odir,  bool trunc);
    
    //status_t transform(srset_t* iset, srset_t* oset, direction_t direction);
    //virtual status_t extend(srset_t* iset, srset_t* oset, direction_t direction);
};

template <class T>
class one2many: public pgraph_t<T> {
 public:
    using pgraph_t<T>::skv_in;
    using pgraph_t<T>::sgraph_out;
    using pgraph_t<T>::flag1;
    using pgraph_t<T>::flag2;
    using pgraph_t<T>::flag1_count;
    using pgraph_t<T>::flag2_count;
    using pgraph_t<T>::blog;
    
    using pgraph_t<T>::read_sgraph;
    using pgraph_t<T>::read_skv;
    using pgraph_t<T>::prep_sgraph;
    using pgraph_t<T>::prep_skv;
    using pgraph_t<T>::file_open_sgraph;
    using pgraph_t<T>::file_open_skv;
    using pgraph_t<T>::prep_sgraph_internal;
    using pgraph_t<T>::store_sgraph;
    using pgraph_t<T>::store_skv;

 public:
    static cfinfo_t* create_instance();
    
    void incr_count(sid_t src, sid_t dst, int del = 0);
    void add_nebr(sid_t src, sid_t dst, int del = 0);
    void prep_graph_baseline();
    void make_graph_baseline();
    void create_snapshot();
    void store_graph_baseline(bool clean = false);
    void read_graph_baseline();
    void file_open(const string& odir,  bool trunc);
    
    //status_t transform(srset_t* iset, srset_t* oset, direction_t direction);
    //virtual status_t extend(srset_t* iset, srset_t* oset, direction_t direction);
};


typedef one2many<dst_id_t> one2many_t;
typedef many2one<dst_id_t> many2one_t;
typedef one2one<dst_id_t> one2one_t;
typedef one2many<lite_edge_t> p_one2many_t;
typedef many2one<lite_edge_t> p_many2one_t;
typedef one2one<lite_edge_t> p_one2one_t;

/***************************************/
template <class T> 
void many2one<T>::prep_graph_baseline()
{
    this->alloc_edgelog(1 << BLOG_SHIFT);
    flag1_count = __builtin_popcountll(flag1);
    flag2_count = __builtin_popcountll(flag2);

    //super bins memory allocation
    tid_t   t_count = g->get_total_types();
    
    if (0 == sgraph_in) {
        sgraph_in  = (onegraph_t<T>**) calloc (sizeof(onegraph_t<T>*), t_count);
    }
    prep_sgraph(flag2, sgraph_in);
    
    if (0 == skv_out) {
        skv_out  = (onekv_t<T>**) calloc (sizeof(onekv_t<T>*), t_count);
    }

    prep_skv(flag1, skv_out);
}

template <class T> 
void many2one<T>::make_graph_baseline()
{
    /*
    if (blog->blog_tail >= blog->blog_marker) return;

    #pragma omp parallel num_threads (THD_COUNT) 
    {
#ifndef BULK
        //Now get the division of work
        vid_t     j_start;
        vid_t     j_end;
        vid_t tid = omp_get_thread_num();
        
        if (tid == 0) { 
            j_start = 0; 
        } else { 
            j_start = this->edge_shard->thd_local[tid - 1].range_end;
        }
        j_end = this->edge_shard->thd_local[tid].range_end;
        
        //actual work
        //this->make_on_classify(sgraph_in, this->edge_shard->global_range, j_start, j_end, 0); 
        //degree count
        this->calc_degree_noatomic(sgraph_in, edge_shard->global_range, j_start, j_end);
        //fill adj-list
        this->fill_adjlist_noatomic(sgraph_in, edge_shard->global_range, j_start, j_end);
        this->fill_skv_in(skv_out, this->edge_shard->global_range, j_start, j_end); 
        #pragma omp barrier 
        this->edge_shard->cleanup(); 
#else
        this->calc_edge_count_in(sgraph_in);
        
        //prefix sum then reset the count
        prep_sgraph_internal(sgraph_in);

        //populate and get the original count back
        //handle kv_out as well.
        this->fill_adj_list_in(skv_out, sgraph_in);
#endif
    }
    blog->blog_tail = blog->blog_marker;
    */
}

template <class T> 
void many2one<T>::store_graph_baseline(bool clean)
{
    store_skv(skv_out);
    store_sgraph(sgraph_in);
}

template <class T> 
void many2one<T>::file_open(const string& odir, bool trunc)
{
    string postfix = "in";
    file_open_sgraph(sgraph_in, odir, postfix, trunc);
    postfix = "out";
    file_open_skv(skv_out, odir, postfix, trunc);
}

template <class T> 
void many2one<T>::read_graph_baseline()
{
    tid_t   t_count = g->get_total_types();
    
    if (0 == skv_out) {
        skv_out  = (onekv_t<T>**) calloc (sizeof(onekv_t<T>*), t_count);
    }
    read_skv(skv_out);
    
    if (0 == sgraph_in) {
        sgraph_in  = (onegraph_t<T>**) calloc (sizeof(onegraph_t<T>*), t_count);
    }
    read_sgraph(sgraph_in);
}

/*******************************************/
template <class T> 
void one2many<T>::prep_graph_baseline()
{
    this->alloc_edgelog(1 << BLOG_SHIFT);
    flag1_count = __builtin_popcountll(flag1);
    flag2_count = __builtin_popcountll(flag2);

    //super bins memory allocation
    tid_t   t_count = g->get_total_types();
    
    if (0 == sgraph_out) {
        sgraph_out  = (onegraph_t<T>**) calloc (sizeof(onegraph_t<T>*), t_count);
    }
    prep_sgraph(flag1, sgraph_out);
    
    if (0 == skv_in) {
        skv_in  = (onekv_t<T>**) calloc (sizeof(onekv_t<T>*), t_count);
    }
    
    prep_skv(flag2, skv_in);
}
    
template <class T> 
void one2many<T>::make_graph_baseline()
{
    if (blog->blog_tail >= blog->blog_marker) return;

    #pragma omp parallel num_threads (THD_COUNT) 
    {
#ifndef BULK
        this->edge_shard->classify_uni(this);
        
        //Now get the division of work
        vid_t     j_start;
        vid_t     j_end;
        
        vid_t tid = omp_get_thread_num();
        
        if (tid == 0) { 
            j_start = 0; 
        } else { 
            j_start = this->edge_shard->thd_local[tid - 1].range_end;
        }
        j_end = this->edge_shard->thd_local[tid].range_end;
        
        this->fill_skv_in(skv_in, this->edge_shard->global_range, j_start, j_end); 
        #pragma omp barrier
        this->edge_shard->cleanup(); 
    
#else 
        this->calc_edge_count_out(sgraph_out);
        
        //prefix sum then reset the count
        prep_sgraph_internal(sgraph_out);

        //populate and get the original count back
        this->fill_adj_list_out(sgraph_out, skv_in);
        //update_count(sgraph_out);
#endif
    }
    blog->blog_tail = blog->blog_marker;  
}

template <class T> 
void one2many<T>::store_graph_baseline(bool clean)
{
    store_sgraph(sgraph_out);
    store_skv(skv_in);
}

template <class T> 
void one2many<T>::file_open(const string& odir, bool trunc)
{
    string postfix = "out";
    file_open_sgraph(sgraph_out, odir, postfix, trunc);
    postfix = "in";
    file_open_skv(skv_in, odir, postfix, trunc);
}

template <class T> 
void one2many<T>::read_graph_baseline()
{
    tid_t   t_count = g->get_total_types();
    
    if (0 == sgraph_out) {
        sgraph_out  = (onegraph_t<T>**) calloc (sizeof(onegraph_t<T>*), t_count);
    }
    read_sgraph(sgraph_out);
    
    if (0 == skv_in) {
        skv_in  = (onekv_t<T>**) calloc (sizeof(onekv_t<T>*), t_count);
    }
    read_skv(skv_in);
}

/************************************************/
template <class T> 
void one2one<T>::prep_graph_baseline()
{
    this->alloc_edgelog(1 << BLOG_SHIFT);
    flag1_count = __builtin_popcountll(flag1);
    flag2_count = __builtin_popcountll(flag2);
    tid_t   t_count    = g->get_total_types();

    //super bins memory allocation
    
    if (0 == skv_in) {
        skv_in  = (onekv_t<T>**) calloc (sizeof(onekv_t<T>*), t_count);
    }
    prep_skv(flag2, skv_in);
    
    if (0 == skv_out) {
        skv_out  = (onekv_t<T>**) calloc (sizeof(onekv_t<T>*), t_count);
    }
    prep_skv(flag1, skv_out);
}

template <class T> 
void one2one<T>::make_graph_baseline()
{
    if (blog->blog_tail >= blog->blog_marker) return;

    //handle kv_out as well as kv_in.
    fill_skv(skv_out, skv_in);
    
}

template <class T> 
void one2one<T>::store_graph_baseline(bool clean)
{
    store_skv(skv_out);
    store_skv(skv_in);
}

template <class T> 
void one2one<T>::file_open(const string& odir, bool trunc)
{
    string postfix = "out";
    file_open_skv(skv_out, odir, postfix, trunc);
    postfix = "in";
    file_open_skv(skv_in, odir, postfix, trunc);
}

template <class T> 
void one2one<T>::read_graph_baseline()
{
    tid_t   t_count    = g->get_total_types();
    
    if (0 == skv_out) {
        skv_out  = (onekv_t<T>**) calloc (sizeof(onekv_t<T>*), t_count);
    }
    read_skv(skv_out);
    
    if (0 == skv_in) {
        skv_in  = (onekv_t<T>**) calloc (sizeof(onekv_t<T>*), t_count);
    }
    read_skv(skv_in);
}

template <class T> 
cfinfo_t* ugraph<T>::create_instance()
{
    return new ugraph<T>;
}

template <class T> 
cfinfo_t* dgraph<T>::create_instance()
{
    return new dgraph<T>;
}

template <class T> 
cfinfo_t* one2one<T>::create_instance()
{
    return new one2one<T>;
}

template <class T> 
cfinfo_t* one2many<T>::create_instance()
{
    return new one2many<T>;
}

template <class T> 
cfinfo_t* many2one<T>::create_instance()
{
    return new many2one_t;
}
//////
template <class T> 
void one2one<T>::incr_count(sid_t src, sid_t dst, int del /*= 0*/)
{
}

template <class T> 
void one2many<T>::incr_count(sid_t src, sid_t dst, int del /*= 0*/)
{
    tid_t dst_index = TO_TID(dst);
    
    vid_t vert2_id = TO_VID(dst);
    
    if (!del) { 
        sgraph_out[dst_index]->increment_count(vert2_id);
    } else { 
        skv_in[dst_index]->decrement_count(vert2_id);
    }
}

template <class T> 
void many2one<T>::incr_count(sid_t src, sid_t dst, int del /*= 0*/)
{
    tid_t src_index = TO_TID(src);
    
    vid_t vert1_id = TO_VID(src);
    
    if (!del) { 
        skv_out[src_index]->increment_count(vert1_id);
    } else { 
        sgraph_in[src_index]->decrement_count(vert1_id);
    }
}

template <class T> 
void one2one<T>::create_snapshot()
{
    return;
}

template <class T> 
void one2many<T>::create_snapshot()
{
    update_count(sgraph_out);
}

template <class T> 
void many2one<T>::create_snapshot()
{
    update_count(sgraph_in);
}

template <class T> 
void one2one<T>::add_nebr(sid_t src, sid_t dst, int del /*= 0*/)
{
    tid_t src_index = TO_TID(src);
    tid_t dst_index = TO_TID(dst);
    
    vid_t vert1_id = TO_VID(src);
    vid_t vert2_id = TO_VID(dst);
    
    if (!del) { 
        skv_out[src_index]->set_value(vert1_id, dst);
        skv_in[dst_index]->set_value(vert2_id, src);
    } else { 
        skv_out[src_index]->set_value(vert1_id, dst);
        skv_in[dst_index]->set_value(vert2_id, src);
    }
}

template <class T> 
void many2one<T>::add_nebr(sid_t src, sid_t dst, int del /*= 0*/)
{
    tid_t src_index = TO_TID(src);
    tid_t dst_index = TO_TID(dst);
    
    vid_t vert1_id = TO_VID(src);
    vid_t vert2_id = TO_VID(dst);
    
    if (!del) { 
        sgraph_in[dst_index]->add_nebr(vert2_id, src);
        skv_out[src_index]->set_value(vert1_id, dst);
    } else { 
        sgraph_in[dst_index]->del_nebr(vert2_id, src);
        skv_out[src_index]->set_value(vert1_id, dst);
    }
}

template <class T> 
void one2many<T>::add_nebr(sid_t src, sid_t dst, int del /*= 0*/)
{
    tid_t src_index = TO_TID(src);
    tid_t dst_index = TO_TID(dst);
    
    vid_t vert1_id = TO_VID(src);
    vid_t vert2_id = TO_VID(dst);
    
    if (!del) { 
        sgraph_out[src_index]->add_nebr(vert1_id, dst);
        skv_in[dst_index]->set_value(vert2_id, src);
    } else { 
        sgraph_out[src_index]->del_nebr(vert1_id, dst);
        skv_in[dst_index]->set_value(vert2_id, src);
    }
}

/*
//due to many2one structure, we give preference to bottom up approach
template <class T>
status_t many2one<T>::extend(srset_t* iset, srset_t* oset, direction_t direction)
{
    if (direction == eout) {
        return extend_kv_td(skv_out,  iset, oset);
    } else {
        assert(direction == ein);
        return extend_adjlist_td(sgraph_in, iset, oset);
    }
    return eOK;
}

template <class T>
status_t dgraph<T>::extend(srset_t* iset, srset_t* oset, direction_t direction)
{
    if (direction == eout) {
        return extend_adjlist_td(sgraph_out, iset, oset);
    } else {
        assert(direction == ein);
        return extend_adjlist_td(sgraph_in, iset, oset);
    }
    return eOK;
}

template <class T>
status_t one2one<T>::extend(srset_t* iset, srset_t* oset, direction_t direction)
{
    if (direction == eout) {
        return extend_kv_td(skv_out, iset, oset);
    } else {
        assert(direction == ein);
        return extend_kv_td(skv_in, iset, oset);
    }
    return eOK;
}

template <class T>
status_t one2many<T>::extend(srset_t* iset, srset_t* oset, direction_t direction)
{
    if (direction == eout) {
        return extend_adjlist_td(sgraph_out, iset, oset);
    } else {
        assert(direction == ein);
        return extend_kv_td(skv_in, iset, oset);
    }
    return eOK;
}

//due to many2one structure, we give preference to bottom up approach
template <class T>
status_t many2one<T>::transform(srset_t* iset, srset_t* oset, direction_t direction)
{
    int total_count = 0;

    if (direction == eout) {
        oset->full_setup(sgraph_in);
        total_count = 0;
        if (iset->get_total_vcount() <= bu_factor*total_count) { //top down approach
            return query_kv_td(skv_out, iset, oset);
        } else { //bottom up approach
            return query_adjlist_bu(sgraph_in, iset, oset);
        }
    } else {
        assert(direction == ein);
        oset->full_setup(skv_out);
        total_count = 0;
        if (iset->get_total_vcount() <= bu_factor*total_count) { //top down approach
            return query_adjlist_td(sgraph_in, iset, oset);
        } else { //bottom up approach 
            return query_kv_bu(skv_out, iset, oset);
        }
    }
    return eOK;
}

template <class T>
status_t one2one<T>::transform(srset_t* iset, srset_t* oset, direction_t direction)
{
    int total_count = 0;
    if (direction == eout) {
        total_count = 0;
        oset->full_setup(skv_in);
        if (iset->get_total_vcount() <= bu_factor*total_count) { //top down approach
            return query_kv_td(skv_out, iset, oset);
        } else { //bottom up approach
            return query_kv_bu(skv_in, iset, oset);
        }
    } else {
        assert(direction == ein);
        total_count = 0;
        oset->full_setup(skv_out);
        if (iset->get_total_vcount() <= bu_factor*total_count) { //top down approach
            return query_kv_td(skv_in, iset, oset);
        } else { //bottom up approach 
            return query_kv_bu(skv_out, iset, oset);
        }
    }
    return eOK;
}

template <class T>
status_t one2many<T>::transform(srset_t* iset, srset_t* oset, direction_t direction)
{
    int total_count = 0;
    if (direction == eout) {
        total_count = 0;
        oset->full_setup(skv_in);
        if (iset->get_total_vcount() <= bu_factor*total_count) { //top down approach
            return query_adjlist_td(sgraph_out, iset, oset);
        } else { //bottom up approach
            return query_kv_bu(skv_in, iset, oset);
        }
    } else {
        assert(direction == ein);
        total_count = 0;
        oset->full_setup(sgraph_out);
        if (iset->get_total_vcount() <= bu_factor*total_count) { //top down approach
            return query_kv_td(skv_in, iset, oset);
        } else { //bottom up approach 
            return query_adjlist_bu(sgraph_out, iset, oset);
        }
    }
    return eOK;
}*/

