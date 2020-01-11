#pragma once
#include "pgraph.h"

template <class T>
class ugraph: public pgraph_t<T> {
 public:
    using pgraph_t<T>::sgraph;
    using pgraph_t<T>::flag1;
    using pgraph_t<T>::flag2;
    using pgraph_t<T>::flag1_count;
    using pgraph_t<T>::flag2_count;
    using pgraph_t<T>::blog;

    using pgraph_t<T>::read_sgraph;
    using pgraph_t<T>::prep_sgraph;
    using pgraph_t<T>::file_open_sgraph;
    using pgraph_t<T>::prep_sgraph_internal;
    using pgraph_t<T>::store_sgraph;


 public:
    static cfinfo_t* create_instance();
    
    void prep_graph_baseline(egraph_t egraph_type=eADJ);
    void make_graph_baseline();
    void compress_graph_baseline();
    void store_graph_baseline(bool clean = false);
    void read_graph_baseline();
    void file_open(const string& odir,  bool trunc);
    
    //status_t transform(srset_t* iset, srset_t* oset, direction_t direction);
    //virtual status_t extend(srset_t* iset, srset_t* oset, direction_t direction);
};

template <class T>
class dgraph: public pgraph_t<T> {
 public:
    using pgraph_t<T>::sgraph_in;
    using pgraph_t<T>::sgraph_out;
    using pgraph_t<T>::flag1;
    using pgraph_t<T>::flag2;
    using pgraph_t<T>::flag1_count;
    using pgraph_t<T>::flag2_count;
    using pgraph_t<T>::blog;
    
    using pgraph_t<T>::prep_sgraph;
    using pgraph_t<T>::read_sgraph;
    using pgraph_t<T>::file_open_sgraph;
    using pgraph_t<T>::prep_sgraph_internal;
    using pgraph_t<T>::store_sgraph;

 public:
    static cfinfo_t* create_instance();
    
    void prep_graph_baseline(egraph_t egraph_type=eADJ);
    void make_graph_baseline();
    void compress_graph_baseline();
    void store_graph_baseline(bool clean = false);
    void read_graph_baseline();
    void file_open(const string& odir,  bool trunc);
    
    //status_t transform(srset_t* iset, srset_t* oset, direction_t direction);
    //virtual status_t extend(srset_t* iset, srset_t* oset, direction_t direction);
};

template <class T>
class unigraph: public pgraph_t<T> {
 public:
    using pgraph_t<T>::sgraph_in;
    using pgraph_t<T>::sgraph_out;
    using pgraph_t<T>::flag1;
    using pgraph_t<T>::flag2;
    using pgraph_t<T>::flag1_count;
    using pgraph_t<T>::flag2_count;
    using pgraph_t<T>::blog;
    
    using pgraph_t<T>::prep_sgraph;
    using pgraph_t<T>::read_sgraph;
    using pgraph_t<T>::file_open_sgraph;
    using pgraph_t<T>::prep_sgraph_internal;
    using pgraph_t<T>::store_sgraph;

 public:
    static cfinfo_t* create_instance();
    
    void prep_graph_baseline(egraph_t egraph_type=eADJ);
    void make_graph_baseline();
    void compress_graph_baseline();
    void store_graph_baseline(bool clean = false);
    void read_graph_baseline();
    void file_open(const string& odir,  bool trunc);
    
    //status_t transform(srset_t* iset, srset_t* oset, direction_t direction);
    //virtual status_t extend(srset_t* iset, srset_t* oset, direction_t direction);
};


typedef ugraph<dst_id_t> ugraph_t;
typedef dgraph<dst_id_t> dgraph_t;
typedef unigraph<dst_id_t> unigraph_t;

typedef ugraph<lite_edge_t> p_ugraph_t;
typedef dgraph<lite_edge_t> p_dgraph_t;
typedef unigraph<lite_edge_t> p_unigraph_t;

/*****************************/
#ifdef BULK
//prefix sum, allocate adj list memory then reset the count
template <class T>
void pgraph_t<T>::prep_sgraph_internal(onegraph_t<T>** sgraph)
{
    tid_t  t_count   = g->get_total_types();
    
    vid_t  v_count   = 0;
    vid_t  portion   = 0;
    vid_t  vid_start = 0;
    vid_t  vid_end   = 0;
    vid_t  total_thds  = omp_get_num_threads();
    vid_t         tid  = omp_get_thread_num();  
    
    for(tid_t i = 0; i < t_count; i++) {
        if (0 == sgraph[i]) continue;
        //sgraph[i]->setup_adjlist();
        
        v_count = sgraph[i]->get_vcount();
        portion = v_count/total_thds;
        vid_start = portion*tid;
        vid_end   = portion*(tid + 1);
        if (tid == total_thds - 1) {
            vid_end = v_count;
        }

        sgraph[i]->setup_adjlist_noatomic(vid_start, vid_end);
        #pragma omp barrier
    }
}
#endif


template <class T>
void pgraph_t<T>::file_open_edge(const string& dir, bool trunc)
{
    string filename = dir + col_info[0]->p_name; 
    string wtfile = filename + ".elog";
    if (trunc) {
        wtf = open(wtfile.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
    } else {
        wtf = open(wtfile.c_str(), O_RDWR|O_CREAT, S_IRWXU);
    }
    
    string etfile = filename + ".str";
    this->mem.file_open(etfile.c_str(), trunc);
}



#ifdef BULK
//estimate edge count
template <class T>
void pgraph_t<T>::calc_edge_count(onegraph_t<T>** sgraph_out, onegraph_t<T>** sgraph_in) 
{
    sid_t     src, dst;
    vid_t     vert1_id, vert2_id;
    tid_t     src_index, dst_index;
    edgeT_t<T>* edges = blog->blog_beg;
    index_t index = 0;
   
    #pragma omp for
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        src = edges[index].src_id;
        dst = get_sid(edges[index].dst_id);
        
        src_index = TO_TID(src);
        dst_index = TO_TID(dst);
        vert1_id = TO_VID(src);
        vert2_id = TO_VID(dst);

        if (!IS_DEL(src)) { 
            sgraph_out[src_index]->increment_count(vert1_id);
            sgraph_in[dst_index]->increment_count(vert2_id);
        } else { 
            sgraph_out[src_index]->decrement_count(vert1_id);
            sgraph_in[dst_index]->decrement_count(vert2_id);
        }
    }
}

//estimate edge count
template <class T>
void pgraph_t<T>::calc_edge_count_out(onegraph_t<T>** sgraph_out)
{
    sid_t     src;
    vid_t     vert1_id;
    tid_t     src_index;
    edgeT_t<T>* edges = blog->blog_beg;
    
    index_t index = 0;
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        src = edges[index].src_id;
        src_index = TO_TID(src);
        vert1_id = TO_VID(src);
        if (!IS_DEL(src)) {
            sgraph_out[src_index]->increment_count(vert1_id);
        } else {
            sgraph_out[src_index]->decrement_count(vert1_id);
        }
    }
}
//estimate edge count
template <class T>
void pgraph_t<T>::calc_edge_count_in(onegraph_t<T>** sgraph_in)
{
    sid_t     src, dst;
    vid_t     vert2_id;
    tid_t     dst_index;
    edgeT_t<T>* edges = blog->blog_beg;
    
    index_t index = 0;
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        src = edges[index].src_id;
        dst = get_sid(edges[index].dst_id);
        dst_index = TO_TID(dst);
        vert2_id = TO_VID(dst);
        if (!IS_DEL(src)) {
            sgraph_in[dst_index]->increment_count(vert2_id);
        } else {
            sgraph_in[dst_index]->decrement_count(vert2_id);
        }
    }
}
template <class T>
void pgraph_t<T>::fill_adj_list(onegraph_t<T>** sgraph_out, onegraph_t<T>** sgraph_in)
{
    sid_t     src, dst2; 
    T         src2, dst;
    vid_t     vert1_id, vert2_id;
    tid_t     src_index, dst_index;
    
    edgeT_t<T>*   edges = blog->blog_beg;

    index_t index = 0;
    #pragma omp for
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        src = edges[index].src_id;
        dst = edges[index].dst_id;
        
        src_index = TO_TID(src);
        vert1_id = TO_VID(src);
        sgraph_out[src_index]->add_nebr(vert1_id, dst);
        
        dst2 = get_sid(dst);
        set_sid(src2, src);
        set_weight(src2, dst);

        dst_index = TO_TID(dst2);
        vert2_id = TO_VID(dst2);
        sgraph_in[dst_index]->add_nebr(vert2_id, src2);
    }
}

template <class T>
void pgraph_t<T>::fill_adj_list_in(onekv_t<T>** skv_out, onegraph_t<T>** sgraph_in) 
{
    sid_t src, dst2;
    T     src2, dst;
    vid_t     vert1_id, vert2_id;
    tid_t     src_index, dst_index;
    edgeT_t<T>*   edges = blog->blog_beg;
    
    index_t index = 0;
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        src = edges[index].src_id;
        dst = edges[index].dst_id;
        
        src_index = TO_TID(src);
        vert1_id = TO_VID(src);
        skv_out[src_index]->set_value(vert1_id, dst);
        
        dst2 = get_sid(dst);
        set_sid(src2, src);
        set_weight(src2, dst);
        
        dst_index = TO_TID(dst2);
        vert2_id = TO_VID(dst2);
        sgraph_in[dst_index]->add_nebr(vert2_id, src2);
    }
}

template <class T>
void pgraph_t<T>::fill_adj_list_out(onegraph_t<T>** sgraph_out, onekv_t<T>** skv_in) 
{
    sid_t   src, dst2;
    T       src2, dst;
    vid_t   vert1_id, vert2_id;
    tid_t   src_index, dst_index; 
    edgeT_t<T>*   edges = blog->blog_beg;
    
    index_t index = 0;
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        src = edges[index].src_id;
        dst = edges[index].dst_id;
        
        src_index = TO_TID(src);
        vert1_id = TO_VID(src);
        sgraph_out[src_index]->add_nebr(vert1_id, dst);
        
        dst2 = get_sid(dst);
        set_sid(src2, src);
        set_weight(src2, dst);
        
        dst_index = TO_TID(dst2);
        vert2_id = TO_VID(dst2);
        skv_in[dst_index]->set_value(vert2_id, src2); 
    }
}
#endif


/************* Semantic graphs  *****************/
template <class T> 
void dgraph<T>::prep_graph_baseline(egraph_t egraph_type)
{
    this->alloc_edgelog(BLOG_SHIFT);
    flag1_count = __builtin_popcountll(flag1);
    flag2_count = __builtin_popcountll(flag2);

    //super bins memory allocation
    tid_t   t_count = g->get_total_types();
    
    if (0 == sgraph_out) {
        sgraph_out  = (onegraph_t<T>**) calloc (sizeof(onegraph_t<T>*), t_count);
    }
    prep_sgraph(flag1, sgraph_out, egraph_type);    
    
    if (0 == sgraph_in) {
        sgraph_in  = (onegraph_t<T>**) calloc (sizeof(onegraph_t<T>*), t_count);
    }
    prep_sgraph(flag2, sgraph_in, egraph_type);
}

//We assume that no new vertex type is defined
template <class T> 
void dgraph<T>::make_graph_baseline()
{
    this->make_graph_d();
}

template <class T> 
void dgraph<T>::compress_graph_baseline()
{
    #pragma omp parallel num_threads(THD_COUNT)
    {
    this->compress_sgraph(sgraph_out);
    this->compress_sgraph(sgraph_in);
    }
}

template <class T> 
void dgraph<T>::store_graph_baseline(bool clean)
{
    //#pragma omp parallel num_threads(THD_COUNT)
    {
    store_sgraph(sgraph_out, clean);
    store_sgraph(sgraph_in, clean);
    this->write_snapshot();
    }
}

template <class T> 
void dgraph<T>::file_open(const string& odir, bool trunc)
{
    this->snapfile =  odir + this->col_info[0]->p_name + ".snap";
    if (trunc) {
        this->snap_f = fopen(this->snapfile.c_str(), "wb");//write + binary
    } else {
        this->snap_f = fopen(this->snapfile.c_str(), "r+b");
    }
    
    assert(this->snap_f != 0);
    this->file_open_edge(odir, trunc);
    string postfix = "out";
    file_open_sgraph(sgraph_out, odir, postfix, trunc);
    postfix = "in";
    file_open_sgraph(sgraph_in, odir, postfix, trunc);
}

template <class T> 
void dgraph<T>::read_graph_baseline()
{
    read_sgraph(sgraph_out);
    read_sgraph(sgraph_in);
    this->mem.handle_read();
    this->read_snapshot();
    snapshot_t* last = this->get_snapshot();
    if (last) {
        blog->readfrom_snapshot(this->get_snapshot());
        last->drop_ref();
    }
}

/*******************************************/
template <class T> 
void ugraph<T>::prep_graph_baseline(egraph_t egraph_type)
{
    this->alloc_edgelog(BLOG_SHIFT);
    flag1 = flag1 | flag2;
    flag2 = flag1;

    flag1_count = __builtin_popcountll(flag1);
    flag2_count = flag1_count;

    //super bins memory allocation
    tid_t   t_count = g->get_total_types();
    
    if (0 == sgraph) {
        sgraph  = (onegraph_t<T>**) calloc (sizeof(onegraph_t<T>*), t_count);
    }
    prep_sgraph(flag1, sgraph, egraph_type);
    this->sgraph_in = sgraph; 
}

template <class T> 
void ugraph<T>::make_graph_baseline()
{
    this->make_graph_u();
}

template <class T> 
void ugraph<T>::compress_graph_baseline()
{
    #pragma omp parallel num_threads(THD_COUNT)
    {
    this->compress_sgraph(sgraph);
    }
}

template <class T> 
void ugraph<T>::store_graph_baseline(bool clean)
{
    double start, end;
    start = mywtime(); 
    store_sgraph(sgraph, clean);
    this->write_snapshot();
    end = mywtime();
    cout << "store graph time = " << end - start << endl;
}

template <class T> 
void ugraph<T>::file_open(const string& odir, bool trunc)
{
    this->snapfile =  odir + this->col_info[0]->p_name + ".snap";
    if (trunc) {
        this->snap_f = fopen(this->snapfile.c_str(), "wb");//write + binary
    } else {
        this->snap_f = fopen(this->snapfile.c_str(), "r+b");
    }
    
    assert(this->snap_f != 0);
    
    this->file_open_edge(odir, trunc);
    string postfix = "";
    file_open_sgraph(sgraph, odir, postfix, trunc);
}

template <class T> 
void ugraph<T>::read_graph_baseline()
{
    read_sgraph(sgraph);
    this->mem.handle_read();
    this->read_snapshot();
    snapshot_t* last = this->get_snapshot();
    if (last) {
        blog->readfrom_snapshot(this->get_snapshot());
        last->drop_ref();
    }
}

/***********/
template <class T> 
void unigraph<T>::prep_graph_baseline(egraph_t egraph_type)
{
    this->alloc_edgelog(BLOG_SHIFT);
    flag1_count = __builtin_popcountll(flag1);
    flag2_count = __builtin_popcountll(flag2);

    //super bins memory allocation
    tid_t   t_count = g->get_total_types();
    
    if (0 == sgraph_out) {
        sgraph_out  = (onegraph_t<T>**) calloc (sizeof(onegraph_t<T>*), t_count);
    }
    prep_sgraph(flag1, sgraph_out, egraph_type);
}

//We assume that no new vertex type is defined
template <class T> 
void unigraph<T>::make_graph_baseline()
{
    this->make_graph_uni();
}

template <class T> 
void unigraph<T>::compress_graph_baseline()
{
    #pragma omp parallel num_threads(THD_COUNT)
    {
    this->compress_sgraph(sgraph_out);
    }
}

template <class T> 
void unigraph<T>::store_graph_baseline(bool clean)
{
    //#pragma omp parallel num_threads(THD_COUNT)
    {
    store_sgraph(sgraph_out, clean);
    this->write_snapshot();
    }
}

template <class T> 
void unigraph<T>::file_open(const string& odir, bool trunc)
{
    this->snapfile =  odir + this->col_info[0]->p_name + ".snap";
    if (trunc) {
        this->snap_f = fopen(this->snapfile.c_str(), "wb");//write + binary
    } else {
        this->snap_f = fopen(this->snapfile.c_str(), "r+b");
    }
    
    assert(this->snap_f != 0);
    this->file_open_edge(odir, trunc);
    string postfix = "out";
    file_open_sgraph(sgraph_out, odir, postfix, trunc);
}

template <class T> 
void unigraph<T>::read_graph_baseline()
{
    read_sgraph(sgraph_out);
    this->mem.handle_read();
    this->read_snapshot();
    snapshot_t* last = this->get_snapshot();
    if (last) {
        blog->readfrom_snapshot(this->get_snapshot());
        last->drop_ref();
    }
}


/*
template <class T>
status_t ugraph<T>::extend(srset_t* iset, srset_t* oset, direction_t direction)
{
    return extend_adjlist_td(sgraph, iset, oset);
}

////////
template <class T>
status_t ugraph<T>::transform(srset_t* iset, srset_t* oset, direction_t direction)
{
    //prepare the output 1,2;
    oset->full_setup(sgraph);
    int total_count = 0;
    
    if (iset->get_total_vcount() <= bu_factor*total_count) { //top down approach
        return query_adjlist_td(sgraph, iset, oset);
    } else { //bottom up approach
        return query_adjlist_bu(sgraph, iset, oset);
    }
    return eOK;
}

template <class T>
status_t dgraph<T>::transform(srset_t* iset, srset_t* oset, direction_t direction)
{
    int total_count = 0;
    if (direction == eout) {
        total_count = 0;
        oset->full_setup(sgraph_in);
        if (iset->get_total_vcount() <= bu_factor*total_count) { //top down approach
            return query_adjlist_td(sgraph_out, iset, oset);
        } else { //bottom up approach
            return query_adjlist_bu(sgraph_in, iset, oset);
        }
    } else {
        assert(direction == ein);
        total_count = 0;
        oset->full_setup(sgraph_out);
        if (iset->get_total_vcount() <= bu_factor*total_count) { //top down approach
            return query_adjlist_td(sgraph_in, iset, oset);
        } else { //bottom up approach 
            return query_adjlist_bu(sgraph_out, iset, oset);
        }
    }
    return eOK;
}

template <class T>
void pgraph_t<T>::update_count(onegraph_t<T>** sgraph)
{
    tid_t       t_count = g->get_total_types();
    
    for(tid_t i = 0; i < t_count; i++) {
        if (0 == sgraph[i]) continue;
        sgraph[i]->update_count();
    }

}
*/
/////////// QUERIES ///////////////////////////
/*
status_t pgraph_t::query_adjlist_td(sgraph_t** sgraph, srset_t* iset, srset_t* oset)
{
    tid_t    iset_count = iset->get_rset_count();
    rset_t*        rset = 0;

    for (tid_t i = 0; i < iset_count; ++i) {
        rset = iset->rset + i;
        vid_t v_count = rset->get_vcount();
        vid_t* vlist = rset->get_vlist();
        
        //get the graph where we will traverse
        tid_t        tid = rset->get_tid();
        if (0 == sgraph[tid]) continue;
        beg_pos_t* graph = sgraph[tid]->get_begpos();

        
        //Get the frontiers
        vid_t     frontier;
        for (vid_t v = 0; v < v_count; v++) {
            frontier = vlist[v];
            sid_t* adj_list = graph[frontier].get_adjlist();
            vid_t nebr_count = adj_list[0];
            ++adj_list;
            
            //traverse the adj list
            for (vid_t k = 0; k < nebr_count; ++k) {
                oset->set_status(adj_list[k]);
            }
        }
    }
    return eOK;
}
status_t pgraph_t::query_kv_td(skv_t** skv, srset_t* iset, srset_t* oset)
{
    tid_t    iset_count = iset->get_rset_count();
    rset_t*        rset = 0;

    for (tid_t i = 0; i < iset_count; ++i) {
        rset = iset->rset + i;
        vid_t v_count = rset->get_vcount();
        vid_t* vlist = rset->get_vlist();
        
        //get the graph where we will traverse
        tid_t        tid = rset->get_tid();
        if (0 == skv[tid]) continue;
        sid_t* kv = skv[tid]->get_kv(); 

        //Get the frontiers
        vid_t     frontier;
        for (vid_t v = 0; v < v_count; v++) {
            frontier = vlist[v];
            oset->set_status(kv[frontier]);
        }
    }
    return eOK;
}

//sgraph_in and oset share the same flag.
status_t pgraph_t::query_adjlist_bu(sgraph_t** sgraph, srset_t* iset, srset_t* oset)
{
    rset_t* rset = 0;
    tid_t   tid  = 0;
    tid_t oset_count = oset->get_rset_count();

    for (tid_t i = 0; i < oset_count; ++i) {
        
        //get the graph where we will traverse
        rset = oset->rset + i;
        tid  = rset->get_tid();
        if (0 == sgraph[tid]) continue; 

        beg_pos_t* graph = sgraph[tid]->get_begpos(); 
        vid_t    v_count = sgraph[tid]->get_vcount();
        
        
        for (vid_t v = 0; v < v_count; v++) {
            //traverse the adj list
            sid_t* adj_list = graph[v].get_adjlist();
            vid_t nebr_count = adj_list[0];
            ++adj_list;
            for (vid_t k = 0; k < nebr_count; ++k) {
                if (iset->get_status(adj_list[k])) {
                    rset->set_status(v);
                    break;
                }
            }
        }
    }
    return eOK;
}

status_t pgraph_t::query_kv_bu(skv_t** skv, srset_t* iset, srset_t* oset) 
{
    rset_t*  rset = 0;
    tid_t    tid  = 0;
    tid_t    oset_count = oset->get_rset_count();
    for (tid_t i = 0; i < oset_count; ++i) {

        //get the graph where we will traverse
        rset = oset->rset + i;
        tid  = rset->get_tid(); 
        if (0 == skv[tid]) continue;

        vid_t*       kv = skv[tid]->get_kv(); 
        sid_t   v_count = skv[tid]->get_vcount();
        
        for (vid_t v = 0; v < v_count; ++v) {
            if (iset->get_status(kv[v])) {
                rset->set_status(v);
                break;
            }
        }
    }
    return eOK;
}

//////extend functions ------------------------
status_t 
pgraph_t::extend_adjlist_td(sgraph_t** sgraph, srset_t* iset, srset_t* oset)
{
    tid_t    iset_count = iset->get_rset_count();
    rset_t*        rset = 0;
    rset_t*        rset2 = 0;

    iset->bitwise2vlist();
    //prepare the output 1,2;
    oset->copy_setup(iset, eAdjlist);

    for (tid_t i = 0; i < iset_count; ++i) {
        rset = iset->rset + i;
        rset2 = oset->rset + i;
        vid_t v_count = rset->get_vcount();
        sid_t* varray = rset->get_vlist();
        
        //get the graph where we will traverse
        tid_t        tid = rset->get_tid();
        if (0 == sgraph[tid]) continue;
        beg_pos_t* graph = sgraph[tid]->get_begpos(); 
        
        for (vid_t v = 0; v < v_count; v++) {
            rset2->add_adjlist_ro(v, graph+varray[v]);
        }
    }
    return eOK;
}

status_t 
pgraph_t::extend_kv_td(skv_t** skv, srset_t* iset, srset_t* oset)
{
    tid_t    iset_count = iset->get_rset_count();
    rset_t*        rset = 0;
    rset_t*       rset2 = 0;

    iset->bitwise2vlist();
    //prepare the output 1,2;
    oset->copy_setup(iset, eKV);

    for (tid_t i = 0; i < iset_count; ++i) {
        rset = iset->rset + i;
        rset2 = oset->rset + i;
        vid_t v_count = rset->get_vcount();
        sid_t* varray = rset->get_vlist();
        
        //get the graph where we will traverse
        tid_t     tid = rset->get_tid();
        if (0 == skv[tid]) continue;
        sid_t*  graph = skv[tid]->get_kv(); 
        
        for (vid_t v = 0; v < v_count; v++) {
            rset2->add_kv(v, graph[varray[v]]);
        }
    }
    return eOK;
}
*/
/**********************************************************/

/*
template <class T>
status_t pgraph_t<T>::query_adjlist_td(onegraph_t<T>** sgraph, srset_t* iset, srset_t* oset)
{
    tid_t    iset_count = iset->get_rset_count();
    rset_t*        rset = 0;

    for (tid_t i = 0; i < iset_count; ++i) {
        rset = iset->rset + i;
        vid_t v_count = rset->get_vcount();
        vid_t* vlist = rset->get_vlist();
        
        //get the graph where we will traverse
        tid_t        tid = rset->get_tid();
        if (0 == sgraph[tid]) continue;
        vert_table_t<T>* graph = sgraph[tid]->get_begpos();

        
        //Get the frontiers
        vid_t     frontier;
        for (vid_t v = 0; v < v_count; v++) {
            frontier = vlist[v];
            T* adj_list = graph[frontier].get_adjlist();
            vid_t nebr_count = get_nebrcount1(adj_list);
            ++adj_list;
            
            //traverse the adj list
            for (vid_t k = 0; k < nebr_count; ++k) {
                oset->set_status(get_nebr(adj_list, k));
            }
        }
    }
    return eOK;
}

template <class T>
status_t pgraph_t<T>::query_kv_td(onekv_t<T>** skv, srset_t* iset, srset_t* oset)
{
    tid_t    iset_count = iset->get_rset_count();
    rset_t*        rset = 0;

    for (tid_t i = 0; i < iset_count; ++i) {
        rset = iset->rset + i;
        vid_t v_count = rset->get_vcount();
        vid_t* vlist = rset->get_vlist();
        
        //get the graph where we will traverse
        tid_t        tid = rset->get_tid();
        if (0 == skv[tid]) continue;
        T* kv = skv[tid]->get_kv(); 

        //Get the frontiers
        vid_t     frontier;
        for (vid_t v = 0; v < v_count; v++) {
            frontier = vlist[v];
            oset->set_status(get_nebr(kv, frontier));
        }
    }
    return eOK;
}
//sgraph_in and oset share the same flag.
template <class T>
status_t pgraph_t<T>::query_adjlist_bu(onegraph_t<T>** sgraph, srset_t* iset, srset_t* oset)
{
    rset_t* rset = 0;
    tid_t   tid  = 0;
    tid_t oset_count = oset->get_rset_count();

    for (tid_t i = 0; i < oset_count; ++i) {
        
        //get the graph where we will traverse
        rset = oset->rset + i;
        tid  = rset->get_tid();
        if (0 == sgraph[tid]) continue; 

        vert_table_t<T>* graph = sgraph[tid]->get_begpos(); 
        vid_t    v_count = sgraph[tid]->get_vcount();
        
        
        for (vid_t v = 0; v < v_count; v++) {
            //traverse the adj list
            T* adj_list = graph[v].get_adjlist();
            vid_t nebr_count = get_nebrcount1(adj_list);
            ++adj_list;
            for (vid_t k = 0; k < nebr_count; ++k) {
                if (iset->get_status(get_nebr(adj_list, k))) {
                    rset->set_status(v);
                    break;
                }
            }
        }
    }
    return eOK;
}

template <class T>
status_t pgraph_t<T>::query_kv_bu(onekv_t<T>** skv, srset_t* iset, srset_t* oset) 
{
    rset_t*  rset = 0;
    tid_t    tid  = 0;
    tid_t    oset_count = oset->get_rset_count();
    for (tid_t i = 0; i < oset_count; ++i) {

        //get the graph where we will traverse
        rset = oset->rset + i;
        tid  = rset->get_tid(); 
        if (0 == skv[tid]) continue;

        T* kv = skv[tid]->get_kv(); 
        sid_t   v_count = skv[tid]->get_vcount();
        
        for (vid_t v = 0; v < v_count; ++v) {
            if (iset->get_status(get_nebr(kv, v))) {
                rset->set_status(v);
                break;
            }
        }
    }
    return eOK;
}
template <class T> 
status_t pgraph_t<T>::extend_adjlist_td(onegraph_t<T>** sgraph, srset_t* iset, srset_t* oset)
{
    tid_t    iset_count = iset->get_rset_count();
    rset_t*        rset = 0;
    rset_t*        rset2 = 0;

    iset->bitwise2vlist();
    //prepare the output 1,2;
    oset->copy_setup(iset, eAdjlist);

    for (tid_t i = 0; i < iset_count; ++i) {
        rset = iset->rset + i;
        rset2 = oset->rset + i;
        vid_t v_count = rset->get_vcount();
        sid_t* varray = rset->get_vlist();
        
        //get the graph where we will traverse
        tid_t        tid = rset->get_tid();
        if (0 == sgraph[tid]) continue;
        vert_table_t<T>* graph = sgraph[tid]->get_begpos(); 
        
        for (vid_t v = 0; v < v_count; v++) {
            rset2->add_adjlist_ro(v, graph+varray[v]);
        }
    }
    return eOK;
}

template <class T>
status_t pgraph_t<T>::extend_kv_td(onekv_t<T>** skv, srset_t* iset, srset_t* oset)
{
    tid_t    iset_count = iset->get_rset_count();
    rset_t*        rset = 0;
    rset_t*       rset2 = 0;

    iset->bitwise2vlist();
    //prepare the output 1,2;
    oset->copy_setup(iset, eKV);

    for (tid_t i = 0; i < iset_count; ++i) {
        rset = iset->rset + i;
        rset2 = oset->rset + i;
        vid_t v_count = rset->get_vcount();
        sid_t* varray = rset->get_vlist();
        
        //get the graph where we will traverse
        tid_t     tid = rset->get_tid();
        if (0 == skv[tid]) continue;
        T*  graph = skv[tid]->get_kv(); 
        
        for (vid_t v = 0; v < v_count; v++) {
            rset2->add_kv(v, get_nebr(graph, varray[v]));
        }
    }
    return eOK;
}
*/
