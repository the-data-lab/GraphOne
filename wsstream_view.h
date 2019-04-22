#pragma once

template <class T>
class wsstream_t {
 public:
    snapshot_t*      snapshot;
    pgraph_t<T>*     pgraph;
    void*            algo_meta;//algorithm specific data
    typename callback<T>::wsfunc   wsstream_func;
    
    //The adjacency data
    vert_table_t<T>* graph_out;
    vert_table_t<T>* graph_in;
    
    //ending marker of the window
    degree_t*        degree_out;
    degree_t*        degree_in;
    
    //starting marker of the window
    degree_t*        degree_out1;
    degree_t*        degree_in1;
    
    //last few edges of above data-structure, 0 for stale
    edgeT_t<T>*      edges; //new edges
    index_t          edge_count;//their count
    
    edgeT_t<T>*      edges1; //old edges
    index_t          edge_count1; //their count
    
    index_t          window_sz;
    vid_t            v_count; 
    int              flag;
 public:
    inline wsstream_t() {
        degree_out = 0;
        degree_in = 0;
        degree_out1 = 0;
        degree_in1 = 0;
        snapshot = 0;
        edges = 0;
        edges1=0;
        edge_count = 0;
        edge_count1 = 0;
        algo_meta = 0;
        pgraph = 0;
    }
    inline ~wsstream_t() {
    }
 public:
    inline void    set_algometa(void* a_meta) {algo_meta = a_meta;}
    inline void*   get_algometa() {return algo_meta;}
    
    degree_t get_nebrs_out(vid_t vid, T*& ptr);
    degree_t get_nebrs_in (vid_t vid, T*& ptr);
    degree_t get_degree_out(vid_t vid);
    degree_t get_degree_in (vid_t vid);
    
    //Don't provide this
    delta_adjlist_t<T>* get_nebrs_archived_out(vid_t);
    delta_adjlist_t<T>* get_nebrs_archived_in(vid_t v);
    
    void init_wsstream_view(pgraph_t<T>* pgraph, index_t window_sz, bool simple, bool priv, bool stale);
    void create_view(pgraph_t<T>* pgraph, bool simple, bool priv, bool stale);
    status_t update_view();
    void create_degreesnap();
    void create_degreesnapd();
    
    void update_degreesnap();
    void update_degreesnapd();
};

template <class T>
void wsstream_t<T>::init_wsstream_view(pgraph_t<T>* ugraph, index_t window_sz1, bool simple, bool priv, bool stale)
{
    snapshot = 0;
    edges = 0;
    edge_count = 0;
    v_count = g->get_type_scount();
    pgraph  = ugraph;
    
    onegraph_t<T>* sgraph_out = ugraph->sgraph_out[0];
    graph_out  = sgraph_out->get_begpos();
    degree_out = (degree_t*) calloc(v_count, sizeof(degree_t));
    degree_out1 = (degree_t*) calloc(v_count, sizeof(degree_t));
    
    if (ugraph->sgraph_in == ugraph->sgraph_out) {
        graph_in   = graph_out;
        degree_in  = degree_out;
        degree_in1 = degree_out1;
    } else if (ugraph->sgraph_in != 0) {
        graph_in   = ugraph->sgraph_in[0]->get_begpos();
        degree_in  = (degree_t*) calloc(v_count, sizeof(degree_t));
        degree_in1 = (degree_t*) calloc(v_count, sizeof(degree_t));
    }
    
    window_sz = window_sz1;
    if (stale) {
        SET_STALE(flag);
    }
    if (priv) {
        SET_PRIVATE(flag);
    }
    if (simple) {
        SET_SIMPLE(flag);
    }
}

template <class T>
status_t wsstream_t<T>::update_view()
{
    blog_t<T>* blog = pgraph->blog;
    index_t  marker = blog->blog_head;
    snapshot_t* new_snapshot = pgraph->get_snapshot();
    
    if (new_snapshot == 0|| (new_snapshot == snapshot)) return eNoWork;
    
    index_t new_marker   = new_snapshot->marker;
    
    //for stale
    edges = blog->blog_beg + (new_marker & blog->blog_mask);
    edge_count = marker - new_marker;
    
    if (pgraph->sgraph_in == 0) {
        update_degreesnap();
    } else {
        update_degreesnapd();
    }

    snapshot = new_snapshot;
    return eOK;
}
