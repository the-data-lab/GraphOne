#pragma once

template <class T>
struct wsstream_t : public sstream_t<T> {
    using sstream_t<T>::pgraph;
    using sstream_t<T>::sstream_func;
    using sstream_t<T>::thread;
    using sstream_t<T>::algo_meta;
    using sstream_t<T>::v_count;
    using sstream_t<T>::flag;
    using sstream_t<T>::graph_out;
    using sstream_t<T>::graph_in;
    using sstream_t<T>::bitmap_out;
    using sstream_t<T>::bitmap_in;
    using sstream_t<T>::reader;
    using sstream_t<T>::reg_id;

 protected: 
    //end  of the window
    using sstream_t<T>::snapshot;
    using sstream_t<T>::prev_snapshot;
    degree_t*        wdegree_out;
    degree_t*        wdegree_in;
    
    //beginning of the the window
    using sstream_t<T>::degree_out;
    using sstream_t<T>::degree_in;
    edgeT_t<T>*       start_edges;
    index_t           start_count;
    index_t           start_marker;
    
    //handling stale, simple private etc.
    using sstream_t<T>::edges;
    using sstream_t<T>::edge_count;
    
    //For edge-centric
    edgeT_t<T>*      new_edges;//New edges
    index_t          new_edge_count;//Their count
    

 public:
    index_t          window_sz;

    inline wsstream_t() {
        start_marker = 0;
        wdegree_out = 0;
        wdegree_in = 0;
    }
    inline ~wsstream_t() {
        if (wdegree_in == wdegree_out && wdegree_out != NULL) {
            free(wdegree_out);
        } else if (wdegree_in != wdegree_out) {
            free(wdegree_out);
            free(wdegree_in);
        }
    }
    degree_t get_nebrs_out(vid_t vid, T* ptr);
    degree_t get_nebrs_in (vid_t vid, T* ptr);
    degree_t get_degree_out(vid_t vid);
    degree_t get_degree_in (vid_t vid);
    
    void init_view(pgraph_t<T>* pgraph, index_t window_sz, index_t flag);
    status_t update_view();
    
    virtual index_t  get_compaction_marker() {
        return start_marker;
    }
    
 private:
    void update_degreesnap();
    void update_degreesnapd();
    void handle_flagu();
    void handle_flagd();
    void handle_flaguni();
};

template <class T>
void wsstream_t<T>::init_view(pgraph_t<T>* pgraph, index_t window_sz1, 
                                       index_t a_flag)
{
    sstream_t<T>::init_view(pgraph, a_flag);
    start_marker = 0;
    if (pgraph->blog) {
        start_marker = pgraph->blog->blog_head;
    }
    window_sz =  window_sz1;
    wdegree_out = (sdegree_t*) calloc(v_count, sizeof(sdegree_t));
    if (graph_in == graph_out) {
        //u
        wdegree_in = wdegree_out;
        
    } else if (graph_in !=0) {
        //d
        wdegree_in = (sdegree_t*) calloc(v_count, sizeof(sdegree_t));
    }
}

template <class T>
status_t wsstream_t<T>::update_view()
{
    snapshot = pgraph->get_snapshot();
    
    if (snapshot == 0|| (snapshot == prev_snapshot)) return eNoWork;
    
    blog_t<T>* blog = pgraph->blog;
    index_t  marker = blog->blog_head;
    index_t old_marker = prev_snapshot? prev_snapshot->marker: 0;
    
    if(marker - start_marker < window_sz) {
            return eNoWork;
    }
    
    index_t snap_marker = snapshot->marker;
    
    //read the older edges
    if (IS_STALE(flag)) {
        start_count = snap_marker - old_marker;
    } else {
       start_count = (marker - old_marker);
    }
    if (0 == start_marker) {
        start_count -= window_sz;
    }
    index_t end_marker = start_marker + start_count;
    start_edges = (edgeT_t<T>*)realloc(start_edges, start_count*sizeof(edgeT_t<T>));
    pgraph->get_prior_edges(start_marker, end_marker, start_edges);

    //for stale
    if (IS_PRIVATE(flag)) {
        edge_count = marker - snap_marker;
        edges = (edgeT_t<T>*)realloc(edges, edge_count*sizeof(edgeT_t<T>));
        
        //copy the edges
        reader.marker = marker;
        reader.tail = snap_marker;
        for (index_t i = reader.tail; i < reader.marker; ++i) {
            read_edge(reader.blog, i, edges[i-reader.tail]);
        }
        reader.marker = -1L;
        reader.tail = -1L;
    } else if (!IS_STALE(flag)) {
        reader.marker = marker;
        reader.tail = snap_marker;
    }
    
    #pragma omp parallel
    {
    if (graph_in == graph_out || graph_in == 0) {
        update_degreesnap();
    } else {
        update_degreesnapd();
    }
    }

    if (prev_snapshot) {
        prev_snapshot->drop_ref();
    }
    
    prev_snapshot = snapshot;
    start_marker += start_count;
    
    //wait for archiving to complete
    if (IS_SIMPLE(flag)) {
        pgraph->waitfor_archive(reader.marker);
    }
    
    //Get the edge copies for edge centric computation XXX
    if (IS_E_CENTRIC(flag)) { 
        assert(0);//FIX ME
        new_edge_count = snap_marker - old_marker;
        new_edges = (edgeT_t<T>*)realloc (new_edges, new_edge_count*sizeof(edgeT_t<T>));
        memcpy(new_edges, blog->blog_beg + (old_marker & blog->blog_mask), new_edge_count*sizeof(edgeT_t<T>));
    }
    
    return eOK;
}

template <class T>
void wsstream_t<T>::update_degreesnap()
{
    snapid_t    snap_id = snapshot->snap_id;
    snapid_t    prev_snapid = this->get_prev_snapid();
    snapid_t    snap_id1 = 0;
    degree_t    nebr_count = 0;
    degree_t    evict_count = 0;; 
    #pragma omp for 
    for (vid_t v = 0; v < v_count; ++v) {
        nebr_count = graph_out->get_degree(v, snap_id);
        snap_id1 = graph_out->get_eviction(v, evict_count);
        if (wdegree_out[v] != nebr_count) {
            wdegree_out[v] = nebr_count;
            bitmap_out->set_bit(v);
        } else {
            bitmap_out->reset_bit(v);
        }

        if (prev_snapid < snap_id1 && snap_id >= snap_id1) {
            degree_out[v] -= evict_count; 
        }
    }

    //Update the degree nodes for the starting marker of window
    // 1. fetch the durable degree edges
    // 2. compute the degree
    #pragma omp for
    for (index_t i = 0; i < start_count; ++i) {
        __sync_fetch_and_add(degree_out + start_edges[i].src_id, 1);
        __sync_fetch_and_add(degree_out + TO_VID(get_dst(start_edges + i)), 1);
        bitmap_out->set_bit_atomic(start_edges[i].src_id);
        bitmap_out->set_bit_atomic(TO_VID(get_dst(start_edges + i)));
    }
}

template <class T>
void wsstream_t<T>::update_degreesnapd()
{
    degree_t nebr_count = 0;
    degree_t evict_count = 0;; 
    snapid_t    snap_id = snapshot->snap_id;
    snapid_t    prev_snapid = this->get_prev_snapid();
    snapid_t    snap_id1 = 0;

    #pragma omp for  
    for (vid_t v = 0; v < v_count; ++v) {
        nebr_count = graph_out->get_degree(v, snap_id);
        if (wdegree_out[v] != nebr_count) {
            wdegree_out[v] = nebr_count;
            bitmap_out->set_bit(v);
        } else {
            bitmap_out->reset_bit(v);
        }
        snap_id1 = graph_out->get_eviction(v, evict_count);
        if (prev_snapid < snap_id1 && snap_id >= snap_id1) {
            degree_out[v] -= evict_count; 
        }
        
        nebr_count = graph_in->get_degree(v, snap_id);
        if (wdegree_in[v] != nebr_count) {
            wdegree_in[v] = nebr_count;
            bitmap_in->set_bit(v);
        } else {
            bitmap_in->reset_bit(v);
        }
        snap_id1 = graph_in->get_eviction(v, evict_count);
        if (prev_snapid < snap_id1 && snap_id >= snap_id1) {
            degree_in[v] -= evict_count; 
        }
    }

    //Update the degree nodes for the starting marker of window
    // 1. fetch the durable degree edges
    // 2. compute the degree
    #pragma omp for
    for (index_t i = 0; i < start_count; ++i) {
        __sync_fetch_and_add(degree_out + start_edges[i].src_id, 1);
        __sync_fetch_and_add(degree_in + TO_VID(get_dst(start_edges + i)), 1);
        bitmap_out->set_bit_atomic(start_edges[i].src_id);
        bitmap_in->set_bit_atomic(TO_VID(get_dst(start_edges + i)));
    }

    return;
}

template <class T>
degree_t wsstream_t<T>::get_degree_out(vid_t v)
{
    return wdegree_out[v] - degree_out[v];
}

template <class T>
degree_t wsstream_t<T>::get_degree_in(vid_t v)
{
    return wdegree_in[v] - degree_in[v];
}

template <class T>
degree_t wsstream_t<T>::get_nebrs_out(vid_t v, T* adj_list)
{
    return graph_out->get_wnebrs(v, adj_list, degree_out[v], get_degree_out(v), reg_id);
}

template<class T>
degree_t wsstream_t<T>::get_nebrs_in(vid_t v, T* adj_list)
{
    return graph_in->get_wnebrs(v, adj_list, degree_in[v], get_degree_in(v), reg_id);
}

template <class T>
void wsstream_t<T>::handle_flagu()
{
    sid_t src, dst;
    edgeT_t<T> edge;
    vid_t src_vid, dst_vid;
    bool is_del = false;
    bool is_private = IS_PRIVATE(flag);
    bool is_simple = IS_SIMPLE(flag);
    if (true == is_simple) {
        #pragma omp for
        for (index_t i = 0; i < edge_count; ++i) {
            src_vid = TO_VID(get_src(edges[i]));
            dst_vid = TO_VID(get_dst(edges[i]));
            //set the bitmap
            bitmap_out->set_bit_atomic(src_vid);
            bitmap_out->set_bit_atomic(dst_vid);

            is_del = IS_DEL(get_src(edges[i]));
#ifdef DEL
            if (is_del) {
            __sync_fetch_and_add(&wdegree_out[src_vid].del_count, 1);
            __sync_fetch_and_add(&wdegree_out[dst_vid].del_count, 1);
            } else {
            __sync_fetch_and_add(&wdegree_out[src_vid].add_count, 1);
            __sync_fetch_and_add(&wdegree_out[dst_vid].add_count, 1);
            }
#else
            if (is_del) {assert(0);
            __sync_fetch_and_add(&wdegree_out+src_vid, 1);
            __sync_fetch_and_add(&wdegree_out+dst_vid, 1);
            }
#endif
        }
    } else if (true == is_simple) {
        for (index_t i = reader.tail; i < reader.marker; ++i) {
            read_edge(reader.blog, i, edge);
            src_vid = TO_VID(get_src(edge));
            dst_vid = TO_VID(get_dst(edge));
            //set the bitmap
            bitmap_out->set_bit_atomic(src_vid);
            bitmap_out->set_bit_atomic(dst_vid);

            is_del = IS_DEL(get_src(edge));
#ifdef DEL
            if (is_del) {
            __sync_fetch_and_add(&wdegree_out[src_vid].del_count, 1);
            __sync_fetch_and_add(&wdegree_out[dst_vid].del_count, 1);
            } else {
            __sync_fetch_and_add(&wdegree_out[src_vid].add_count, 1);
            __sync_fetch_and_add(&wdegree_out[dst_vid].add_count, 1);
            }
#else
            if (is_del) {assert(0);
            __sync_fetch_and_add(&wdegree_out+src_vid, 1);
            __sync_fetch_and_add(&wdegree_out+dst_vid, 1);
            }
#endif
        }
    }
}

template <class T>
void wsstream_t<T>::handle_flagd()
{
    vid_t src_vid, dst_vid;
    edgeT_t<T> edge;
    bool is_del = false;
    bool is_private = IS_PRIVATE(flag);
    bool is_simple = IS_SIMPLE(flag);
    if (true == is_simple) {
        #pragma omp for
        for (index_t i = 0; i < edge_count; ++i) {
            src_vid = TO_VID(get_src(edges[i]));
            dst_vid = TO_VID(get_dst(edges[i]));
            bitmap_out->set_bit_atomic(src_vid);
            bitmap_in->set_bit_atomic(dst_vid);
            is_del = IS_DEL(get_src(edges[i]));
#ifdef DEL
            if (is_del) {
            __sync_fetch_and_add(&wdegree_out[src_vid].del_count, 1);
            __sync_fetch_and_add(&wdegree_in[dst_vid].del_count, 1);
            } else {
            __sync_fetch_and_add(&wdegree_out[src_vid].add_count, 1);
            __sync_fetch_and_add(&wdegree_in[dst_vid].add_count, 1);
            }
#else
            if (is_del) {assert(0);
            __sync_fetch_and_add(&wdegree_out+src_vid, 1);
            __sync_fetch_and_add(&wdegree_in+dst_vid, 1);
            }
#endif
        }
    } else if (true == is_simple) {
        for (index_t i = reader.tail; i < reader.marker; ++i) {
            read_edge(reader.blog, i, edge);
            src_vid = TO_VID(get_src(edge));
            dst_vid = TO_VID(get_dst(edge));
            bitmap_out->set_bit_atomic(src_vid);
            bitmap_in->set_bit_atomic(dst_vid);
            is_del = IS_DEL(get_src(edge));
#ifdef DEL
            if (is_del) {
            __sync_fetch_and_add(&wdegree_out[src_vid].del_count, 1);
            __sync_fetch_and_add(&wdegree_in[dst_vid].del_count, 1);
            } else {
            __sync_fetch_and_add(&wdegree_out[src_vid].add_count, 1);
            __sync_fetch_and_add(&wdegree_in[dst_vid].add_count, 1);
            }
#else
            if (is_del) {assert(0);
            __sync_fetch_and_add(&wdegree_out+src_vid, 1);
            __sync_fetch_and_add(&wdegree_in+dst_vid, 1);
            }
#endif
        }
    }
}

template <class T>
void wsstream_t<T>::handle_flaguni()
{
    vid_t src_vid, dst_vid;
    edgeT_t<T> edge;
    bool is_del = false;
    bool is_private = IS_PRIVATE(flag);
    bool is_simple = IS_SIMPLE(flag);
    if (true == is_simple && true == is_private) {
        #pragma omp for
        for (index_t i = 0; i < edge_count; ++i) {
            src_vid = TO_VID(get_src(edges[i]));
            dst_vid = TO_VID(get_dst(edges[i]));
            bitmap_out->set_bit_atomic(src_vid);
            is_del = IS_DEL(get_src(edges[i]));
#ifdef DEL
            if (is_del) {
            __sync_fetch_and_add(&wdegree_out[src_vid].del_count, 1);
            
            } else {
            __sync_fetch_and_add(&wdegree_out[src_vid].del_count, 1);
            }
#else
            if (is_del) {assert(0);
            __sync_fetch_and_add(&wdegree_out+src_vid, 1);
            }
#endif
        }
    } else if (true == is_simple) {
        //
        #pragma omp for
        for (index_t i = reader.tail; i < reader.marker; ++i) {
            read_edge(reader.blog, i, edge);
            src_vid = TO_VID(get_src(edge));
            dst_vid = TO_VID(get_dst(edge));
            bitmap_out->set_bit_atomic(src_vid);
            is_del = IS_DEL(get_src(edge));
#ifdef DEL
            if (is_del) {
            __sync_fetch_and_add(&wdegree_out[src_vid].del_count, 1);
            } else {
            __sync_fetch_and_add(&wdegree_out[src_vid].del_count, 1);
            }
#else
            if (is_del) {assert(0);
            __sync_fetch_and_add(&wdegree_out+src_vid, 1);
            }
#endif
        }
    }
}
