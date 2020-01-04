#pragma once

template <class T>
struct diff_view_t : public sstream_t<T> {
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
    using sstream_t<T>::reader_id;

 protected: 
    //end  of the window
    using sstream_t<T>::snapshot;
    using sstream_t<T>::prev_snapshot;
    degree_t*        wdegree_out;
    degree_t*        wdegree_in;
    
    //beginning of the the window
    using sstream_t<T>::degree_out;
    using sstream_t<T>::degree_in;
    
    //handling stale, simple private etc.
    using sstream_t<T>::edges;
    using sstream_t<T>::edge_count;

 public:   
    inline diff_view_t() {
        wdegree_out = 0;
        wdegree_in = 0;
    }
    inline ~diff_view_t() {
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
    
    inline degree_t get_prior_degree_out(vid_t vid) {
        return get_actual(degree_out[vid]);
    }
    inline degree_t get_prior_degree_in (vid_t vid) {
        return get_actual(degree_in[vid]);
    }
    

    inline degree_t get_diff_degree_out(vid_t v) {
        return get_actual(degree_out[v]) + get_total(wdegree_out[v]) - get_total(degree_out[v]);
    }

    inline degree_t get_diff_degree_in(vid_t v) {
        return get_actual(degree_in[v]) + get_total(wdegree_in[v]) - get_total(degree_in[v]);
    }

    inline degree_t get_diff_nebrs_out(vid_t v, T* adj_list, degree_t& diff_degree) {
        return graph_out->get_diff_nebrs(v, adj_list, degree_out[v], wdegree_out[v], reader_id, diff_degree);
    }

    inline degree_t get_diff_nebrs_in(vid_t v, T* adj_list, degree_t& diff_degree) {
        return graph_in->get_diff_nebrs(v, adj_list, degree_in[v], wdegree_in[v], reader_id, diff_degree);
    }
    
    void init_view(pgraph_t<T>* pgraph, index_t flag);
    status_t update_view();
    
 private:
    void update_degreesnap();
    void update_degreesnapd();
    void handle_flagu();
    void handle_flagd();
    void handle_flaguni();
};

template <class T>
void diff_view_t<T>::init_view(pgraph_t<T>* pgraph, index_t a_flag)
{
    sstream_t<T>::init_view(pgraph, a_flag);
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
status_t diff_view_t<T>::update_view()
{
    snapshot = pgraph->get_snapshot();
    
    if (snapshot == 0|| (snapshot == prev_snapshot)) return eNoWork;
    
    blog_t<T>* blog = pgraph->blog;
    index_t  marker = blog->blog_head;
    index_t snap_marker = snapshot->marker;
    index_t old_marker = prev_snapshot? prev_snapshot->marker: 0;
    
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
    
    #pragma omp parallel num_threads(THD_COUNT)
    {
        if (graph_in == graph_out || graph_in == 0) {
            update_degreesnap();
        } else {
            update_degreesnapd();
        }
        if (graph_in != graph_out && graph_in != 0) {
            handle_flagd();
        } else if (graph_in == graph_out) {
            handle_flagu();
        } else {
            handle_flaguni();
        }
    }

    if (prev_snapshot) {
        prev_snapshot->drop_ref();
    }
    
    prev_snapshot = snapshot;
    
    //wait for archiving to complete
    if (IS_SIMPLE(flag)) {
        pgraph->waitfor_archive(reader.marker);
    }
    
    return eOK;
}

template <class T>
void diff_view_t<T>::update_degreesnap()
{
    snapid_t    snap_id = snapshot->snap_id;
    snapid_t    prev_snapid = this->get_prev_snapid();
    snapid_t    snap_id1 = 0;
    degree_t    nebr_count = 0;
    degree_t    evict_count = 0;; 
    #pragma omp for 
    for (vid_t v = 0; v < v_count; ++v) {
        nebr_count = graph_out->get_degree(v, snap_id);
        degree_out[v] = wdegree_out[v];//XXX swap it
        snap_id1 = graph_out->get_eviction(v, evict_count);
        if (wdegree_out[v] != nebr_count) {
            wdegree_out[v] = nebr_count;
            bitmap_out->set_bit(v);
        } else {
            bitmap_out->reset_bit(v);
        }

        if (prev_snapid < snap_id1 && snap_id >= snap_id1) {
            #ifdef DEL
            degree_out[v].add_count -= evict_count; 
            degree_out[v].del_count -= evict_count; 
            #else
            degree_out[v] -= evict_count; 
            #endif
        }
    }
}

template <class T>
void diff_view_t<T>::update_degreesnapd()
{
    degree_t nebr_count = 0;
    degree_t evict_count = 0;; 
    snapid_t    snap_id = snapshot->snap_id;
    snapid_t    prev_snapid = this->get_prev_snapid();
    snapid_t    snap_id1 = 0;

    #pragma omp for  
    for (vid_t v = 0; v < v_count; ++v) {
        nebr_count = graph_out->get_degree(v, snap_id);
        degree_out[v] = wdegree_out[v];//XXX swap it
        if (wdegree_out[v] != nebr_count) {
            wdegree_out[v] = nebr_count;
            bitmap_out->set_bit(v);
        } else {
            bitmap_out->reset_bit(v);
        }
        snap_id1 = graph_out->get_eviction(v, evict_count);
        if (prev_snapid < snap_id1 && snap_id >= snap_id1) {
            #ifdef DEL
            degree_out[v].add_count -= evict_count;
            degree_out[v].del_count -= evict_count; 
            #else
            degree_out[v] -= evict_count; 
            #endif
        }
        
        nebr_count = graph_in->get_degree(v, snap_id);
        degree_in[v] = wdegree_in[v];
        if (wdegree_in[v] != nebr_count) {
            wdegree_in[v] = nebr_count;
            bitmap_in->set_bit(v);
        } else {
            bitmap_in->reset_bit(v);
        }
        snap_id1 = graph_in->get_eviction(v, evict_count);
        if (prev_snapid < snap_id1 && snap_id >= snap_id1) {
            #ifdef DEL
            degree_in[v].add_count -= evict_count; 
            degree_in[v].del_count -= evict_count; 
            #else
            degree_in[v] -= evict_count; 
            #endif
        }
    }

    return;
}

template <class T>
degree_t diff_view_t<T>::get_degree_out(vid_t v)
{
    return get_actual(wdegree_out[v]);
}

template <class T>
degree_t diff_view_t<T>::get_degree_in(vid_t v)
{
    return get_actual(wdegree_in[v]);
}

template <class T>
degree_t diff_view_t<T>::get_nebrs_out(vid_t v, T* adj_list)
{
    return graph_out->get_nebrs(v, adj_list, wdegree_out[v], reader_id);
}
template<class T>
degree_t diff_view_t<T>::get_nebrs_in(vid_t v, T* adj_list)
{
    return graph_in->get_nebrs(v, adj_list, wdegree_in[v], reader_id);
}


template <class T>
void diff_view_t<T>::handle_flagu()
{
    sid_t src, dst;
    edgeT_t<T> edge;
    vid_t src_vid, dst_vid;
    bool is_del = false;
    bool is_private = IS_PRIVATE(flag);
    bool is_simple = IS_SIMPLE(flag);
    if (true == is_simple && true == is_private) {
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
void diff_view_t<T>::handle_flagd()
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
void diff_view_t<T>::handle_flaguni()
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
