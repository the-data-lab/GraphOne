#pragma once

template <class T>
struct wsstream_t : public sstream_t<T> {
    using sstream_t<T>::pgraph;
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
    sdegree_t*        wdegree_out;
    sdegree_t*        wdegree_in;
    
    //beginning of the the window
    using sstream_t<T>::degree_out;
    using sstream_t<T>::degree_in;
    edgeT_t<T>*       start_edges;
    index_t           start_count;
    index_t           start_marker;
    
    //handling stale, simple private etc.
    using sstream_t<T>::edges;
    using sstream_t<T>::edge_count;
    
 public:
    index_t          window_sz;

    inline wsstream_t() {
        start_marker = 0;
        wdegree_out = 0;
        wdegree_in = 0;
        start_edges = 0;
        start_count = 0;
        start_marker = 0;
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
    status_t update_view_tumble();
    
    virtual index_t  get_compaction_marker() {
        return start_marker;
    }
    
 private:
    void update_degreesnap();
    void update_degreesnapd();
};

template <class T>
void wsstream_t<T>::init_view(pgraph_t<T>* pgraph, index_t window_sz1, 
                                       index_t a_flag)
{
    sstream_t<T>::init_view(pgraph, a_flag);
    start_marker = 0;
    snapshot = pgraph->get_snapshot();
    
    if (snapshot != 0) {
        start_marker = snapshot->marker;
        #pragma omp parallel num_threads(THD_COUNT)
        {
        this->create_degreesnap(graph_out, degree_out);
        if (graph_in != graph_out && graph_in != 0) {
            this->create_degreesnap(graph_in, degree_in);
        }
        }
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
    index_t snap_marker = snapshot->marker;
    index_t old_marker = prev_snapshot? prev_snapshot->marker: 0;
    index_t end_marker = 0; //start_marker + start_count;
    
    if (IS_STALE(flag)) {
        if (snap_marker - start_marker >= window_sz) {
            end_marker = snap_marker - window_sz;
        } else {
            return eNoWork;
        }
    } else {
        if(marker - start_marker >= window_sz) {
            end_marker = marker - window_sz;
        } else {
            return eNoWork;
        }
    }
    start_count = end_marker - start_marker;
    
    if (start_count) {
        start_edges = (edgeT_t<T>*)realloc(start_edges, start_count*sizeof(edgeT_t<T>));
        assert(start_edges);
        pgraph->get_prior_edges(start_marker, end_marker, start_edges);
    }

    //for stale
    this->handle_visibility(marker, snap_marker); 
    #pragma omp parallel num_threads(THD_COUNT)
    {
    if (graph_in == graph_out || graph_in == 0) {
        update_degreesnap();
    } else {
        update_degreesnapd();
    }
    if (graph_in != graph_out && graph_in != 0) {
        this->handle_flagd(wdegree_out, wdegree_in);
    } else if (graph_in == graph_out) {
        this->handle_flagu(wdegree_out, bitmap_out);
    } else {
        this->handle_flaguni(wdegree_out, bitmap_out);
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
    
    return eOK;
}

template <class T>
status_t wsstream_t<T>::update_view_tumble()
{
    snapshot = pgraph->get_snapshot();
    
    if (snapshot == 0|| (snapshot == prev_snapshot)) return eNoWork;
    
    blog_t<T>*  blog = pgraph->blog;
    snapid_t    snap_id = snapshot->snap_id;
    sdegree_t    nebr_count;

    for (vid_t v = 0; v < v_count; ++v) {
        nebr_count = graph_out->get_degree(v, snap_id);
        degree_out[v] = wdegree_out[v];
        wdegree_out[v] = nebr_count;
    }
    
    if (prev_snapshot) {
        prev_snapshot->drop_ref();
    }
    
    prev_snapshot = snapshot;
    return eOK;
}

template <class T>
void wsstream_t<T>::update_degreesnap()
{
#ifndef DEL
    snapid_t    snap_id = snapshot->snap_id;
    snapid_t    prev_snapid = this->get_prev_snapid();
    snapid_t    snap_id1 = 0;
    sdegree_t    nebr_count = 0;
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
            #ifdef DEL
            assert(0);
            #else
            degree_out[v] -= evict_count; 
            #endif
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
#endif
}

template <class T>
void wsstream_t<T>::update_degreesnapd()
{
    #ifndef DEL
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

    #endif
    return;
}

template <class T>
degree_t wsstream_t<T>::get_degree_out(vid_t v)
{
    return get_actual(wdegree_out[v]) - get_actual(degree_out[v]);
}

template <class T>
degree_t wsstream_t<T>::get_degree_in(vid_t v)
{
    return get_actual(wdegree_in[v]) - get_actual(degree_in[v]);
}

template <class T>
degree_t wsstream_t<T>::get_nebrs_out(vid_t v, T* adj_list)
{
    return graph_out->get_wnebrs(v, adj_list, degree_out[v], get_degree_out(v), reader_id);
}

template<class T>
degree_t wsstream_t<T>::get_nebrs_in(vid_t v, T* adj_list)
{
    return graph_in->get_wnebrs(v, adj_list, degree_in[v], get_degree_in(v), reader_id);
}
