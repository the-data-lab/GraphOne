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

 protected: 
    //end  of the window
    using sstream_t<T>::snapshot;
    using sstream_t<T>::prev_snapshot;
    degree_t*        wdegree_out;
    degree_t*        wdegree_in;
    
    //beginning of the the window
    using sstream_t<T>::degree_out;
    using sstream_t<T>::degree_in;
    using sstream_t<T>::edges;
    using sstream_t<T>::edge_count;
    index_t          marker;
    
    index_t          window_sz;

 public:
    inline wsstream_t() {
        marker = 0;
        wdegree_out = 0;
        wdegree_in = 0;
    }
    inline ~wsstream_t() {
        if (wdegree_in == wdegree_out && wdegree_out != NULL) {
            delete wdegree_out;
        } else if (wdegree_in != wdegree_out) {
            delete wdegree_out;
            delete wdegree_in;
        }
    }
    degree_t get_nebrs_out(vid_t vid, T* ptr);
    degree_t get_nebrs_in (vid_t vid, T* ptr);
    degree_t get_degree_out(vid_t vid);
    degree_t get_degree_in (vid_t vid);
    
    void init_view(pgraph_t<T>* pgraph, index_t window_sz, index_t flag);
    status_t update_view();
    void update_degreesnap();
    void update_degreesnapd();
};

template <class T>
void wsstream_t<T>::init_view(pgraph_t<T>* pgraph, index_t window_sz1, 
                                       index_t a_flag)
{
    sstream_t<T>::init_view(pgraph, window_sz1, a_flag);
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
    snapshot_t* new_snapshot = pgraph->get_snapshot();
    
    if (new_snapshot == 0|| (new_snapshot == prev_snapshot)) return eNoWork;
    
    index_t new_marker = new_snapshot->marker;
    
    if(new_marker - marker < window_sz) {
            return eNoWork;
    }
    
    index_t old_marker = prev_snapshot? prev_snapshot->marker: 0;
    edge_count = new_marker - old_marker;

    snapshot = new_snapshot;
    
    edges = pgraph->get_prior_edges(marker, marker + edge_count);
    marker += edge_count;
    
    if (pgraph->sgraph_in == 0) {
        update_degreesnap();
    } else {
        update_degreesnapd();
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
    #pragma omp parallel
    {
        snapid_t    snap_id = snapshot->snap_id;
        snapid_t    prev_snapid = prev_snapshot->snap_id;
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
        for (index_t i = 0; i < edge_count; ++i) {
            __sync_fetch_and_add(degree_out + edges[i].src_id, 1);
            __sync_fetch_and_add(degree_out + get_dst(edges + i), 1);
            bitmap_out->set_bit_atomic(edges[i].src_id);
            bitmap_out->set_bit_atomic(get_dst(edges + i));
        }
    }
}

template <class T>
void wsstream_t<T>::update_degreesnapd()
{
    #pragma omp parallel
    {
        degree_t nebr_count = 0;
        degree_t evict_count = 0;; 
        snapid_t    snap_id = snapshot->snap_id;
        snapid_t    prev_snapid = prev_snapshot->snap_id;
        snapid_t    snap_id1 = 0;

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
            
            nebr_count = graph_in->get_degree(v, snap_id);
            snap_id1 = graph_in->get_eviction(v, evict_count);
            if (wdegree_in[v] != nebr_count) {
                wdegree_in[v] = nebr_count;
                bitmap_in->set_bit(v);
            } else {
                bitmap_in->reset_bit(v);
            }
            if (prev_snapid < snap_id1 && snap_id >= snap_id1) {
                degree_in[v] -= evict_count; 
            }
        }

        //Update the degree nodes for the starting marker of window
        // 1. fetch the durable degree edges
        // 2. compute the degree
        #pragma omp for
        for (index_t i = 0; i < edge_count; ++i) {
            __sync_fetch_and_add(degree_out + edges[i].src_id, 1);
            __sync_fetch_and_add(degree_in + get_dst(edges + i), 1);
            bitmap_out->set_bit_atomic(edges[i].src_id);
            bitmap_in->set_bit_atomic(get_dst(edges + i));
        }
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
    return graph_out->get_wnebrs(v, adj_list, degree_out[v], get_degree_out(v));
}

template<class T>
degree_t wsstream_t<T>::get_nebrs_in(vid_t v, T* adj_list)
{
    return graph_in->get_wnebrs(v, adj_list, degree_in[v], get_degree_in(v));
}
