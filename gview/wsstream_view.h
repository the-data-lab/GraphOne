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
    using sstream_t<T>::degree_out;
    using sstream_t<T>::degree_in;
    //using sstream_t<T>::edges;
    //using sstream_t<T>::edge_count;
    
    //start of the window
    index_t          wmarker;
    degree_t*        wdegree_out;
    degree_t*        wdegree_in;
    //Edges at the begining of the window,
    edgeT_t<T>*      wedges; //old edges
    index_t          wedge_count; //their count
    
    index_t          window_sz;

 public:
    inline wsstream_t() {
        wmarker = 0;
        wdegree_out = 0;
        wdegree_in = 0;
        wedges = 0;
        wedge_count = 0;
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
void wsstream_t<T>::init_view(pgraph_t<T>* ugraph, index_t window_sz1, 
                                       index_t a_flag)
{
    sstream_t<T>::init_view(ugraph, window_sz1, a_flag);
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
    
    if(new_marker - wmarker < window_sz) {
            return eNoWork;
    }
    
    index_t old_marker = prev_snapshot? prev_snapshot->marker: 0;
    wedge_count = new_marker - old_marker;

    snapshot = new_snapshot;
    
    wedges = pgraph->get_prior_edges(wmarker, wmarker + wedge_count);
    wmarker += wedge_count;
    
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
        degree_t    nebr_count = 0;
        
        #pragma omp for 
        for (vid_t v = 0; v < v_count; ++v) {
            nebr_count = graph_out->get_degree(v, snap_id);
            if (degree_out[v] != nebr_count) {
                degree_out[v] = nebr_count;
                bitmap_out->set_bit(v);
            } else {
                bitmap_out->reset_bit(v);
            }
        }

        //Update the degree nodes for the starting marker of window
        // 1. fetch the durable degree edges
        // 2. compute the degree
        #pragma omp for
        for (index_t i = 0; i < wedge_count; ++i) {
            __sync_fetch_and_add(wdegree_out + wedges[i].src_id, 1);
            __sync_fetch_and_add(wdegree_out + get_dst(wedges + i), 1);
        }
    }
}

template <class T>
void wsstream_t<T>::update_degreesnapd()
{
    #pragma omp parallel
    {
        degree_t nebr_count = 0;
        snapid_t snap_id = snapshot->snap_id;

        #pragma omp for  
        for (vid_t v = 0; v < v_count; ++v) {
            nebr_count = graph_out->get_degree(v, snap_id);
            if (degree_out[v] != nebr_count) {
                degree_out[v] = nebr_count;
                bitmap_out->set_bit(v);
            } else {
                bitmap_out->reset_bit(v);
            }
            nebr_count = graph_in->get_degree(v, snap_id);
            if (degree_in[v] != nebr_count) {
                degree_in[v] = nebr_count;
                bitmap_in->set_bit(v);
            } else {
                bitmap_in->reset_bit(v);
            }
        }

        //Update the degree nodes for the starting marker of window
        // 1. fetch the durable degree edges
        // 2. compute the degree
        #pragma omp for
        for (index_t i = 0; i < wedge_count; ++i) {
            __sync_fetch_and_add(wdegree_out + wedges[i].src_id, 1);
            __sync_fetch_and_add(wdegree_in + get_dst(wedges + i), 1);
        }
    }

    return;
}

template <class T>
degree_t wsstream_t<T>::get_degree_out(vid_t v)
{
    return degree_out[v] - wdegree_out[v];
}

template <class T>
degree_t wsstream_t<T>::get_degree_in(vid_t v)
{
    return degree_in[v] - wdegree_in[v];
}

template <class T>
degree_t wsstream_t<T>::get_nebrs_out(vid_t v, T* adj_list)
{
    return graph_out->get_wnebrs(v, adj_list, wdegree_out[v], get_degree_out(v));
}

template<class T>
degree_t wsstream_t<T>::get_nebrs_in(vid_t v, T* adj_list)
{
    return graph_in->get_wnebrs(v, adj_list, wdegree_in[v], get_degree_in(v));
}
