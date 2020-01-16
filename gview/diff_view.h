#pragma once

template <class T>
struct diff_view_t : public sstream_t<T> {
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
    sdegree_t    nebr_count = 0;
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
    sdegree_t nebr_count = 0;
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
