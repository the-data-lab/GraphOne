#pragma once

#include "static_view.h"

template <class T>
class sstream_t : public snap_t<T> {
 public:
    using snap_t<T>::pgraph;

 protected:
    using snap_t<T>::degree_in;
    using snap_t<T>::degree_out;
    using snap_t<T>::graph_out;
    using snap_t<T>::graph_in;
    using snap_t<T>::snapshot;
    using snap_t<T>::edges;
    using snap_t<T>::edge_count;
    using snap_t<T>::v_count;
    using snap_t<T>::flag;

    Bitmap*  bitmap_in;
    Bitmap*  bitmap_out;
 public:
    void*    algo_meta;//algorithm specific data
    typename callback<T>::sfunc   sstream_func; 

 public:
    inline sstream_t(): snap_t<T>() {
        algo_meta = 0;
        pgraph = 0;
        bitmap_in = 0;
        bitmap_out = 0;
    }
    inline ~sstream_t() {
        if (bitmap_in != bitmap_out) {
            delete bitmap_in;
            bitmap_in = 0;
        }
        if(bitmap_out != 0) {
            delete bitmap_out;
            bitmap_out = 0;
        }
    }

    inline void    set_algometa(void* a_meta) {algo_meta = a_meta;}
    inline void*   get_algometa() {return algo_meta;}
    
    status_t    update_view();
    inline bool has_vertex_changed_out(vid_t v) {return bitmap_out->get_bit(v);}
    inline bool has_vertex_changed_in(vid_t v) {return bitmap_in->get_bit(v);}
    
    inline void init_sstream_view(pgraph_t<T>* pgraph, bool simple, bool priv, bool stale)
    {
        this->init_view(pgraph, simple, priv, stale);
        
        bitmap_out = new Bitmap(v_count);
        if (graph_out == graph_in) {
            bitmap_in = bitmap_out;
        } else {
            bitmap_in = new Bitmap(v_count);
        }
    }
    void update_degreesnap();
    void update_degreesnapd();
};

template <class T>
void sstream_t<T>::update_degreesnap()
{
    snapid_t snap_id = 0;
    if (snapshot) {
        snap_id = snapshot->snap_id;
    } 

    #pragma omp parallel
    {
        snapT_t<T>*   snap_blob = 0;
        degree_t        nebr_count = 0;
        
        #pragma omp for 
        for (vid_t v = 0; v < v_count; ++v) {
            snap_blob = graph_out[v].get_snapblob();
            if (0 == snap_blob) { 
                degree_out[v] = 0;
                continue; 
            }
            
            nebr_count = 0;
            if (snap_id >= snap_blob->snap_id) {
                nebr_count = snap_blob->degree; 
            } else {
                snap_blob = snap_blob->prev;
                while (snap_blob && snap_id < snap_blob->snap_id) {
                    snap_blob = snap_blob->prev;
                }
                if (snap_blob) {
                    nebr_count = snap_blob->degree; 
                }
            }
            if (degree_out[v] != nebr_count) {
                degree_out[v] = nebr_count;
                bitmap_out->set_bit(v);
            } else {
                bitmap_out->reset_bit(v);
            }
            //cout << v << " " << degree_out[v] << endl;
        }
        if (false == IS_STALE(flag)) {
        #pragma omp for
        for (index_t i = 0; i < edge_count; ++i) {
            __sync_fetch_and_add(degree_out + edges[i].src_id, 1);
            __sync_fetch_and_add(degree_out + get_dst(edges + i), 1);
        }
        }
    }
}

template <class T>
void sstream_t<T>::update_degreesnapd()
{
    snapid_t snap_id = 0;
    if (snapshot) {
        snap_id = snapshot->snap_id;
    }

    vid_t   vcount_out = v_count;
    vid_t   vcount_in  = v_count;

    #pragma omp parallel
    {
        snapT_t<T>*   snap_blob = 0;
        degree_t      nebr_count = 0;
        
        #pragma omp for nowait 
        for (vid_t v = 0; v < vcount_out; ++v) {
            snap_blob = graph_out[v].get_snapblob();
            if (0 == snap_blob) {
                degree_out[v] = 0;
                continue; 
            }
            
            nebr_count = 0;
            if (snap_id >= snap_blob->snap_id) {
                nebr_count = snap_blob->degree; 
            } else {
                snap_blob = snap_blob->prev;
                while (snap_blob && snap_id < snap_blob->snap_id) {
                    snap_blob = snap_blob->prev;
                }
                if (snap_blob) {
                    nebr_count = snap_blob->degree; 
                }
            }
            if (degree_out[v] != nebr_count) {
                degree_out[v] = nebr_count;
                bitmap_out->set_bit(v);
            } else {
                bitmap_out->reset_bit(v);
            }
        }
        
        #pragma omp for nowait 
        for (vid_t v = 0; v < vcount_in; ++v) {
            snap_blob = graph_in[v].get_snapblob();
            if (0 == snap_blob) { 
                degree_in[v] = 0;
                continue; 
            }
            
            nebr_count = 0;
            if (snap_id >= snap_blob->snap_id) {
                nebr_count = snap_blob->degree; 
            } else {
                snap_blob = snap_blob->prev;
                while (snap_blob && snap_id < snap_blob->snap_id) {
                    snap_blob = snap_blob->prev;
                }
                if (snap_blob) {
                    nebr_count = snap_blob->degree; 
                }
            }
            if (degree_in[v] != nebr_count) {
                degree_in[v] = nebr_count;
                bitmap_in->set_bit(v);
            } else {
                bitmap_in->reset_bit(v);
            }
        }
        if (false == IS_STALE(flag)) {
        #pragma omp for
        for (index_t i = 0; i < edge_count; ++i) {
            __sync_fetch_and_add(degree_out + edges[i].src_id, 1);
            __sync_fetch_and_add(degree_in + get_dst(edges+i), 1);
        }
        }
    }

    return;
}

template <class T>
status_t sstream_t<T>::update_view()
{
    blog_t<T>* blog = pgraph->blog;
    index_t  marker = blog->blog_head;
    snapshot_t* new_snapshot = pgraph->get_snapshot();
    
    if (new_snapshot == 0|| (new_snapshot == snapshot)) return eNoWork;
    
    snapshot = new_snapshot;
    
    index_t new_marker   = new_snapshot->marker;
    
    //for stale
    edges = blog->blog_beg + (new_marker & blog->blog_mask);
    edge_count = marker - new_marker;
    
    if (pgraph->sgraph_in == 0) {
        update_degreesnap();
    } else {
        update_degreesnapd();
    }

    return eOK;
}
