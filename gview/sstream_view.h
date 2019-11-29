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
    //For edge-centric
    edgeT_t<T>*      new_edges;//New edges
    index_t          new_edge_count;//Their count

 public:   
    Bitmap*  bitmap_in;
    Bitmap*  bitmap_out;

 public:
    inline sstream_t(): snap_t<T>() {
        bitmap_in = 0;
        bitmap_out = 0;
        new_edges = 0;
        new_edge_count = 0;
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
    
    inline void init_view(pgraph_t<T>* pgraph, index_t a_flag) 
    {
        snap_t<T>::init_view(pgraph, a_flag);
        
        bitmap_out = new Bitmap(v_count);
        if (graph_out == graph_in) {
            bitmap_in = bitmap_out;
        } else if (graph_in !=0){
            bitmap_in = new Bitmap(v_count);
        }
    }

    status_t    update_view();
 private:
    void  update_degreesnap();
    void update_degreesnapd();
 public:
    //These two functions are for vertex centric programming
    inline bool has_vertex_changed_out(vid_t v) {return bitmap_out->get_bit(v);}
    inline bool has_vertex_changed_in(vid_t v) {return bitmap_in->get_bit(v);}
    
    //This function is for edge centric programming. Memory management is our headache
    inline index_t get_new_edges(edgeT_t<T>*& changed_edges) {changed_edges = new_edges; return new_edge_count;}
    //inline index_t get_new_edges_length() {return 0;}

    
    inline void set_vertex_changed_out(vid_t v) {bitmap_out->set_bit_atomic(v);;}
    inline void reset_vertex_changed_out(vid_t v) {bitmap_out->reset_bit(v);;}
};

template <class T>
void sstream_t<T>::update_degreesnap()
{
    #pragma omp parallel num_threads(THD_COUNT)
    {
        sdegree_t nebr_count = 0;
        snapid_t snap_id = 0;
        if (snapshot) {
            snap_id = snapshot->snap_id;

            #pragma omp for 
            for (vid_t v = 0; v < v_count; ++v) {
                nebr_count = graph_out->get_degree(v, snap_id);
                if (degree_out[v] != nebr_count) {
                    degree_out[v] = nebr_count;
                    bitmap_out->set_bit(v);
                } else {
                    bitmap_out->reset_bit(v);
                }
                //cout << v << " " << degree_out[v] << endl;
            }
        }
        /*
        if (false == IS_STALE(flag)) {
            #pragma omp for
            for (index_t i = 0; i < edge_count; ++i) {
                __sync_fetch_and_add(degree_out + edges[i].src_id, 1);
                __sync_fetch_and_add(degree_out + get_dst(edges + i), 1);
            }
        }*/
    }
}

template <class T>
void sstream_t<T>::update_degreesnapd()
{
    #pragma omp parallel num_threads(THD_COUNT)
    {
        sdegree_t      nebr_count = 0;
        snapid_t snap_id = 0;
        if (snapshot) {
            snap_id = snapshot->snap_id;

            vid_t   vcount_out = v_count;
            vid_t   vcount_in  = v_count;

            #pragma omp for nowait 
            for (vid_t v = 0; v < vcount_out; ++v) {
                nebr_count = graph_out->get_degree(v, snap_id);
                if (degree_out[v] != nebr_count) {
                    degree_out[v] = nebr_count;
                    bitmap_out->set_bit(v);
                } else {
                    bitmap_out->reset_bit(v);
                }
            }
            
            #pragma omp for nowait 
            for (vid_t v = 0; v < vcount_in; ++v) {
                nebr_count = graph_in->get_degree(v, snap_id);;
                if (degree_in[v] != nebr_count) {
                    degree_in[v] = nebr_count;
                    bitmap_in->set_bit(v);
                } else {
                    bitmap_in->reset_bit(v);
                }
            }
        }
        /*
        if (false == IS_STALE(flag)) {
            #pragma omp for
            for (index_t i = 0; i < edge_count; ++i) {
                __sync_fetch_and_add(degree_out + edges[i].src_id, 1);
                __sync_fetch_and_add(degree_in + get_dst(edges+i), 1);
            }
        }*/
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
    index_t old_marker = snapshot? snapshot->marker: 0;
    index_t new_marker = new_snapshot->marker;
    
    //Get the edge copies for edge centric computation
    if (IS_E_CENTRIC(flag)) { 
        new_edge_count = new_marker - old_marker;
        if (new_edges!= 0) free(new_edges);
        new_edges = (edgeT_t<T>*)malloc (new_edge_count*sizeof(edgeT_t<T>));
        memcpy(new_edges, blog->blog_beg + (old_marker & blog->blog_mask), new_edge_count*sizeof(edgeT_t<T>));
    }
    snapshot = new_snapshot;
    
    //for stale
    edges = blog->blog_beg + (new_marker & blog->blog_mask);
    edge_count = marker - new_marker;
    
    if (graph_in == graph_out || graph_in == 0) {
        update_degreesnap();
    } else {
        update_degreesnapd();
    }

    return eOK;
}
