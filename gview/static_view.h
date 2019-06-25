#pragma once

#include "view_interface.h"

template <class T>
class snap_t : public gview_t <T> {
 protected: 
    onegraph_t<T>*   graph_out;
    onegraph_t<T>*   graph_in;
    degree_t*        degree_out;
    degree_t*        degree_in;

    snapshot_t*      snapshot;
    
    edgeT_t<T>*      edges; //Non archived edges
    index_t          edge_count;//their count
    
    //For edge-centric
    edgeT_t<T>*      new_edges;//New edges
    index_t          new_edge_count;//Their count

    vid_t            v_count;
    int              flag;
 public:
    pgraph_t<T>*     pgraph;  
    void*    algo_meta;//algorithm specific data

 public:
    inline snap_t() {
        flag = 0;
        v_count = 0;
        algo_meta = 0;
        new_edges = 0;
        new_edge_count = 0;
    }
    inline ~snap_t() {
        if (degree_in ==  degree_out) {
            delete degree_out;
        } else {
            delete degree_out;
            delete degree_in;
        }
    }
    inline void set_algometa(void* a_meta) {algo_meta = a_meta;}
    inline void* get_algometa() {return algo_meta;}
    inline index_t get_snapmarker() {
        if (snapshot) return snapshot->marker;
        return 0;
    }

    degree_t get_nebrs_out(vid_t vid, T* ptr);
    degree_t get_nebrs_in (vid_t vid, T* ptr);
    degree_t get_degree_out(vid_t vid);
    degree_t get_degree_in (vid_t vid);
    vid_t    get_vcount() { return v_count;};     
    
    delta_adjlist_t<T>* get_nebrs_archived_out(vid_t);
    delta_adjlist_t<T>* get_nebrs_archived_in(vid_t);
    index_t get_nonarchived_edges(edgeT_t<T>*& ptr);

    void init_view(pgraph_t<T>* ugraph, bool simple, bool priv, bool stale, index_t v_or_e_centric);
    void create_degreesnap();
    void create_degreesnapd();
    void create_view(pgraph_t<T>* pgraph, bool simple, bool priv, bool stale);
    status_t update_view();
    inline int  get_snapid() {return snapshot->snap_id;}
};

template <class T>
void snap_t<T>::create_degreesnap()
{
    #pragma omp parallel
    {
        snapid_t snap_id = 0;
        if (snapshot) {
            snap_id = snapshot->snap_id;

            #pragma omp for 
            for (vid_t v = 0; v < v_count; ++v) {
                degree_out[v] = graph_out->get_degree(v, snap_id);
                //cout << v << " " << degree_out[v] << endl;
            }
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
void snap_t<T>::create_degreesnapd()
{
    #pragma omp parallel
    {
        snapid_t snap_id = 0;
        if (snapshot) {
            snap_id = snapshot->snap_id;
        }

        vid_t vcount_out = v_count;
        vid_t vcount_in  = v_count;

        #pragma omp for nowait 
        for (vid_t v = 0; v < vcount_out; ++v) {
            degree_out[v] = graph_out->get_degree(v, snap_id);;
        }
        
        #pragma omp for nowait 
        for (vid_t v = 0; v < vcount_in; ++v) {
            degree_in[v] = graph_in->get_degree(v, snap_id);
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
void snap_t<T>::init_view(pgraph_t<T>* ugraph, bool simple, bool priv, bool stale, index_t v_or_e_centric)
{
    snapshot = 0;
    edges = 0;
    edge_count = 0;
    v_count = g->get_type_scount();
    pgraph  = ugraph;
    
    graph_out = ugraph->sgraph_out[0];
    degree_out = (degree_t*) calloc(v_count, sizeof(degree_t));
    
    if (ugraph->sgraph_in == ugraph->sgraph_out) {
        graph_in   = graph_out;
        degree_in  = degree_out;
    } else if (ugraph->sgraph_in != 0) {
        graph_in  = ugraph->sgraph_in[0];
        degree_in = (degree_t*) calloc(v_count, sizeof(degree_t));
    }
    
    if (stale) {
        SET_STALE(flag);
    }
    if (priv) {
        SET_PRIVATE(flag);
    }
    if (simple) {
        SET_SIMPLE(flag);
    }
    if (IS_V_CENTRIC(v_or_e_centric)) {
        SET_V_CENTRIC(flag);
    }
    if (IS_E_CENTRIC(v_or_e_centric)) {
        SET_E_CENTRIC(flag);
    }
}

template <class T>
void snap_t<T>::create_view(pgraph_t<T>* pgraph1, bool simple, bool priv, bool stale)
{
    init_view(pgraph1, simple, priv, stale, V_CENTRIC);

    blog_t<T>*  blog = pgraph->blog;
    index_t marker = blog->blog_head;
    snapshot = pgraph->get_snapshot();
    
    index_t old_marker = 0;
    if (snapshot) {
        old_marker = snapshot->marker;
    }
    //cout << "old marker = " << old_marker << endl << "end marker = " << marker << endl;    
    //need to copy it. TODO
    edges     = blog->blog_beg + (old_marker & blog->blog_mask);
    edge_count = marker - old_marker;
    //if (stale)  edge_count = 0;
    
    //Degree and actual edge arrays
    
    if (pgraph->sgraph_in == pgraph->sgraph_out) {
        create_degreesnap();
    } else if (pgraph->sgraph_in != 0) {
        create_degreesnapd();
    }
}
template <class T>
status_t snap_t<T>::update_view()
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
        create_degreesnap();
    } else {
        create_degreesnapd();
    }

    return eOK;
}

template <class T>
delta_adjlist_t<T>* snap_t<T>::get_nebrs_archived_in(vid_t v)
{
    return graph_in->get_delta_adjlist(v);
}

template <class T>
delta_adjlist_t<T>* snap_t<T>::get_nebrs_archived_out(vid_t v)
{
    return graph_out->get_delta_adjlist(v);
}

template <class T>
index_t snap_t<T>::get_nonarchived_edges(edgeT_t<T>*& ptr)
{
    ptr = edges;
    return edge_count;
}

template <class T>
degree_t snap_t<T>::get_degree_out(vid_t v)
{
    return degree_out[v];
}

template <class T>
degree_t snap_t<T>::get_degree_in(vid_t v)
{
    return degree_in[v];
}

template <class T>
degree_t snap_t<T>::get_nebrs_out(vid_t v, T* adj_list)
{
    return graph_out->get_nebrs(v, adj_list, get_degree_out(v));
}
template<class T>
degree_t snap_t<T>::get_nebrs_in(vid_t v, T* adj_list)
{
    return graph_in->get_nebrs(v, adj_list, get_degree_in(v));
}
