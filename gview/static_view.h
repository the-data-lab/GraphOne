#pragma once

#include "view_interface.h"

template <class T>
class snap_t : public gview_t <T> {
 protected: 
    onegraph_t<T>*   graph_out;
    onegraph_t<T>*   graph_in;
    degree_t*        degree_out;
    degree_t*        degree_in;

    edgeT_t<T>*      edges; //Non archived edges
    index_t          edge_count;//their count
    
    //For edge-centric
    edgeT_t<T>*      new_edges;//New edges
    index_t          new_edge_count;//Their count

 
 public:
    using gview_t<T>::pgraph;
    using gview_t<T>::snapshot;
    using gview_t<T>::sstream_func;
    using gview_t<T>::thread;
    using gview_t<T>::algo_meta;
    using gview_t<T>::v_count;
    using gview_t<T>::flag;
 
 public:
    inline snap_t() {
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

    degree_t get_nebrs_out(vid_t vid, T* ptr);
    degree_t get_nebrs_in (vid_t vid, T* ptr);
    degree_t get_degree_out(vid_t vid);
    degree_t get_degree_in (vid_t vid);

    degree_t start_out(vid_t v, header_t<T>& header);
    degree_t start_in(vid_t v, header_t<T>& header);
    degree_t start_wout(vid_t v, header_t<T>& header, degree_t start);
    degree_t start_win(vid_t v, header_t<T>& header, degree_t start);
    status_t next(header_t<T>& header, T& dst);
    
    delta_adjlist_t<T>* get_nebrs_archived_out(vid_t);
    delta_adjlist_t<T>* get_nebrs_archived_in(vid_t);
    index_t get_nonarchived_edges(edgeT_t<T>*& ptr);

    void init_view(pgraph_t<T>* ugraph, index_t flag);
    status_t update_view();
    void create_degreesnap();
    void create_degreesnapd();
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
void snap_t<T>::init_view(pgraph_t<T>* ugraph, index_t a_flag)
{
    snapshot = 0;
    edges = 0;
    edge_count = 0;
    v_count = g->get_type_scount();
    pgraph  = ugraph;
    flag = a_flag;
    
    graph_out = ugraph->sgraph_out[0];
    degree_out = (degree_t*) calloc(v_count, sizeof(degree_t));
    
    if (ugraph->sgraph_in == ugraph->sgraph_out) {
        graph_in   = graph_out;
        degree_in  = degree_out;
    } else if (ugraph->sgraph_in != 0) {
        graph_in  = ugraph->sgraph_in[0];
        degree_in = (degree_t*) calloc(v_count, sizeof(degree_t));
    }
}

template <class T>
status_t snap_t<T>::update_view()
{
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
degree_t snap_t<T>::start_out(vid_t v, header_t<T>& header)
{
    degree_t degree = get_degree_out(v);
    if (degree == 0) return 0;

    delta_adjlist_t<T>* delta_adjlist = graph_out->get_delta_adjlist(v);
    header.count = 0; 
    header.next = delta_adjlist->next;
    header.max_count = delta_adjlist->max_count;
    header.adj_list = delta_adjlist->get_adjlist();
}

template <class T>
degree_t snap_t<T>::start_in(vid_t v, header_t<T>& header)
{
    degree_t degree = get_degree_in(v);
    if (degree == 0) return 0;
    
    delta_adjlist_t<T>* delta_adjlist = graph_in->get_delta_adjlist(v);
    header.count = 0; 
    header.next = delta_adjlist->next;
    header.max_count = delta_adjlist->max_count;
    header.adj_list = delta_adjlist->get_adjlist();

}

template <class T>
degree_t snap_t<T>::start_wout(vid_t v, header_t<T>& header, degree_t start)
{
    degree_t degree = get_degree_out(v) - start;

    delta_adjlist_t<T>* delta_adjlist = graph_out->get_delta_adjlist(v);
    
    //traverse the delta adj list
    degree_t delta_degree = start; 
    degree_t local_degree = 0;
    
    while (delta_adjlist != 0 && delta_degree > 0) {
        local_degree = delta_adjlist->get_nebrcount();
        if (delta_degree >= local_degree) {
            delta_adjlist = delta_adjlist->get_next();
            delta_degree -= local_degree;
        } else {
            break;
        }
    }
    
    header.next = delta_adjlist->get_next();
    header.max_count = delta_adjlist->get_maxcount();
    header.count = delta_degree;
    header.adj_list = delta_adjlist->get_adjlist();
    
    return degree;
}

template <class T>
degree_t snap_t<T>::start_win(vid_t v, header_t<T>& header, degree_t start)
{
    degree_t degree = get_degree_in(v) - start;

    delta_adjlist_t<T>* delta_adjlist = graph_in->get_delta_adjlist(v);
    
    //traverse the delta adj list
    degree_t delta_degree = start; 
    degree_t local_degree = 0;
    
    while (delta_adjlist != 0 && delta_degree > 0) {
        local_degree = delta_adjlist->get_nebrcount();
        if (delta_degree >= local_degree) {
            delta_adjlist = delta_adjlist->get_next();
            delta_degree -= local_degree;
        } else {
            break;
        }
    }
    
    header.next = delta_adjlist->get_next();
    header.max_count = delta_adjlist->get_maxcount();
    header.count = delta_degree;
    header.adj_list = delta_adjlist->get_adjlist();
    
    return degree;
}

template <class T>
status_t snap_t<T>::next(header_t<T>& header, T& dst)
{
    if (header.max_count == header.count) {
        delta_adjlist_t<T>* delta_adjlist = header.next;
        header.next = delta_adjlist->get_next();
        header.max_count = delta_adjlist->get_maxcount();
        header.count = 0;
        header.adj_list = delta_adjlist->get_adjlist();
    }
    
    T* local_adjlist = header.adj_list;
    dst = local_adjlist[header.count++];
    return eOK;
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
