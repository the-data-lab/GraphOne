#pragma once

#include "view_interface.h"

template <class T>
class snap_t : public gview_t <T> {
 protected: 
    onegraph_t<T>*   graph_out;
    onegraph_t<T>*   graph_in;
    sdegree_t*       degree_out;
    sdegree_t*       degree_in;

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
        graph_out = 0;
        graph_in = 0;
        degree_out = 0;
        degree_in = 0;
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

    degree_t start_out(vid_t v, header_t<T>& header, degree_t offset = 0);
    degree_t start_in(vid_t v, header_t<T>& header, degree_t offset = 0);
    status_t next(header_t<T>& header, T& dst);
    
    delta_adjlist_t<T>* get_nebrs_archived_out(vid_t);
    delta_adjlist_t<T>* get_nebrs_archived_in(vid_t);
    index_t get_nonarchived_edges(edgeT_t<T>*& ptr);

    virtual int  is_unidir() {
       return ((graph_out != graph_in) && (graph_in ==0));
    }
    void init_view(pgraph_t<T>* ugraph, index_t flag);
    status_t update_view();
    void create_degreesnap(onegraph_t<T>* graph, sdegree_t* degree);
};

template <class T>
void snap_t<T>::create_degreesnap(onegraph_t<T>* graph, sdegree_t* degree)
{
    {
        snapid_t snap_id = 0;
        if (snapshot) {
            snap_id = snapshot->snap_id;

            #pragma omp for nowait 
            for (vid_t v = 0; v < v_count; ++v) {
                degree[v] = graph->get_degree(v, snap_id);
                //cout << v << " " << degree_out[v] << endl;
            }
        }
    }
    /*
    if (false == IS_STALE(flag)) {
        #pragma omp for
        for (index_t i = 0; i < edge_count; ++i) {
            __sync_fetch_and_add(degree_out + edges[i].src_id, 1);
            __sync_fetch_and_add(degree_out + get_dst(edges + i), 1);
        }
    }
    if (false == IS_STALE(flag)) {
    #pragma omp for
    for (index_t i = 0; i < edge_count; ++i) {
        __sync_fetch_and_add(degree_out + edges[i].src_id, 1);
        __sync_fetch_and_add(degree_in + get_dst(edges+i), 1);
    }
    }*/
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
    degree_out = (sdegree_t*) calloc(v_count, sizeof(sdegree_t));
    
    if (ugraph->sgraph_in == ugraph->sgraph_out) {
        graph_in   = graph_out;
        degree_in  = degree_out;
    } else if (ugraph->sgraph_in != 0) {
        graph_in  = ugraph->sgraph_in[0];
        degree_in = (sdegree_t*) calloc(v_count, sizeof(sdegree_t));
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
    #pragma omp parallel num_threads(THD_COUNT)
    {
    create_degreesnap(graph_out, degree_out);
    if (graph_in != graph_out && graph_in != 0) {
        create_degreesnap(graph_in, degree_in);
    }
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
degree_t snap_t<T>::start_out(vid_t v, header_t<T>& header, degree_t offset/*=0*/)
{
    degree_t degree = get_degree_out(v) - offset;
    if (degree != 0) {
        graph_out->start(v, header, offset);
    }
    return degree;
}

template <class T>
degree_t snap_t<T>::start_in(vid_t v, header_t<T>& header, degree_t offset/*=0*/)
{
    degree_t degree = get_degree_in(v) - offset;
    if (degree != 0) {
        graph_in->start(v, header, offset);
    }
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
#ifdef DEL
    return degree_out[v].add_count - degree_out[v].del_count;
#else
    return degree_out[v];
#endif
    
}

template <class T>
degree_t snap_t<T>::get_degree_in(vid_t v)
{
#ifdef DEL
    return degree_in[v].add_count - degree_in[v].del_count;
#else
    return degree_in[v];
#endif
}

template <class T>
degree_t snap_t<T>::get_nebrs_out(vid_t v, T* adj_list)
{
    return graph_out->get_nebrs(v, adj_list, degree_out[v]);
}
template<class T>
degree_t snap_t<T>::get_nebrs_in(vid_t v, T* adj_list)
{
    return graph_in->get_nebrs(v, adj_list, degree_in[v]);
}
