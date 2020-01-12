#pragma once

#include "view_interface.h"

template <class T>
class snap_t : public gview_t <T> {
 protected: 
    onegraph_t<T>*   graph_out;
    onegraph_t<T>*   graph_in;
    sdegree_t*       degree_out;
    sdegree_t*       degree_in;
    int              reader_id;

    //private copies, if flag is set
    edgeT_t<T>*      edges; //Non archived edges
    index_t          edge_count;//their count
    
 public:
    using gview_t<T>::pgraph;
    using gview_t<T>::snapshot;
    using gview_t<T>::prev_snapshot;
    using gview_t<T>::sstream_func;
    using gview_t<T>::thread;
    using gview_t<T>::algo_meta;
    using gview_t<T>::v_count;
    using gview_t<T>::flag;
    using gview_t<T>::reader;
    using gview_t<T>::reg_id;
 
 public:
    inline snap_t() {
        graph_out = 0;
        graph_in = 0;
        degree_out = 0;
        degree_in = 0;
        edges = 0;
        edge_count = 0;
        reader_id - -1;
    }
    inline ~snap_t() {
        if (reg_id != -1) {
            reader.blog->unregister_reader(reg_id);
        }
        if (degree_in ==  degree_out) {
            graph_out->unregister_reader(reader_id);
            free(degree_out);
            
        } else {
            free(degree_out);
            if (degree_in) {
                graph_in->unregister_reader(reader_id);
                free(degree_in);
            }
        }
        if (snapshot) snapshot->drop_ref();
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

    inline int  is_unidir() {
       return ((graph_out != graph_in) && (graph_in ==0));
    }
    inline bool is_ddir() {
       return ((graph_out != graph_in) && (graph_in !=0));
    }
    inline bool is_udir() {
       return (graph_out == graph_in);
    }
    void init_view(pgraph_t<T>* ugraph, index_t flag);
    status_t update_view();
  protected:
    void create_degreesnap(onegraph_t<T>* graph, sdegree_t* degree);
    void handle_flagu();
    void handle_flagd();
    void handle_flaguni();
};

template <class T>
void snap_t<T>::create_degreesnap(onegraph_t<T>* graph, sdegree_t* degree)
{
    snapid_t snap_id = 0;
    if (snapshot) {
        snap_id = snapshot->snap_id;

        #pragma omp for 
        for (vid_t v = 0; v < v_count; ++v) {
            degree[v] = graph->get_degree(v, snap_id);
            //cout << v << " " << degree_out[v] << endl;
        }
    }
}

template <class T>
void snap_t<T>::handle_flagu()
{
    vid_t src_vid, dst_vid;
    edgeT_t<T> edge;
    bool is_del = false;
    bool is_private = IS_PRIVATE(flag);
    bool is_simple = IS_SIMPLE(flag);
    if (true == is_simple && is_private) {
        #pragma omp for
        for (index_t i = 0; i < edge_count; ++i) {
            src_vid = TO_VID(get_src(edges[i]));
            dst_vid = TO_VID(get_dst(edges[i]));
            is_del = IS_DEL(get_src(edges[i]));
#ifdef DEL
            if (is_del) {
            __sync_fetch_and_add(&degree_out[src_vid].del_count, 1);
            __sync_fetch_and_add(&degree_out[dst_vid].del_count, 1);
            } else {
            __sync_fetch_and_add(&degree_out[src_vid].add_count, 1);
            __sync_fetch_and_add(&degree_out[dst_vid].add_count, 1);
            }
#else
            if (is_del) {assert(0);
            __sync_fetch_and_add(&degree_out+src_vid, 1);
            __sync_fetch_and_add(&degree_out+dst_vid, 1);
            }
#endif
        }
    } else if (true == is_simple) {
        for (index_t i = reader.tail; i < reader.marker; ++i) {
            read_edge(reader.blog, i, edge);
            src_vid = TO_VID(get_src(edge));
            dst_vid = TO_VID(get_dst(edge));
            is_del = IS_DEL(get_src(edge));
#ifdef DEL
            if (is_del) {
            __sync_fetch_and_add(&degree_out[src_vid].del_count, 1);
            __sync_fetch_and_add(&degree_out[dst_vid].del_count, 1);
            } else {
            __sync_fetch_and_add(&degree_out[src_vid].add_count, 1);
            __sync_fetch_and_add(&degree_out[dst_vid].add_count, 1);
            }
#else
            if (is_del) {assert(0);
            __sync_fetch_and_add(&degree_out+src_vid, 1);
            __sync_fetch_and_add(&degree_out+dst_vid, 1);
            }
#endif
        }
    }
}

template <class T>
void snap_t<T>::handle_flagd()
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
            is_del = IS_DEL(get_src(edges[i]));
#ifdef DEL
            if (is_del) {
            __sync_fetch_and_add(&degree_out[src_vid].del_count, 1);
            __sync_fetch_and_add(&degree_in[dst_vid].del_count, 1);
            } else {
            __sync_fetch_and_add(&degree_out[src_vid].add_count, 1);
            __sync_fetch_and_add(&degree_in[dst_vid].add_count, 1);
            }
#else
            if (is_del) {assert(0);
            __sync_fetch_and_add(&degree_out+src_vid, 1);
            __sync_fetch_and_add(&degree_in+dst_vid, 1);
            }
#endif
        }
    } else if (true == is_simple) {
        for (index_t i = reader.tail; i < reader.marker; ++i) {
            read_edge(reader.blog, i, edge);
            src_vid = TO_VID(get_src(edge));
            dst_vid = TO_VID(get_dst(edge));
            is_del = IS_DEL(get_src(edge));
#ifdef DEL
            if (is_del) {
            __sync_fetch_and_add(&degree_out[src_vid].del_count, 1);
            __sync_fetch_and_add(&degree_in[dst_vid].del_count, 1);
            } else {
            __sync_fetch_and_add(&degree_out[src_vid].add_count, 1);
            __sync_fetch_and_add(&degree_in[dst_vid].add_count, 1);
            }
#else
            if (is_del) {assert(0);
            __sync_fetch_and_add(&degree_out+src_vid, 1);
            __sync_fetch_and_add(&degree_in+dst_vid, 1);
            }
#endif
        }
    }
}

template <class T>
void snap_t<T>::handle_flaguni()
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
            is_del = IS_DEL(get_src(edges[i]));
#ifdef DEL
            if (is_del) {
            __sync_fetch_and_add(&degree_out[src_vid].del_count, 1);
            } else {
            __sync_fetch_and_add(&degree_out[src_vid].del_count, 1);
            }
#else
            if (is_del) {assert(0);
            __sync_fetch_and_add(&degree_out+src_vid, 1);
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
            is_del = IS_DEL(get_src(edge));
#ifdef DEL
            if (is_del) {
            __sync_fetch_and_add(&degree_out[src_vid].del_count, 1);
            } else {
            __sync_fetch_and_add(&degree_out[src_vid].del_count, 1);
            }
#else
            if (is_del) {assert(0);
            __sync_fetch_and_add(&degree_out+src_vid, 1);
            }
#endif
        }
    }
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
    
    if (!IS_STALE(flag)) {
        reader.blog = pgraph->blog;
        reader_id = reader.blog->register_reader(&reader);
    }
    graph_out = ugraph->sgraph_out[0];
    degree_out = (sdegree_t*) calloc(v_count, sizeof(sdegree_t));
    reader_id = graph_out->register_reader(this, degree_out);
    
    if (ugraph->sgraph_in == ugraph->sgraph_out) {
        graph_in   = graph_out;
        degree_in  = degree_out;
    } else if (ugraph->sgraph_in != 0) {
        graph_in  = ugraph->sgraph_in[0];
        degree_in = (sdegree_t*) calloc(v_count, sizeof(sdegree_t));
        reader_id = graph_in->register_reader(this, degree_in);
    }
}

template <class T>
status_t snap_t<T>::update_view()
{
    blog_t<T>*  blog = pgraph->blog;
    index_t marker = blog->blog_head;
    
    snapshot = pgraph->get_snapshot();
    
    index_t snap_marker = 0;
    if (snapshot) {
        snap_marker = snapshot->marker;
    }

    if (IS_PRIVATE(flag)) {
        edge_count = marker - snap_marker;
        edges = (edgeT_t<T>*)realloc(edges, edge_count*sizeof(edgeT_t<T>));
        
        //copy the edges
        reader.marker = marker;
        reader.tail = snap_marker;
        for (index_t i = reader.tail; i < reader.marker; ++i) {
            read_edge(reader.blog, i, edges[reader.tail - i]);
        }
        reader.marker = -1L;
        reader.tail = -1L;
    } else if (!IS_STALE(flag)) {
        reader.marker = marker;
        reader.tail = snap_marker;
    }
    
    //Degree and actual edge arrays
    #pragma omp parallel num_threads(THD_COUNT)
    {
    create_degreesnap(graph_out, degree_out);
    if (graph_in != graph_out && graph_in != 0) {
        create_degreesnap(graph_in, degree_in);
    }
    if (graph_in != graph_out && graph_in != 0) {
        handle_flagd();
    } else if (graph_in == graph_out) {
        handle_flagu();
    } else {
        handle_flaguni();
    }
    }
    if (prev_snapshot) prev_snapshot->drop_ref();
    
    //update complete
    prev_snapshot = snapshot;

    //wait for archiving to complete
    if (IS_SIMPLE(flag)) {
        pgraph->waitfor_archive(reader.marker);
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
    return get_actual(degree_out[v]);
}

template <class T>
degree_t snap_t<T>::get_degree_in(vid_t v)
{
    return get_actual(degree_in[v]);
}

template <class T>
degree_t snap_t<T>::get_nebrs_out(vid_t v, T* adj_list)
{
    return graph_out->get_nebrs(v, adj_list, degree_out[v], reader_id);
}
template<class T>
degree_t snap_t<T>::get_nebrs_in(vid_t v, T* adj_list)
{
    return graph_in->get_nebrs(v, adj_list, degree_in[v], reader_id);
}
