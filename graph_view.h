#pragma once
#include <stdlib.h>

#include "graph.h"

using namespace std;


template <class T>
class vert_table_t;


template <class T>
struct snap_t {
    vert_table_t<T>* graph_out;
    vert_table_t<T>* graph_in;
    degree_t*        degree_out;
    degree_t*        degree_in;

    snapshot_t*      snapshot;
    pgraph_t<T>*     pgraph;  
    edgeT_t<T>*      edges; //new edges
    vid_t            edge_count;//their count
    vid_t            v_count;
public:

    degree_t get_nebrs_out(vid_t vid, T*& ptr);
    degree_t get_nebrs_in (vid_t vid, T*& ptr);
    degree_t get_degree_out(vid_t vid);
    degree_t get_degree_in (vid_t vid);
     
    //Raw access, low level APIs. TODO
    //nebrcount_t get_nebrs_archived_out(vid_t, T*& ptr);
    //nebrcount_t get_nebrs_archived_in(vid_t, T*& ptr);

    void create_view(pgraph_t<T>* pgraph, bool simple, bool priv, bool stale);
    void update_view();
};


template <class T>
class sstream_t : public snap_t<T> {
 public:
    using snap_t<T>::graph_out;
    using snap_t<T>::graph_in;
    using snap_t<T>::degree_out;
    using snap_t<T>::degree_in;

    using snap_t<T>::snapshot;
    using snap_t<T>::pgraph;
    using snap_t<T>::edges; //new edges
    using snap_t<T>::edge_count;
    using snap_t<T>::v_count;
    
    void*            algo_meta;//algorithm specific data
    typename callback<T>::sfunc   sstream_func; 

 public:
    sstream_t(): snap_t<T>(){
        algo_meta = 0;
        pgraph = 0;
    }
 public:
    inline void    set_algometa(void* a_meta) {algo_meta = a_meta;}
    inline void*   get_algometa() {return algo_meta;}

};

template <class T>
class wsstream_t {
 public:
    snapshot_t*      snapshot;
    pgraph_t<T>*     pgraph;
    vid_t            v_count; 
    void*            algo_meta;//algorithm specific data
    typename callback<T>::wsfunc   wsstream_func;
    
    //ending marker of the window
    degree_t*        degree_out;
    degree_t*        degree_in;
    
    //starting marker of the window
    degree_t*        degree_out1;
    degree_t*        degree_in1;
    
    edgeT_t<T>*      edges; //new edges
    
    //For edge centric, zero otherwise
    index_t          edge_count;//their count
    
    //last few edges of above data-structure, 0 for stale
    index_t          non_archived_count;
    
    
    edgeT_t<T>*      edges1; //old edges
    index_t          edge_count1; //their count

 public:
    wsstream_t(){
        degree_out = 0;
        degree_in = 0;
        degree_out1 = 0;
        degree_in1 = 0;
        snapshot = 0;
        edges = 0;
        edges1=0;
        edge_count = 0;
        algo_meta = 0;
        pgraph = 0;
    }
 public:
    inline void    set_algometa(void* a_meta) {algo_meta = a_meta;}
    inline void*   get_algometa() {return algo_meta;}
    
    degree_t get_nebrs_out(vid_t vid, T*& ptr);
    degree_t get_nebrs_in (vid_t vid, T*& ptr);
    degree_t get_degree_out(vid_t vid);
    degree_t get_degree_in (vid_t vid);
    
    nebrcount_t get_nebrs_archived_out(wsstream_t<T>* snaph, vid_t, T*& ptr);
    nebrcount_t get_nebrs_archived_in(wsstream_t<T>* snaph, vid_t, T*& ptr);
    
    void update_wsstream_view(wsstream_t<T>* wsstreamh);
};

template <class T>
class stream_t {
 public:
    index_t          edge_offset;//Compute starting offset
    edgeT_t<T>*      edges; //mew edges
    index_t          edge_count;//their count
    pgraph_t<T>*     pgraph;
    
    void*            algo_meta;//algorithm specific data
    typename callback<T>::func   stream_func;


 public:
    stream_t(){
        edges = 0;
        edge_count = 0;
        algo_meta = 0;
    }
 public:
    inline void    set_algometa(void* a_meta) {algo_meta = a_meta;}
    inline void*   get_algometa() {return algo_meta;}

    inline edgeT_t<T>* get_edges() { return edges;}
    inline void        set_edges(edgeT_t<T>*a_edges) {edges = a_edges;}
    
    inline index_t     get_edgecount() { return edge_count;}
    inline void        set_edgecount(index_t a_edgecount){edge_count = a_edgecount;}

 public:   
    void update_stream_view();
};

template <class T>
struct prior_snap_t {
    degree_t*        degree_out;
    degree_t*        degree_in;
    degree_t*        degree_out1;
    degree_t*        degree_in1;

    pgraph_t<T>*     pgraph;  
    vid_t            v_count;
public:
    prior_snap_t() {
        degree_out = 0;
        degree_in  = 0;
        degree_out1= 0;
        degree_in1 = 0;
    }
    degree_t get_nebrs_out(vid_t vid, T* ptr);
    degree_t get_nebrs_in (vid_t vid, T* ptr);
    degree_t get_degree_out(vid_t vid);
    degree_t get_degree_in (vid_t vid);
};

template <class T>
degree_t* create_degreesnap (vert_table_t<T>* graph, vid_t v_count, snapshot_t* snapshot, index_t marker, edgeT_t<T>* edges, degree_t* degree_array)
{
    snapid_t snap_id = 0;
    index_t old_marker = 0;
    if (snapshot) {
        snap_id = snapshot->snap_id;
        old_marker = snapshot->marker;
    }

    #pragma omp parallel
    {
        snapT_t<T>*   snap_blob = 0;
        vid_t        nebr_count = 0;
        
        #pragma omp for 
        for (vid_t v = 0; v < v_count; ++v) {
            snap_blob = graph[v].get_snapblob();
            if (0 == snap_blob) { 
                degree_array[v] = 0;
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
            degree_array[v] = nebr_count;
            //cout << v << " " << degree_array[v] << endl;
        }

        #pragma omp for
        for (index_t i = old_marker; i < marker; ++i) {
            __sync_fetch_and_add(degree_array + edges[i].src_id, 1);
            __sync_fetch_and_add(degree_array + get_dst(edges + i), 1);
        }
    }

    return degree_array;
}

template <class T>
void create_degreesnapd (vert_table_t<T>* begpos_out, vert_table_t<T>* begpos_in,
                         snapshot_t* snapshot, index_t marker, edgeT_t<T>* edges, 
                         degree_t* &degree_out, degree_t* &degree_in, vid_t v_count)
{
    snapid_t snap_id = 0;
    index_t old_marker = 0;
    if (snapshot) {
        snap_id = snapshot->snap_id;
        old_marker = snapshot->marker;
    }

    vid_t           vcount_out = v_count;
    vid_t           vcount_in  = v_count;

    #pragma omp parallel
    {
        snapT_t<T>*   snap_blob = 0;
        vid_t        nebr_count = 0;
        
        #pragma omp for nowait 
        for (vid_t v = 0; v < vcount_out; ++v) {
            snap_blob = begpos_out[v].get_snapblob();
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
            degree_out[v] = nebr_count;
        }
        
        #pragma omp for nowait 
        for (vid_t v = 0; v < vcount_in; ++v) {
            snap_blob = begpos_in[v].get_snapblob();
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
            degree_in[v] = nebr_count;
        }

        #pragma omp for
        for (index_t i = old_marker; i < marker; ++i) {
            __sync_fetch_and_add(degree_out + edges[i].src_id, 1);
            __sync_fetch_and_add(degree_in + get_dst(edges+i), 1);
        }
    }

    return;
}


template <class T>
void snap_t<T>::update_view()
{
    snapshot_t* new_snapshot = pgraph->get_snapshot();
    if (new_snapshot == 0|| (new_snapshot == snapshot)) return;
    
    blog_t<T>* blog = pgraph->blog;
    
    //index_t  marker = blog->blog_head;
    index_t new_marker   = new_snapshot->marker;
    
    //for stale
    edges = blog->blog_beg;
    edge_count = 0;//marker - new_marker;
    
        
    if (pgraph->sgraph_in == 0) {
        create_degreesnap(graph_out, v_count, new_snapshot, 
                          new_marker, edges, degree_out);
    } else {
        create_degreesnapd(graph_out, graph_in, new_snapshot,
                           new_marker, edges, degree_out,
                           degree_in, v_count);
    }

    snapshot = new_snapshot;
}

template <class T>
void stream_t<T>::update_stream_view()
{
    blog_t<T>* blog = pgraph->blog;
    index_t  marker = blog->blog_head;
    
    index_t a_edge_count = edge_count;
    
    //XXX need to copy it
    edges = blog->blog_beg + edge_offset + a_edge_count;
    edge_count = marker - edge_offset;
    edge_offset += a_edge_count;
}


template <class T>
void snap_t<T>::create_view(pgraph_t<T>* pgraph1, bool simple, bool priv, bool stale)
{
    pgraph   = pgraph1;
    
    snapshot = pgraph->get_snapshot();
    blog_t<T>*        blog = pgraph->blog;
    index_t         marker = blog->blog_head;
    
    index_t old_marker = 0;
    if (snapshot) {
        old_marker = snapshot->marker;
    }
    cout << "old marker = " << old_marker << endl;    
    cout << "end marker = " << marker << endl;    

    //need to copy it. TODO
    edges     = blog->blog_beg;
    if (stale) {
        edge_count = 0;
    } else {
        edge_count = marker - old_marker;
    }
    
    v_count    = g->get_type_scount();
    
    //Degree and actual edge arrays
    graph_out  = pgraph->sgraph[0]->get_begpos();
    degree_out = (degree_t*) calloc(v_count, sizeof(degree_t));
    
    if (pgraph->sgraph_in == pgraph->sgraph_out) {
        graph_in   = graph_out;
        degree_in  = degree_out;
        create_degreesnap(graph_out, v_count, snapshot,
                          marker, edges, degree_out);
    } else if (pgraph->sgraph_in != 0) {
        graph_in  = pgraph->sgraph_in[0]->get_begpos();
        degree_in = (degree_t*) calloc(v_count, sizeof(degree_t));
        create_degreesnapd(graph_out, graph_in, snapshot,
                           marker, edges, degree_out, degree_in, v_count);
    }
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
degree_t snap_t<T>::get_nebrs_out(vid_t v, T*& adj_list)
{
    vert_table_t<T>* graph  = graph_out;
    vunit_t<T>*      v_unit = graph[v].get_vunit();
    if (0 == v_unit) return 0;
    
    degree_t local_degree = 0;
    degree_t total_degree = 0;
    T* local_adjlist = 0;

    delta_adjlist_t<T>* delta_adjlist;
    delta_adjlist           = v_unit->delta_adjlist;
    degree_t nebr_count     = get_degree_out(v);
    degree_t delta_degree   = nebr_count;

    //cout << "delta adjlist " << delta_degree << endl;	
    //cout << "Nebr list of " << v <<" degree = " << nebr_count << endl;	
    
    //traverse the delta adj list
    while (delta_adjlist != 0 && delta_degree > 0) {
        local_adjlist    = delta_adjlist->get_adjlist();
        local_degree     = delta_adjlist->get_nebrcount();
        degree_t i_count = min(local_degree, delta_degree);
        
        memcpy(adj_list + total_degree, local_adjlist, sizeof(T)*i_count);
        delta_adjlist = delta_adjlist->get_next();
        delta_degree -= i_count;
        total_degree += i_count;
    }
    return nebr_count;
}
template<class T>
degree_t snap_t<T>::get_nebrs_in(vid_t v, T*& adj_list)
{
    vert_table_t<T>* graph  = graph_in;
    vunit_t<T>*      v_unit = graph[v].get_vunit();
    if (0 == v_unit) return 0;
    
    degree_t local_degree = 0;
    degree_t total_degree = 0;
    T* local_adjlist = 0;

    delta_adjlist_t<T>* delta_adjlist;
    delta_adjlist           = v_unit->delta_adjlist;
    degree_t nebr_count     = get_degree_in(v);
    degree_t delta_degree   = nebr_count;

    //cout << "delta adjlist " << delta_degree << endl;	
    //cout << "Nebr list of " << v <<" degree = " << nebr_count << endl;	
    
    //traverse the delta adj list
    while (delta_adjlist != 0 && delta_degree > 0) {
        local_adjlist    = delta_adjlist->get_adjlist();
        local_degree     = delta_adjlist->get_nebrcount();
        degree_t i_count = min(local_degree, delta_degree);
        
        memcpy(adj_list + total_degree, local_adjlist, sizeof(T)*i_count);
        delta_adjlist = delta_adjlist->get_next();
        delta_degree -= i_count;
        total_degree += i_count;
    }
    return nebr_count;
}
     

template <class T>
degree_t prior_snap_t<T>::get_degree_out(vid_t vid)
{
    return degree_out[vid] - degree_out1[vid];
}

template <class T>
degree_t prior_snap_t<T>::get_degree_in(vid_t vid)
{
    return degree_in[vid] - degree_in1[vid];
}

template <class T>
degree_t prior_snap_t<T>::get_nebrs_out(vid_t vid, T* ptr)
{
    return pgraph->get_wnebrs_out(vid, ptr, degree_out1[vid], get_degree_out(vid));
}

template <class T>
degree_t prior_snap_t<T>::get_nebrs_in(vid_t vid, T* ptr)
{
    if (degree_out == degree_in) {
        return pgraph->get_wnebrs_out(vid, ptr, degree_in1[vid], get_degree_in(vid));
    } else {
        return pgraph->get_wnebrs_in(vid, ptr, degree_in1[vid], get_degree_in(vid));
    }
}

template <class T>
snap_t<T>* create_static_view(pgraph_t<T>* pgraph, bool simple, bool priv, bool stale)
{
    snap_t<T>* snaph = new snap_t<T>;
    snaph->create_view(pgraph, simple, priv, stale); 
    return snaph;
}

template <class T>
void delete_static_view(snap_t<T>* snaph) 
{
    if (snaph->degree_in ==  snaph->degree_out) {
        delete snaph->degree_out;
    } else {
        delete snaph->degree_out;
        delete snaph->degree_in;
    }
    //if edge log allocated, delete it
    delete snaph;
}

template <class T>
sstream_t<T>* reg_sstream_view(pgraph_t<T>* ugraph, typename callback<T>::sfunc func, bool simple, bool priv, bool stale)
{
    sstream_t<T>* sstreamh = new sstream_t<T>;
    sstreamh->sstream_func = func;
    sstreamh->algo_meta = 0;
    
    sid_t v_count = g->get_type_scount();
    sstreamh->v_count = v_count;
    
    sstreamh->pgraph     = ugraph;
    
    onegraph_t<T>* sgraph_out = ugraph->sgraph_out[0];
    sstreamh->graph_out  = sgraph_out->get_begpos();
    sstreamh->degree_out = (degree_t*) calloc(v_count, sizeof(degree_t));
    
    if (ugraph->sgraph_in == ugraph->sgraph_out) {
        sstreamh->graph_in   = sstreamh->graph_out;
        sstreamh->degree_in  = sstreamh->degree_out;
    } else if (ugraph->sgraph_in != 0) {
        sstreamh->graph_in  = ugraph->sgraph_in[0]->get_begpos();
        sstreamh->degree_in = (degree_t*) calloc(v_count, sizeof(degree_t));
    }
    
    return sstreamh;
}

template <class T>
void unreg_sstream_view(sstream_t<T>* sstreamh)
{
    if (sstreamh->degree_in ==  sstreamh->degree_out) {
        delete sstreamh->degree_out;
    } else {
        delete sstreamh->degree_out;
        delete sstreamh->degree_in;
    }
    delete sstreamh;
}

template <class T>
stream_t<T>* reg_stream_view(pgraph_t<T>* ugraph, typename callback<T>::func func1) 
{
    stream_t<T>* streamh = new stream_t<T>;
    blog_t<T>* blog = ugraph->blog;
    index_t  marker = blog->blog_head;

    streamh->edge_offset = marker;
    streamh->edges      = 0;
    streamh->edge_count = 0;
    
    streamh->pgraph = ugraph;
    streamh->stream_func = func1;
    streamh->algo_meta = 0;

    return streamh;
}

template <class T>
void unreg_stream_view(stream_t<T>* a_streamh)
{
    //XXX may need to delete the edge log
    delete a_streamh;
}

template <class T>
prior_snap_t<T>* create_prior_static_view(pgraph_t<T>* pgraph, index_t start_offset, index_t end_offset)
{
    prior_snap_t<T>* snaph = new prior_snap_t<T>;
    /*
    pgraph_t<T>* pgraph = (pgraph_t<T>*)get_plaingraph();
    
    
    pgraph   = pgraph1;
    
    v_count  = g->get_type_scount();
    degree_out = (degree_t*) calloc(v_count, sizeof(degree_t));
    degree_out1= (degree_t*) calloc(v_count, sizeof(degree_t));
    
    if (pgraph->sgraph_in == pgraph->sgraph_out) {
        degree_in  = degree_out;
        degree_in1 = degree_out1;
    } else if (pgraph->sgraph_in != 0) {
        degree_in  = (degree_t*) calloc(v_count, sizeof(degree_t));
        degree_in1 = (degree_t*) calloc(v_count, sizeof(degree_t));
    }

    if (0 != start_offset) {
        pgraph->create_degree(degree_out1, degree_in1, 0, start_offset);
        memcpy(degree_out, degree_out1, sizeof(degree_t)*v_count);
        
        if (pgraph->sgraph_out != pgraph->sgraph_in && pgraph->sgraph_in != 0) {
            memcpy(degree_in, degree_in1, sizeof(degree_t)*v_count);
        }
    }
        
    pgraph->create_degree(degree_out, degree_in, start_offset, end_offset);

    */

    return snaph;
}
