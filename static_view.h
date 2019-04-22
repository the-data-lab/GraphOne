#pragma once

template <class T>
struct snap_t {
    pgraph_t<T>*     pgraph;  
 protected: 
    vert_table_t<T>* graph_out;
    vert_table_t<T>* graph_in;
    degree_t*        degree_out;
    degree_t*        degree_in;

    snapshot_t*      snapshot;
    edgeT_t<T>*      edges; //new edges
    index_t          edge_count;//their count
    vid_t            v_count;
    int              flag;

 public:
    inline snap_t() {
        flag = 0;
        v_count = 0;
    }
    inline ~snap_t() {
        if (degree_in ==  degree_out) {
            delete degree_out;
        } else {
            delete degree_out;
            delete degree_in;
        }
    }
    degree_t get_nebrs_out(vid_t vid, T*& ptr);
    degree_t get_nebrs_in (vid_t vid, T*& ptr);
    degree_t get_degree_out(vid_t vid);
    degree_t get_degree_in (vid_t vid);
    vid_t    get_vcount() { return v_count;};     
    
    delta_adjlist_t<T>* get_nebrs_archived_out(vid_t);
    delta_adjlist_t<T>* get_nebrs_archived_in(vid_t);
    index_t get_nonarchived_edges(edgeT_t<T>*& ptr);

    void init_view(pgraph_t<T>* pgraph, bool simple, bool priv, bool stale);
    void create_degreesnap();
    void create_degreesnapd();
    void create_view(pgraph_t<T>* pgraph, bool simple, bool priv, bool stale);
};

template <class T>
void snap_t<T>::create_degreesnap()
{
    snapid_t snap_id = 0;
    if (snapshot) {
        snap_id = snapshot->snap_id;
    } 

    #pragma omp parallel
    {
        snapT_t<T>*   snap_blob = 0;
        vid_t        nebr_count = 0;
        
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
            degree_out[v] = nebr_count;
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
void snap_t<T>::create_degreesnapd()
{
    snapid_t snap_id = 0;
    if (snapshot) {
        snap_id = snapshot->snap_id;
    }

    vid_t           vcount_out = v_count;
    vid_t           vcount_in  = v_count;

    #pragma omp parallel
    {
        snapT_t<T>*   snap_blob = 0;
        vid_t        nebr_count = 0;
        
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
            degree_out[v] = nebr_count;
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
            degree_in[v] = nebr_count;
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
void snap_t<T>::init_view(pgraph_t<T>* ugraph, bool simple, bool priv, bool stale)
{
    snapshot = 0;
    edges = 0;
    edge_count = 0;
    v_count = g->get_type_scount();
    pgraph  = ugraph;
    
    onegraph_t<T>* sgraph_out = ugraph->sgraph_out[0];
    graph_out  = sgraph_out->get_begpos();
    degree_out = (degree_t*) calloc(v_count, sizeof(degree_t));
    
    if (ugraph->sgraph_in == ugraph->sgraph_out) {
        graph_in   = graph_out;
        degree_in  = degree_out;
    } else if (ugraph->sgraph_in != 0) {
        graph_in  = ugraph->sgraph_in[0]->get_begpos();
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
}

template <class T>
void snap_t<T>::create_view(pgraph_t<T>* pgraph1, bool simple, bool priv, bool stale)
{
    init_view(pgraph1, simple, priv, stale);

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
delta_adjlist_t<T>* snap_t<T>::get_nebrs_archived_in(vid_t v)
{
    vunit_t<T>* v_unit = graph_in[v].get_vunit();
    if (0 == v_unit) return 0;

    return v_unit->delta_adjlist;
}

template <class T>
delta_adjlist_t<T>* snap_t<T>::get_nebrs_archived_out(vid_t v)
{
    vunit_t<T>* v_unit = graph_out[v].get_vunit();
    if (0 == v_unit) return 0;

    return v_unit->delta_adjlist;
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
