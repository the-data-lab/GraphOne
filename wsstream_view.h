#pragma once

template <class T>
class wsstream_t {
 public:
    snapshot_t*      snapshot;//new snapshot
    snapshot_t*      snapshot1;//old snapshot
    pgraph_t<T>*     pgraph;
    void*            algo_meta;//algorithm specific data
    typename callback<T>::wsfunc   wsstream_func;
    
    //The adjacency data
    vert_table_t<T>* graph_out;
    vert_table_t<T>* graph_in;
    
    //ending marker of the window
    degree_t*        degree_out;
    degree_t*        degree_in;
    
    //starting marker of the window
    degree_t*        degree_out1;
    degree_t*        degree_in1;
    
    //few edges at the end of window, 0 for stale
    edgeT_t<T>*      edges; //new edges
    index_t          edge_count;//their count
    
    //Edges at the begining of the window, 0 for stale
    edgeT_t<T>*      edges1; //old edges
    index_t          edge_count1; //their count
    
    Bitmap*          bitmap_in;
    Bitmap*          bitmap_out;
    
    index_t          window_sz;
    vid_t            v_count; 
    int              flag;
 public:
    inline wsstream_t() {
        degree_out = 0;
        degree_in = 0;
        degree_out1 = 0;
        degree_in1 = 0;
        snapshot = 0;
        edges = 0;
        edges1=0;
        edge_count = 0;
        edge_count1 = 0;
        algo_meta = 0;
        pgraph = 0;
    }
    inline ~wsstream_t() {
    }
 public:
    inline void    set_algometa(void* a_meta) {algo_meta = a_meta;}
    inline void*   get_algometa() {return algo_meta;}
    
    degree_t get_nebrs_out(vid_t vid, T*& ptr);
    degree_t get_nebrs_in (vid_t vid, T*& ptr);
    degree_t get_degree_out(vid_t vid);
    degree_t get_degree_in (vid_t vid);
    
    //Don't provide this
    delta_adjlist_t<T>* get_nebrs_archived_out(vid_t);
    delta_adjlist_t<T>* get_nebrs_archived_in(vid_t v);
    
    void init_wsstream_view(pgraph_t<T>* pgraph, index_t window_sz, bool simple, bool priv, bool stale);
    void create_view(pgraph_t<T>* pgraph, bool simple, bool priv, bool stale);
    status_t update_view();
    index_t get_current_window_sz();
    
    void create_degreesnap();
    void create_degreesnapd();
    
    void update_degreesnap();
    void update_degreesnapd();
};

template <class T>
void wsstream_t<T>::init_wsstream_view(pgraph_t<T>* ugraph, index_t window_sz1, bool simple, bool priv, bool stale)
{
    pgraph  = ugraph;
    snapshot1 = pgraph->get_snapshot();

    blog_t<T>* blog = pgraph->blog;
    index_t  marker = blog->blog_head;
    index_t new_marker = snapshot1->marker;

    //for degree creation at the beginning marker of window
    edges1 = blog->blog_beg + (new_marker & blog->blog_mask);
    edge_count1 = marker - new_marker;
    
    snapshot = 0;
    edges = 0;
    edge_count = 0;
    v_count = g->get_type_scount();
    
    window_sz = window_sz1;
    if (stale) {
        SET_STALE(flag);
    }
    if (priv) {
        SET_PRIVATE(flag);
    }
    if (simple) {
        SET_SIMPLE(flag);
    }
    
    onegraph_t<T>* sgraph_out = ugraph->sgraph_out[0];
    graph_out  = sgraph_out->get_begpos();
    
    degree_out = (degree_t*) calloc(v_count, sizeof(degree_t));
    degree_out1 = (degree_t*) calloc(v_count, sizeof(degree_t));
    
    if (ugraph->sgraph_in == ugraph->sgraph_out) {
        graph_in   = graph_out;
        degree_in  = degree_out;
        degree_in1 = degree_out1;
        create_degreesnap();
    } else if (ugraph->sgraph_in != 0) {
        graph_in   = ugraph->sgraph_in[0]->get_begpos();
        degree_in  = (degree_t*) calloc(v_count, sizeof(degree_t));
        degree_in1 = (degree_t*) calloc(v_count, sizeof(degree_t));
        create_degreesnapd();
    }

    if (graph_out == graph_in) {
        bitmap_in = bitmap_out;
    } else {
        bitmap_in = new Bitmap(v_count);
    }
}

template <class T>
status_t wsstream_t<T>::update_view()
{
    blog_t<T>* blog = pgraph->blog;
    index_t  marker = blog->blog_head;
    snapshot_t* new_snapshot = pgraph->get_snapshot();
    
    if (new_snapshot == 0|| (new_snapshot == snapshot)) return eNoWork;
    
    index_t new_marker = new_snapshot->marker;
    index_t old_marker = edge_count1 + snapshot1 ? snapshot1->marker : 0;

    if (false == IS_STALE(flag)) {
        if(marker - old_marker < window_sz) {
            return eNoWork;
        } 
    } else {
        if(new_marker - old_marker < window_sz) {
            return eNoWork;
        }
    }
    
    snapshot = new_snapshot;
    
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

template <class T>
void wsstream_t<T>::create_degreesnap()
{
    #pragma omp parallel
    {
    snapid_t snap_id = 0;
    if (snapshot1) {
        snap_id = snapshot1->snap_id;
    
        snapT_t<T>*   snap_blob = 0;
        degree_t      nebr_count = 0;
        
        #pragma omp for 
        for (vid_t v = 0; v < v_count; ++v) {
            snap_blob = graph_out[v].get_snapblob();
            if (0 == snap_blob) { 
                degree_out1[v] = 0;
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
            degree_out1[v] = nebr_count;
            //cout << v << " " << degree_out[v] << endl;
        }
    }
        //if (false == IS_STALE(flag)) {
        #pragma omp for
        for (index_t i = 0; i < edge_count1; ++i) {
            __sync_fetch_and_add(degree_out1 + edges1[i].src_id, 1);
            __sync_fetch_and_add(degree_out1 + get_dst(edges1 + i), 1);
        }
    } 
}

template <class T>
void wsstream_t<T>::create_degreesnapd()
{
    #pragma omp parallel
    {
        snapid_t snap_id = 0;
        if (snapshot1) {
            snap_id = snapshot1->snap_id;
        }

        vid_t   vcount_out = v_count;
        vid_t   vcount_in  = v_count;

        snapT_t<T>*   snap_blob = 0;
        vid_t        nebr_count = 0;
        
        #pragma omp for nowait 
        for (vid_t v = 0; v < vcount_out; ++v) {
            snap_blob = graph_out[v].get_snapblob();
            if (0 == snap_blob) {
                degree_out1[v] = 0;
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
            degree_out1[v] = nebr_count;
        }
        
        #pragma omp for nowait 
        for (vid_t v = 0; v < vcount_in; ++v) {
            snap_blob = graph_in[v].get_snapblob();
            if (0 == snap_blob) { 
                degree_in1[v] = 0;
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
            degree_in1[v] = nebr_count;
        }
        if (false == IS_STALE(flag)) {
            #pragma omp for
            for (index_t i = 0; i < edge_count1; ++i) {
                __sync_fetch_and_add(degree_out1 + edges1[i].src_id, 1);
                __sync_fetch_and_add(degree_in1 + get_dst(edges1+i), 1);
            }
        }
    }
    return;
}

template <class T>
void wsstream_t<T>::update_degreesnap()
{
    #pragma omp parallel
    {
    snapid_t snap_id = 0;
    if (snapshot) {
        snap_id = snapshot->snap_id;
    
        snapT_t<T>*   snap_blob = 0;
        degree_t      nebr_count = 0;
        
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
    }
    if (false == IS_STALE(flag)) {
        #pragma omp for
        for (index_t i = 0; i < edge_count; ++i) {
            __sync_fetch_and_add(degree_out + edges[i].src_id, 1);
            __sync_fetch_and_add(degree_out + get_dst(edges + i), 1);
        }
    }
    }

    //Update the degree nodes for the starting marker of window
    // 1. fetch the durable degree edges
    index_t how_much = (IS_STALE(flag) ? 0: edge_count)
            + snapshot->marker - snapshot1->marker - edge_count1;
    edges1 = pgraph->get_prior_edges(snapshot1->marker + edge_count1, how_much);
    // 2. compute the degree
    for (index_t i = 0; i < how_much; ++i) {
        __sync_fetch_and_add(degree_out1 + edges1[i].src_id, 1);
        __sync_fetch_and_add(degree_out1 + get_dst(edges1 + i), 1);
    }
}

template <class T>
void wsstream_t<T>::update_degreesnapd()
{

    #pragma omp parallel
    {
        vid_t   vcount_out = v_count;
        vid_t   vcount_in  = v_count;
        snapT_t<T>*   snap_blob = 0;
        degree_t      nebr_count = 0;
        
        snapid_t snap_id = 0;
        if (snapshot) {
            snap_id = snapshot->snap_id;

            
            
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

        //Update the degree nodes for the starting marker of window
        // 1. fetch the durable degree edges
        index_t how_much = (IS_STALE(flag) ? 0: edge_count)
                + snapshot->marker - snapshot1->marker - edge_count1;
        edges1 = pgraph->get_prior_edges(snapshot1->marker + edge_count1, how_much);
        // 2. compute the degree
        for (index_t i = 0; i < how_much; ++i) {
            __sync_fetch_and_add(degree_out1 + edges1[i].src_id, 1);
            __sync_fetch_and_add(degree_in1 + get_dst(edges1 + i), 1);
        }
    }

    return;
}
