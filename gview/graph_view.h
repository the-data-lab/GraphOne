#pragma once
#include <stdlib.h>

#include "graph.h"

using namespace std;

#define STALE_MASK 0x1
#define PRIVATE_MASK 0x2
#define SIMPLE_MASK  0x4

#define SET_STALE(x) (x | STALE_MASK)
#define SET_PRIVATE(x) (x | PRIVATE_MASK)
#define SET_SIMPLE(x) (x | SIMPLE_MASK)

#define IS_STALE(x) (x & STALE_MASK)
#define IS_PRIVATE(x) (x & PRIVATE_MASK)
#define IS_SIMPLE(x) (x & SIMPLE_MASK)

template <class T>
class vert_table_t;

#include "static_view.h"
#include "sstream_view.h"
#include "stream_view.h"
#include "wsstream_view.h"

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
    vid_t    get_vcount() { return v_count; }
};
    
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
    //if edge log allocated, delete it
    delete snaph;
}

template <class T>
sstream_t<T>* reg_sstream_view(pgraph_t<T>* ugraph, typename callback<T>::sfunc func,
                               bool simple, bool priv, bool stale)
{
    sstream_t<T>* sstreamh = new sstream_t<T>;
    
    sstreamh->init_sstream_view(ugraph, simple, priv, stale);
    sstreamh->sstream_func = func;
    sstreamh->algo_meta = 0;
    
    return sstreamh;
}

template <class T>
void unreg_sstream_view(sstream_t<T>* sstreamh)
{
    delete sstreamh;
}

template <class T>
wsstream_t<T>* reg_wsstream_view(pgraph_t<T>* ugraph, index_t window_sz, 
                typename callback<T>::wsfunc func, bool simple, bool priv, bool stale)
{
    wsstream_t<T>* wsstreamh = new wsstream_t<T>;
    
    wsstreamh->init_wsstream_view(ugraph, window_sz, simple, priv, stale);
    wsstreamh->wsstream_func = func;
    wsstreamh->algo_meta = 0;
    
    return wsstreamh;
}

template <class T>
void unreg_wsstream_view(wsstream_t<T>* wsstreamh)
{
    delete wsstreamh;
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
