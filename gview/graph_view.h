#pragma once
#include <stdlib.h>

#include "graph.h"

using namespace std;

#define STALE_MASK 0x1
#define PRIVATE_MASK 0x2
#define SIMPLE_MASK  0x4
#define V_CENTRIC 0x8
#define E_CENTRIC  0x10

#define SET_STALE(x) (x = (x | STALE_MASK))
#define SET_PRIVATE(x) (x = (x | PRIVATE_MASK))
#define SET_SIMPLE(x) (x = (x | SIMPLE_MASK))

#define IS_STALE(x) (x & STALE_MASK)
#define IS_PRIVATE(x) (x & PRIVATE_MASK)
#define IS_SIMPLE(x) (x & SIMPLE_MASK)

#define SET_V_CENTRIC(x) (x = (x | V_CENTRIC))
#define SET_E_CENTRIC(x) (x = (x | E_CENTRIC))
#define IS_V_CENTRIC(x) (x & V_CENTRIC)
#define IS_E_CENTRIC(x) (x & E_CENTRIC)

template <class T>
class vert_table_t;

#include "static_view.h"
#include "sstream_view.h"
#include "scopy_view.h"
#include "stream_view.h"
#include "wsstream_view.h"
#include "historical_view.h"
    
// ------ VIEW CREATE/REGISTER API --------------
template <class T>
snap_t<T>* create_static_view(pgraph_t<T>* pgraph, bool simple, bool priv, bool stale)
{
    snap_t<T>* snaph = new snap_t<T>;
    snaph->init_view(pgraph, simple, priv, stale, V_CENTRIC); 
    snaph->update_view();
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
                               bool simple, bool priv, bool stale, index_t v_or_e_centric = V_CENTRIC)
{
    sstream_t<T>* sstreamh = new sstream_t<T>;
    
    sstreamh->init_sstream_view(ugraph, simple, priv, stale, v_or_e_centric);
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
scopy_server_t<T>* reg_scopy_server(pgraph_t<T>* ugraph, typename callback<T>::sfunc func,
                               bool simple, bool priv, bool stale, index_t v_or_e_centric = V_CENTRIC)
{
    scopy_server_t<T>* sstreamh = new scopy_server_t<T>;
    
    sstreamh->init_sstream_view(ugraph, simple, priv, stale, v_or_e_centric);
    sstreamh->sstream_func = func;
    sstreamh->algo_meta = 0;
    
    return sstreamh;
}

template <class T>
void unreg_scopy_server(scopy_server_t<T>* sstreamh)
{
    delete sstreamh;
}

template <class T>
scopy_client_t<T>* reg_scopy_client(pgraph_t<T>* ugraph, typename callback<T>::sfunc func,
                               bool simple, bool priv, bool stale, index_t v_or_e_centric = V_CENTRIC)
{
    scopy_client_t<T>* sstreamh = new scopy_client_t<T>;
    
    sstreamh->init_sstream_view(ugraph, simple, priv, stale, v_or_e_centric);
    sstreamh->sstream_func = func;
    sstreamh->algo_meta = 0;
    
    return sstreamh;
}

template <class T>
void unreg_scopy_server(scopy_server_t<T>* sstreamh)
{
    delete sstreamh;
}

template <class T>
wsstream_t<T>* reg_wsstream_view(pgraph_t<T>* ugraph, index_t window_sz, 
                typename callback<T>::sfunc func, bool simple, bool priv, bool stale)
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
    snaph->create_view(pgraph, start_offset, end_offset);
    return snaph;
}

