#pragma once
#include <stdlib.h>
#include <pthread.h>
#include "graph.h"

using namespace std;

#define STALE_MASK 0x1
#define PRIVATE_MASK 0x2
#define SIMPLE_MASK  0x4
#define V_CENTRIC 0x8
#define E_CENTRIC  0x10
#define C_THREAD  0x20

#define SET_STALE(x) (x = (x | STALE_MASK))
#define SET_PRIVATE(x) (x = (x | PRIVATE_MASK))
#define SET_SIMPLE(x) (x = (x | SIMPLE_MASK))
#define SET_THREAD(x) (x = (x | C_THREAD))

#define IS_STALE(x) (x & STALE_MASK)
#define IS_PRIVATE(x) (x & PRIVATE_MASK)
#define IS_SIMPLE(x) (x & SIMPLE_MASK)
#define IS_THREAD(x) (x & C_THREAD)

#define SET_V_CENTRIC(x) (x = (x | V_CENTRIC))
#define SET_E_CENTRIC(x) (x = (x | E_CENTRIC))
#define IS_V_CENTRIC(x) (x & V_CENTRIC)
#define IS_E_CENTRIC(x) (x & E_CENTRIC)

template <class T>
class vert_table_t;


#include "static_view.h"
#include "sstream_view.h"
#include "stream_view.h"
#include "wsstream_view.h"
#include "historical_view.h"

template<class T>
void* sstream_func(void* arg) 
{
    gview_t<T>* sstreamh = (gview_t<T>*)arg;
    sstreamh->sstream_func(sstreamh);
    return 0;
}
    
// ------ VIEW CREATE/REGISTER API --------------
template <class T>
snap_t<T>* create_static_view(pgraph_t<T>* pgraph, index_t flag)
{
    if (IS_THREAD(flag)) {
        cout << "Thread creation is not implemented yet" << endl;
        assert(0);
    }
    snap_t<T>* snaph = new snap_t<T>;
    snaph->init_view(pgraph, flag); 
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
                               index_t flag, void* algo_meta = 0)
{
    sstream_t<T>* sstreamh = new sstream_t<T>;
    
    sstreamh->init_view(ugraph, flag);
    sstreamh->sstream_func = func;
    sstreamh->algo_meta = algo_meta;
    
    if (IS_THREAD(flag)) {
        if (0 != pthread_create(&sstreamh->thread, 0, &sstream_func<T>, sstreamh)) {
            assert(0);
        }
        cout << "created sstream thread" << endl;
    }
    
    return sstreamh;
}

template <class T>
void unreg_sstream_view(sstream_t<T>* sstreamh)
{
    delete sstreamh;
}

template <class T>
stream_t<T>* reg_stream_view(pgraph_t<T>* pgraph, typename callback<T>::sfunc func1, 
                               index_t flag, void* algo_meta = 0)
{
    stream_t<T>* streamh = new stream_t<T>;
    streamh->init_view(pgraph, flag);
    streamh->sstream_func = func1;
    streamh->algo_meta = 0;

    if (IS_THREAD(flag)) {
        if (0 != pthread_create(&streamh->thread, 0, &sstream_func<T>, streamh)) {
            assert(0);
        }
        cout << "created sstream thread" << endl;
    }
    return streamh;
}

template <class T>
void unreg_stream_view(stream_t<T>* a_streamh)
{
    //XXX may need to delete the edge log
    delete a_streamh;
}

template <class T>
wsstream_t<T>* reg_wsstream_view(pgraph_t<T>* ugraph, index_t window_sz, 
                typename callback<T>::sfunc func, index_t flag)
{
    wsstream_t<T>* wsstreamh = new wsstream_t<T>;
    
    wsstreamh->init_wsstream_view(ugraph, window_sz, flag);
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
prior_snap_t<T>* create_prior_static_view(pgraph_t<T>* pgraph, index_t start_offset, index_t end_offset)
{
    prior_snap_t<T>* snaph = new prior_snap_t<T>;
    snaph->create_view(pgraph, start_offset, end_offset);
    return snaph;
}
