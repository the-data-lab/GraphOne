#pragma once

#ifdef _MPI

#include "scopy_view.h"
#include "scopy1d_view.h"
#include "scopy2d_view.h"
#include "copy2d_view.h"

template <class T>
scopy2d_server_t<T>* reg_scopy2d_server(pgraph_t<T>* ugraph, 
                    typename callback<T>::sfunc func, index_t flag)
{
    scopy2d_server_t<T>* sstreamh = new scopy2d_server_t<T>;
    
    sstreamh->init_view(ugraph, flag);
    sstreamh->sstream_func = func;
    sstreamh->algo_meta = 0;
    
    if (IS_THREAD(flag)) {
        if (0 != pthread_create(&sstreamh->thread, 0, &sstream_func<T>, sstreamh)) {
            assert(0);
        }
        //cout << "created scopy2d_server thread" << endl;
    }
    
    return sstreamh;
}

template <class T>
void unreg_scopy2d_server(scopy2d_server_t<T>* sstreamh)
{
    delete sstreamh;
}

template <class T>
scopy2d_client_t<T>* reg_scopy2d_client(pgraph_t<T>* ugraph, 
                        typename callback<T>::sfunc func, index_t flag)
{
    scopy2d_client_t<T>* sstreamh = new scopy2d_client_t<T>;
    
    sstreamh->init_view(ugraph, flag);
    sstreamh->sstream_func = func;
    
    if (IS_THREAD(flag)) {
        if (0 != pthread_create(&sstreamh->thread, 0, &sstream_func<T>, sstreamh)) {
            assert(0);
        }
        //cout << "created scopy2d_client thread" << endl;
    }
    
    return sstreamh;
}

template <class T>
void unreg_scopy2d_client(scopy2d_client_t<T>* sstreamh)
{
    delete sstreamh;
}

template <class T>
copy2d_server_t<T>* reg_copy2d_server(pgraph_t<T>* ugraph, 
                    typename callback<T>::sfunc func, index_t flag)
{
    copy2d_server_t<T>* sstreamh = new copy2d_server_t<T>;
    
    sstreamh->init_view(ugraph, flag);
    sstreamh->sstream_func = func;
    sstreamh->algo_meta = 0;
    
    if (IS_THREAD(flag)) {
        if (0 != pthread_create(&sstreamh->thread, 0, &sstream_func<T>, sstreamh)) {
            assert(0);
        }
        //cout << "created scopy2d_server thread" << endl;
    }
    
    return sstreamh;
}

template <class T>
void unreg_copy2d_server(copy2d_server_t<T>* sstreamh)
{
    delete sstreamh;
}

template <class T>
copy2d_client_t<T>* reg_copy2d_client(pgraph_t<T>* ugraph, 
                        typename callback<T>::sfunc func, index_t flag)
{
    copy2d_client_t<T>* sstreamh = new copy2d_client_t<T>;
    
    sstreamh->init_view(ugraph, flag);
    sstreamh->sstream_func = func;
    
    if (IS_THREAD(flag)) {
        if (0 != pthread_create(&sstreamh->thread, 0, &sstream_func<T>, sstreamh)) {
            assert(0);
        }
        //cout << "created scopy2d_client thread" << endl;
    }
    
    return sstreamh;
}

template <class T>
void unreg_copy2d_client(copy2d_client_t<T>* sstreamh)
{
    delete sstreamh;
}

template <class T>
scopy_server_t<T>* reg_scopy_server(pgraph_t<T>* ugraph, 
                    typename callback<T>::sfunc func, index_t flag)
{
    scopy_server_t<T>* sstreamh = new scopy_server_t<T>;
    
    sstreamh->init_view(ugraph, flag);
    sstreamh->sstream_func = func;
    sstreamh->algo_meta = 0;
    
    if (IS_THREAD(flag)) {
        if (0 != pthread_create(&sstreamh->thread, 0, &sstream_func<T>, sstreamh)) {
            assert(0);
        }
        cout << "created scopy_server thread" << endl;
    }
    
    return sstreamh;
}

template <class T>
scopy_client_t<T>* reg_scopy_client(pgraph_t<T>* ugraph, 
                        typename callback<T>::sfunc func, index_t flag)
{
    scopy_client_t<T>* sstreamh = new scopy_client_t<T>;
    
    sstreamh->init_view(ugraph, flag);
    sstreamh->sstream_func = func;
    
    if (IS_THREAD(flag)) {
        if (0 != pthread_create(&sstreamh->thread, 0, &sstream_func<T>, sstreamh)) {
            assert(0);
        }
        cout << "created scopy_client thread" << endl;
    }
    
    return sstreamh;
}

template <class T>
void unreg_scopy_client(scopy_client_t<T>* sstreamh)
{
    delete sstreamh;
}

template <class T>
void unreg_scopy_server(scopy_server_t<T>* sstreamh)
{
    delete sstreamh;
}

template <class T>
scopy1d_server_t<T>* reg_scopy1d_server(pgraph_t<T>* ugraph, 
                    typename callback<T>::sfunc func, index_t flag)
{
    scopy1d_server_t<T>* sstreamh = new scopy1d_server_t<T>;
    
    sstreamh->init_view(ugraph, flag);
    sstreamh->sstream_func = func;
    sstreamh->algo_meta = 0;
    
    if (IS_THREAD(flag)) {
        if (0 != pthread_create(&sstreamh->thread, 0, &sstream_func<T>, sstreamh)) {
            assert(0);
        }
        cout << "created scopy1d_server thread" << endl;
    }
    
    return sstreamh;
}

template <class T>
void unreg_scopy1d_server(scopy1d_server_t<T>* sstreamh)
{
    delete sstreamh;
}

template <class T>
scopy1d_client_t<T>* reg_scopy1d_client(pgraph_t<T>* ugraph, 
                        typename callback<T>::sfunc func, index_t flag)
{
    scopy1d_client_t<T>* sstreamh = new scopy1d_client_t<T>;
    
    sstreamh->init_view(ugraph, flag);
    sstreamh->sstream_func = func;
    
    if (IS_THREAD(flag)) {
        if (0 != pthread_create(&sstreamh->thread, 0, &sstream_func<T>, sstreamh)) {
            assert(0);
        }
        cout << "created scopy1d_client thread" << endl;
    }
    
    return sstreamh;
}

template <class T>
void unreg_scopy1d_client(scopy1d_client_t<T>* sstreamh)
{
    delete sstreamh;
}
#endif
