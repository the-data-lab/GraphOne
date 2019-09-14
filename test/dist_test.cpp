#ifdef _MPI           

#include <iostream>
using namespace std;

#include "plain_to_edge.h"

#include "comm_view.h"

#include "scopy_analytics.h"
#include "scopy1d_analytics.h"
#include "scopy2d_analytics.h"
#include "copy2d_analytics.h"

template <class T>
void serial_scopy_bfs(const string& idir, const string& odir,
               typename callback<T>::sfunc stream_fn,
               typename callback<T>::sfunc scopy_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    pgraph_t<T>* pgraph = manager.get_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph(_global_vcount);    
    
    if (_rank == 0) {
        //create scopy_server
        scopy_server_t<T>* scopyh = reg_scopy_server(pgraph, scopy_fn, 
                                            STALE_MASK|V_CENTRIC|C_THREAD);
        //CorePin(0);
        manager.prep_graph_edgelog(idir, odir);
        void* ret;
        pthread_join(scopyh->thread, &ret);

    } else {
        //create scopy_client
        scopy_client_t<T>* sclienth = reg_scopy_client(pgraph, stream_fn, 
                                                STALE_MASK|V_CENTRIC|C_THREAD);
        void* ret;
        pthread_join(sclienth->thread, &ret);
    }
}
            
template <class T>
void serial_copy2d_bfs(const string& idir, const string& odir, 
                              typename callback<T>::sfunc stream_fn, 
                              typename callback<T>::sfunc copy_fn)
{
    //int i = 0 ; while (i < 100000) { usleep(100); ++i;}
    plaingraph_manager_t<T> manager;
    assert(_part_count*_part_count + 1 == _numtasks);
    vid_t rem = _global_vcount%_part_count;
    _global_vcount = rem? _global_vcount + _part_count - rem :_global_vcount;
    //cout << "global " <<  _global_vcount << endl; 

    // extract the original group handle
    create_analytics_comm();
    create_2d_comm();
    if (_rank == 0) {
        manager.schema(_dir);
        pgraph_t<T>* pgraph = manager.get_plaingraph();
        //do some setup for plain graphs
        manager.setup_graph(_global_vcount);    
        
        //create scopy_server
        copy2d_server_t<T>* copyh = reg_copy2d_server(pgraph, copy_fn, 
                                            STALE_MASK|V_CENTRIC|C_THREAD);
        manager.prep_graph_edgelog(idir, odir);
        void* ret;
        pthread_join(copyh->thread, &ret);

    } else {
        if (_dir == 0) {
            manager.schema(2);
            _edge_count += _edge_count;
        } else {
            manager.schema(_dir);
        }
        pgraph_t<T>* pgraph = manager.get_plaingraph();

        //do some setup for plain graphs
        vid_t local_vcount = _global_vcount/_part_count;
        manager.setup_graph(local_vcount);
        g->create_threads(true, false);
        
        copy2d_client_t<T>* clienth = reg_copy2d_client(pgraph, stream_fn, 
                                                STALE_MASK|V_CENTRIC);
        
        sstream_t<T>* sstreamh = reg_sstream_view(pgraph, stream_bfs2d, 
                                    STALE_MASK|V_CENTRIC|C_THREAD);
        stream_fn(clienth);
        void* ret;
        pthread_join(sstreamh->thread, &ret);
    }
}
            
template <class T>
void snb_copy2d_bfs(const string& idir, const string& odir, 
                              typename callback<T>::sfunc stream_fn, 
                              typename callback<T>::sfunc copy_fn)
{
    //int i = 0 ; while (i < 100000) { usleep(100); ++i;}
    plaingraph_manager_t<T> manager;
    assert(_part_count*_part_count + 1 == _numtasks);
    vid_t rem = _global_vcount%_part_count;
    _global_vcount = rem? _global_vcount + _part_count - rem :_global_vcount;
    //cout << "global " <<  _global_vcount << endl; 

    // extract the original group handle
    create_analytics_comm();
    create_2d_comm();
    if (_rank == 0) {
        manager.schema(_dir);
        pgraph_t<T>* pgraph = manager.get_plaingraph();
        //do some setup for plain graphs
        manager.setup_graph(_global_vcount, eSNB);    
        
        //create scopy_server
        copy2d_server_t<T>* copyh = reg_copy2d_server(pgraph, copy_fn, 
                                            STALE_MASK|V_CENTRIC|C_THREAD);
        manager.prep_graph_edgelog(idir, odir);
        void* ret;
        pthread_join(copyh->thread, &ret);

    } else {
        if (_dir == 0) {
            manager.schema(2);
            _edge_count += _edge_count;
        } else {
            manager.schema(_dir);
        }
        pgraph_t<T>* pgraph = manager.get_plaingraph();

        //do some setup for plain graphs
        vid_t local_vcount = _global_vcount/_part_count;
        manager.setup_graph(local_vcount, eSNB);
        g->create_threads(true, false);
        
        copy2d_client_t<T>* clienth = reg_copy2d_client(pgraph, stream_fn, 
                                                STALE_MASK|V_CENTRIC);
        
        sstream_t<T>* sstreamh = reg_sstream_view(pgraph, snb_bfs2d, 
                                    STALE_MASK|V_CENTRIC|C_THREAD);
        stream_fn(clienth);
        void* ret;
        pthread_join(sstreamh->thread, &ret);
    }
}

template <class T>
void serial_scopy2d_bfs(const string& idir, const string& odir,
               typename callback<T>::sfunc stream_fn,
               typename callback<T>::sfunc scopy_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    pgraph_t<T>* pgraph = manager.get_plaingraph();
    assert(_part_count*_part_count + 1 == _numtasks);
    vid_t rem = _global_vcount%_part_count;
    _global_vcount = rem? _global_vcount + _part_count - rem :_global_vcount;
    //cout << "global " <<  _global_vcount << endl; 

    // extract the original group handle
    create_analytics_comm();
    create_2d_comm();
    if (_rank == 0) {
        //do some setup for plain graphs
        manager.setup_graph(_global_vcount);    
        
        //create scopy_server
        scopy2d_server_t<T>* scopyh = reg_scopy2d_server(pgraph, scopy_fn, 
                                            STALE_MASK|V_CENTRIC|C_THREAD);
        manager.prep_graph(idir, odir);
        void* ret;
        pthread_join(scopyh->thread, &ret);

    } else {
        //do some setup for plain graphs
        vid_t local_vcount = _global_vcount/_part_count;
        manager.setup_graph(local_vcount);    
        //create scopy_client
        scopy2d_client_t<T>* sclienth = reg_scopy2d_client(pgraph, stream_fn, 
                                                STALE_MASK|V_CENTRIC);
        
        stream_fn(sclienth);
    }
}

template <class T>
void serial_scopy1d_bfs(const string& idir, const string& odir,
               typename callback<T>::sfunc stream_fn,
               typename callback<T>::sfunc scopy_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    pgraph_t<T>* pgraph = manager.get_plaingraph();
    
    /*
    int  i = 0;
    while (i < 15000) {
        usleep(1000);
        ++i;
    }*/
    vid_t rem = _global_vcount%_part_count;
    _global_vcount = rem? _global_vcount + _part_count - rem :_global_vcount;
   
    // extract the original group handle
    create_analytics_comm();
    
    if (_rank == 0) {
        //do some setup for plain graphs
        manager.setup_graph(_global_vcount);    
        
        //create scopy_server
        scopy1d_server_t<T>* scopyh = reg_scopy1d_server(pgraph, scopy_fn, 
                                            STALE_MASK|V_CENTRIC|C_THREAD);
        //CorePin(0);
        manager.prep_graph(idir, odir);
        void* ret;
        pthread_join(scopyh->thread, &ret);

    } else {
        //do some setup for plain graphs
        vid_t local_vcount = (_global_vcount/(_numtasks - 1));
        vid_t v_offset = (_rank - 1)*local_vcount;
        if (_rank == _numtasks - 1) {//last rank
            local_vcount = _global_vcount - v_offset;
        }
        manager.setup_graph(local_vcount);    
        //create scopy_client
        scopy1d_client_t<T>* sclienth = reg_scopy1d_client(pgraph, stream_fn, 
                                                STALE_MASK|V_CENTRIC);
        stream_fn(sclienth);
    
    }
}

int dist_init(int argc, char* argv[])
{
    //MPI_Init(&argc,&argv);
    int required = MPI_THREAD_MULTIPLE;
    int provided;
    MPI_Init_thread(&argc, &argv, required, &provided);

    if (provided != required) {
        cout << "Your MPI does not have MPI_THREAD_MULTIPLE support" << endl;
        assert(0);
        return 1;
    }

    MPI_Comm_size(MPI_COMM_WORLD, &_numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &_rank);

    return 0;
}

void dist_test(vid_t v_count1, const string& idir, const string& odir, int job)
{
    switch (job) {
        case 0:
            serial_scopy2d_bfs<dst_id_t>(idir, odir, scopy2d_client, scopy2d_server);
            break;
        case 1:
            serial_scopy1d_bfs<dst_id_t>(idir, odir, scopy1d_client, scopy1d_server);
            break;
        case 2:
            serial_scopy_bfs<dst_id_t>(idir, odir, scopy_client, scopy_server);
            break;

        case 10://edge centric
            serial_copy2d_bfs<dst_id_t>(idir, odir, copy2d_client, copy2d_server);
            break;
        case 12://edge centric
            snb_copy2d_bfs<dst_id_t>(idir, odir, copy2d_client, copy2d_server);
            break;
        default:
            break;
    }
}

#endif        
