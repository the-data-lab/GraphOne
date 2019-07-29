#pragma once
#include "graph_view.h"
#include "communicator.h"

template<class T>
void copy2d_serial_bfs(gview_t<T>* viewh)
{
    cout << " Rank " << _rank <<" Copy2d Client Started" << endl;
    copy2d_client_t<T>* snaph = dynamic_cast<copy2d_client_t<T>*>(viewh);
    pgraph_t<T>* pgraph  = snaph->pgraph;
    vid_t v_count = snaph->get_vcount();
    int update_count = 0;
    
    sstream_t<T>* sstreamh = reg_sstream_view(pgraph, stream2d_bfs, 
                                    STALE_MASK|V_CENTRIC|C_THREAD);
    
    while (snaph->get_snapmarker() < _edge_count) {
        //update the sstream view
        if (eOK != snaph->update_view()) {
            usleep(100);
            continue;
        }
        ++update_count;
    }
    snaph->pgraph->create_marker(0);
    _edge_count = snaph->pgraph->blog->blog_head; 
    void* ret;
    pthread_join(sstreamh->thread, &ret);
}

template<class T>
void copy2d_server(gview_t<T>* viewh)
{
    copy2d_server_t<T>* sstreamh = dynamic_cast<copy2d_server_t<T>*>(viewh);
    pgraph_t<T>* pgraph  = sstreamh->pgraph;
    vid_t        v_count = sstreamh->get_vcount();
    
    int update_count = 1;
    
    cout << " Copy2d Server Started" << endl;
    
    while (sstreamh->get_snapmarker() < _edge_count) {
        //update the sstream view
        if (eOK != sstreamh->update_view()) {
            usleep(100);
            continue;
        }
        ++update_count;
    }
    //cout << " RANK" << _rank << " update_count = " << update_count << endl;
}
