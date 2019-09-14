#pragma once

#ifdef _MPI 

#include "graph_view.h"
#include "communicator.h"

template<class T>
void copy2d_client(gview_t<T>* viewh)
{
    cout << " Rank " << _rank <<" Copy2d Client Started" << endl;
    copy2d_client_t<T>* snaph = dynamic_cast<copy2d_client_t<T>*>(viewh);
    pgraph_t<T>* pgraph  = snaph->pgraph;
    int update_count = 0;
    
    while (snaph->get_snapmarker() < _edge_count) {
        //update the sstream view
        if (eOK != snaph->update_view()) {
            usleep(100);
            continue;
        }
        ++update_count;
    }
    pgraph->waitfor_archive();
    _edge_count = pgraph->global_snapmarker; 
}

template<class T>
void copy2d_server(gview_t<T>* viewh)
{
    copy2d_server_t<T>* sstreamh = dynamic_cast<copy2d_server_t<T>*>(viewh);
    pgraph_t<T>* pgraph  = sstreamh->pgraph;
    
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
    cout << " RANK:" << _rank << " update_count = " << update_count << endl;
}
#endif
