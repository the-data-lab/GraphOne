#pragma once
#include <omp.h>
#include "graph_view.h"

template <class T>
void wsstream_example(gview_t<T>* view) 
{
    wsstream_t<T>* viewh = dynamic_cast<wsstream_t<T>*>(view);
    vid_t v_count = viewh->get_vcount();
    index_t window_sz = viewh->window_sz;
    int update_count = 0;
    index_t edge_count = 0;
    degree_t degree;
    index_t check_sz = window_sz;
    if (viewh->is_udir()) check_sz += window_sz;
    
    degree_t sz = 1024;
    T* adj_list = (T*)malloc(sz*sizeof(T));
    assert(adj_list);

    //sleep(1);
    while (viewh->get_snapmarker() < _edge_count) {
        if (eOK != viewh->update_view()) {
            usleep(10);
            continue;
        }
        ++update_count;
        edge_count = 0;
        
        for (vid_t v = 0; v < v_count; ++v) {
            degree = viewh->get_degree_out(v);
            if (degree == 0) continue;
            assert (degree < check_sz);
            edge_count += degree;
            if (degree > sz) {
                //assert(0);
                sz = degree;
                adj_list = (T*)realloc(adj_list, sz*sizeof(T));
                assert(adj_list);
            }
            //viewh->get_nebrs_out(v, adj_list);
        }
        assert(edge_count == check_sz);
        //cout << check_sz << ":" << edge_count << endl;
    }

    cout << "wsstream: update_count " << update_count << ":" << viewh->get_snapid() << endl;//edge_count << endl;
}
