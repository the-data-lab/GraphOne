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
    index_t check_sz = window_sz;
    if (viewh->is_udir()) check_sz += window_sz;

    while (viewh->get_snapmarker() < _edge_count) {
        if (eOK != viewh->update_view()) continue;
        ++update_count;
        edge_count = 0;
        
        for (vid_t v = 0; v < v_count; ++v) {
            edge_count += viewh->get_degree_out(v);
        }
        assert(edge_count == check_sz);
    }

    cout << "wsstream: update_count " << update_count << ":" << edge_count << endl;
}
