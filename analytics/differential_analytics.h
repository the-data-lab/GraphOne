#pragma once
#include <omp.h>
#include "graph_view.h"
#include "sstream_analytics.h"

template <class T>
void diff_streambfs(gview_t<T>* viewh)
{
    //
    double start = mywtime ();
    double end = 0;
    int update_count = 0;
   
    init_bfs(viewh);
    sstream_t<T>* sstreamh = dynamic_cast<sstream_t<T>*>(viewh);
    vid_t v_count = sstreamh->get_vcount();


    while (sstreamh->get_snapmarker() < _edge_count) {
        if (eOK != sstreamh->update_view()) continue;
        if (update_count == 0) {
            do_streambfs(sstreamh);
        } else {
            do_diffbfs(sstreamh);
        }
        ++update_count;
    }
    print_bfs(viewh);
    cout << " update_count = " << update_count 
         << " snapshot count = " << sstreamh->get_snapid() << endl;
}

template <class T> 
void do_diffbfs(sstream_t<T>* viewh) 
{
    uint8_t* status = (uint8_t*)viewh->get_algometa();
    vid_t   v_count = viewh->get_vcount();
    uint8_t level = 1;//start from second iteration

    index_t frontier = 0;
    do {
        frontier = 0;
        //double start = mywtime();
        #pragma omp parallel num_threads (THD_COUNT) reduction(+:frontier)
        {
        sid_t sid;
        uint8_t backup_level = 0;
        uint8_t new_level = 255;
        degree_t nebr_count = 0;
        degree_t prior_sz = 65536;
        T* local_adjlist = (T*)malloc(prior_sz*sizeof(T));

        #pragma omp for nowait
        for (vid_t v = 0; v < v_count; v++) {
            if(false == viewh->has_vertex_changed_out(v) || status[v] < level ) continue;
            
            backup_level = status[v];
            new_level =  255;

            //handle the in-edges
            nebr_count = viewh->get_degree_in(v);
            if (nebr_count == 0) {
                continue;
            } else if (nebr_count > prior_sz) {
                prior_sz = nebr_count;
                free(local_adjlist);
                local_adjlist = (T*)malloc(prior_sz*sizeof(T));
            }

            viewh->get_nebrs_in(v, local_adjlist);

            for (degree_t i = 0; i < nebr_count; ++i) {
                sid = get_sid(local_adjlist[i]);
                new_level = min(new_level, status[sid]);
            }

            if (new_level+1 == level && backup_level >= level) {//upgrade case
                status[v] = level;
                viewh->reset_vertex_changed_out(v);
                ++frontier;
            } else if (new_level+1 != level &&  backup_level == level) { //infinity case
                status[v] == 255;
                viewh->reset_vertex_changed_out(v);
                ++frontier;
            } else if (backup_level > level && backup_level != 255) {
                ++frontier;
                continue;
            } else {
                continue;
            }

            nebr_count = viewh->get_degree_out(v);
            if (nebr_count == 0) {
                continue;
            } else if (nebr_count > prior_sz) {
                prior_sz = nebr_count;
                free(local_adjlist);
                local_adjlist = (T*)malloc(prior_sz*sizeof(T));
            }

            viewh->get_nebrs_out(v, local_adjlist);

            for (degree_t i = 0; i < nebr_count; ++i) {
                sid = get_sid(local_adjlist[i]);
                viewh->set_vertex_changed_out(sid);
            }
        }
        }
        ++level;
    } while (frontier);
}
