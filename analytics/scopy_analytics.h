#pragma once

#ifdef _MPI
#include "graph_view.h"

template<class T>
void scopy_client(gview_t<T>* viewh)
{
    cout << "Client Started" << endl;
    scopy_client_t<T>* sstreamh = dynamic_cast<scopy_client_t<T>*>(viewh);
    pgraph_t<T>* pgraph  = sstreamh->pgraph;
    vid_t        v_count = sstreamh->get_vcount();
    
    sstream_t<T>* snaph = reg_sstream_view(pgraph, 0, 
                            STALE_MASK|V_CENTRIC, 0);
    
    //cout << "starting BFS" << endl;
    uint8_t* status = (uint8_t*)malloc(v_count*sizeof(uint8_t));
    memset(status, 255, v_count);

    sid_t    root   = 1;
    uint8_t  level  = 0;
    sid_t  frontier = 0;
    status[root] = level;
    int update_count = 0;
    
    double start = mywtime();
    double end = 0;

    usleep(10000);
    
    while (pgraph->get_snapshot_marker() < _edge_count) {
        cout << "Client " << pgraph->get_snapshot_marker() << endl;
        //update the sstream view
        if (eOK != sstreamh->update_view()) {
            usleep(100);
        }
        snaph->update_view();
        ++update_count;
    
        cout << "BFS Started" << endl;

        bool top_down = true;
        start = mywtime();
	    do {
		    frontier = 0;
		    #pragma omp parallel reduction(+:frontier)
		    {
            sid_t sid;
            uint8_t level = 0;
            degree_t nebr_count = 0;
            degree_t prior_sz = 65536;
            T* local_adjlist = (T*)malloc(prior_sz*sizeof(T));

            #pragma omp for nowait
            for (vid_t v = 0; v < v_count; v++) {
                if(false == snaph->has_vertex_changed_out(v) || status[v] == 255) continue;
                snaph->reset_vertex_changed_out(v);
                nebr_count = snaph->get_degree_out(v);
                if (nebr_count == 0) {
                    continue;
                } else if (nebr_count > prior_sz) {
                    prior_sz = nebr_count;
                    free(local_adjlist);
                    local_adjlist = (T*)malloc(prior_sz*sizeof(T));
                }

                level = status[v];
                snaph->get_nebrs_out(v, local_adjlist);

                for (degree_t i = 0; i < nebr_count; ++i) {
                    sid = get_sid(local_adjlist[i]);
                    if (status[sid] > level + 1) {
                        status[sid] = level + 1;
                        snaph->set_vertex_changed_out(sid);
                        ++frontier;
                        //cout << " " << sid << endl;
                    }
                }
            }
            }
        } while (frontier);
		
        end = mywtime();
        cout << "BFS Time at Batch " << update_count << " = " << end - start << endl;
    } 
    
    for (int l = 0; l < 7; ++l) {
        vid_t vid_count = 0;
        #pragma omp parallel for reduction (+:vid_count) 
        for (vid_t v = 0; v < v_count; ++v) {
            if (status[v] == l) ++vid_count;
        }
        cout << " Level = " << l << " count = " << vid_count << endl;
    }
    cout << "update_count = " << update_count << endl;
}

template<class T>
void scopy_server(gview_t<T>* viewh)
{
    scopy_server_t<T>* sstreamh = dynamic_cast<scopy_server_t<T>*>(viewh);
    pgraph_t<T>* pgraph  = sstreamh->pgraph;
    vid_t        v_count = sstreamh->get_vcount();
    
    index_t beg_marker = 0;//(_edge_count >> 1);
    index_t rest_edges = _edge_count - beg_marker;
    index_t do_count = residue;
    index_t batch_size = rest_edges/do_count;
    index_t marker = 0; 
    int update_count = 1;
    
    cout << "Copy Started" << endl;
    
    while (pgraph->get_archived_marker() < _edge_count) {
        marker = beg_marker + update_count*batch_size;
        if (update_count == do_count) marker = _edge_count;

        pgraph->create_marker(marker);
        pgraph->create_snapshot();

        cout << "Created snapshot at" << marker << endl;
        
        //update the sstream view
        if (eOK != sstreamh->update_view()) {
            usleep(100);
        }
        ++update_count;
    }
    cout << "update_count = " << update_count << endl;
}
#endif
