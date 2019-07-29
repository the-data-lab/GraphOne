#pragma once
#include "graph_view.h"

template<class T>
void scopy1d_serial_bfs(gview_t<T>* viewh)
{
    cout << " Rank " << _rank <<" SCopy1D Client Started" << endl;
    scopy1d_client_t<T>* sstreamh = dynamic_cast<scopy1d_client_t<T>*>(viewh);
    pgraph_t<T>* pgraph  = sstreamh->pgraph;
    vid_t        v_count = sstreamh->global_vcount;

    scopy1d_client_t<T>* snaph = sstreamh;
    
    uint8_t* status = (uint8_t*)malloc(v_count*sizeof(uint8_t));
    memset(status, 255, v_count);

    sid_t    root   = 1;
    index_t  frontier = 0;
    status[root] = 0;
    int update_count = 0;
    
    double start = mywtime();
    double end = 0;
    
    while (pgraph->get_snapshot_marker() < _edge_count) {
        //update the sstream view
        if (eOK != sstreamh->update_view()) {
            usleep(100);
            continue;
        }
        ++update_count;
    
        start = mywtime();
	    do {
		    frontier = 0;
		    #pragma omp parallel num_threads(THD_COUNT) reduction(+:frontier)
		    {
            sid_t sid;
            vid_t vid;
            uint8_t level = 0;
            degree_t nebr_count = 0;
            degree_t prior_sz = 65536;
            T* local_adjlist = (T*)malloc(prior_sz*sizeof(T));

            #pragma omp for nowait
            for (vid_t v = 0; v < snaph->v_count; v++) 
            {
                vid = v + snaph->v_offset;
                if(false == snaph->has_vertex_changed_out(vid) || status[vid] == 255) continue;
                snaph->reset_vertex_changed_out(vid);

                nebr_count = snaph->get_degree_out(v);
                if (nebr_count == 0) {
                    continue;
                } else if (nebr_count > prior_sz) {
                    prior_sz = nebr_count;
                    free(local_adjlist);
                    local_adjlist = (T*)malloc(prior_sz*sizeof(T));
                }

                level = status[vid];
                snaph->get_nebrs_out(v, local_adjlist);

                for (degree_t i = 0; i < nebr_count; ++i) {
                    sid = get_nebr(local_adjlist, i);
                    if (status[sid] > level + 1) {
                        status[sid] = level + 1;
                        snaph->set_vertex_changed_out(sid);
                        ++frontier;
                        //cout << " " << sid << endl;
                    }
                }
            }
            }
            //Finalize the iteration
            MPI_Allreduce(MPI_IN_PLACE, status, v_count, MPI_UINT8_T, MPI_MIN, _analytics_comm);
            MPI_Allreduce(MPI_IN_PLACE, snaph->bitmap_out->get_start(), snaph->bitmap_out->get_size(),
                          MPI_UINT64_T, MPI_BOR, _analytics_comm);
            MPI_Allreduce(MPI_IN_PLACE, &frontier, 1, MPI_UINT64_T, MPI_SUM, _analytics_comm);
        } while (frontier);
		
        end = mywtime();
        cout << " BFS Time at Batch " << update_count << " = " << end - start << endl;
        
    } 
   
    if (_rank == 1) { 
        for (int l = 0; l < 10; ++l) {
            vid_t vid_count = 0;
            #pragma omp parallel for reduction (+:vid_count) 
            for (vid_t v = 0; v < v_count; ++v) {
                if (status[v] == l) ++vid_count;
            }
            cout << " Level = " << l << " count = " << vid_count << endl;
        }
        cout << " update_count = " << update_count << endl;
    }
}

template<class T>
void scopy1d_server(gview_t<T>* viewh)
{
    scopy1d_server_t<T>* sstreamh = dynamic_cast<scopy1d_server_t<T>*>(viewh);
    pgraph_t<T>* pgraph  = sstreamh->pgraph;
    vid_t        v_count = sstreamh->get_vcount();
    
    /*index_t beg_marker = 0;//(_edge_count >> 1);
    index_t rest_edges = _edge_count - beg_marker;
    index_t do_count = residue;
    index_t batch_size = rest_edges/do_count;
    index_t marker = 0; 
    */
    int update_count = 1;
    
    cout << " SCopy1d Server Started" << endl;
    
    while (sstreamh->get_snapmarker() < _edge_count) {
        /*
        marker = beg_marker + update_count*batch_size;
        if (update_count == do_count) marker = _edge_count;

        pgraph->create_marker(marker);
        pgraph->create_snapshot();

        cout << " Created snapshot at " << marker << endl;
        */
        //update the sstream view
        if (eOK != sstreamh->update_view()) {
            usleep(100);
            continue;
        }
        ++update_count;
    }
    cout << " RANK" << _rank 
         << " update_count = " << update_count << endl;
}
/*
template<class T>
void scopy1d_serial_bfs(gview_t<T>* viewh)
{
    cout << " Rank " << _rank <<" SCopy1D Client Started" << endl;
    scopy1d_client_t<T>* sstreamh = dynamic_cast<scopy1d_client_t<T>*>(viewh);
    pgraph_t<T>* pgraph  = sstreamh->pgraph;
    vid_t        v_count = sstreamh->get_vcount();

    scopy1d_client_t<T>* snaph = sstreamh;
    
    uint8_t* status = (uint8_t*)malloc(v_count*sizeof(uint8_t));
    memset(status, 255, v_count);

    sid_t    root   = 1;
    index_t  frontier = 0;
    status[root] = 0;
    int update_count = 0;
    
    double start = mywtime();
    double end = 0;
    
    usleep(10000);

    
    while (pgraph->get_snapshot_marker() < _edge_count) {
        //update the sstream view
        if (eOK != sstreamh->update_view()) {
            usleep(100);
        }
        //snaph->update_view();
        ++update_count;
        cout << " Rank " << _rank << " "<< snaph->v_offset << ":" << snaph->local_vcount << endl;
    
        start = mywtime();
        uint8_t level = 0;

	    do {
		    frontier = 0;
		    //#pragma omp parallel num_threads(THD_COUNT) reduction(+:frontier)
		    {
            sid_t sid;
            degree_t nebr_count = 0;
            degree_t prior_sz = 65536;
            T* local_adjlist = (T*)malloc(prior_sz*sizeof(T));

            //#pragma omp for nowait
            for (vid_t v = snaph->v_offset; v < snaph->v_offset + snaph->local_vcount; v++) 
            {
                if(status[v] != level) continue;

                nebr_count = snaph->get_degree_out(v);
                if (level == 1) {
                    cout << " Rank " << _rank << " " << v << ":" << nebr_count << endl;
                }
                if (nebr_count == 0) {
                    continue;
                } else if (nebr_count > prior_sz) {
                    prior_sz = nebr_count;
                    free(local_adjlist);
                    local_adjlist = (T*)malloc(prior_sz*sizeof(T));
                }

                snaph->get_nebrs_out(v, local_adjlist);

                for (degree_t i = 0; i < nebr_count; ++i) {
                    sid = get_nebr(local_adjlist, i);
                    if (status[sid] == 255) {
                        status[sid] = level + 1;
                        snaph->set_vertex_changed_out(sid);
                        ++frontier;
                        if (level == 0) {
                            cout << " " << sid << endl;
                        }
                    }
                }
            }
            }
            cout << " level " << (int)level << " "  << frontier << endl;
            //Finalize the iteration
            MPI_Allreduce(MPI_IN_PLACE, status, v_count, MPI_UINT8_T, MPI_MIN, _analytics_comm);
            //MPI_Allreduce(MPI_IN_PLACE, snaph->bitmap_out->get_start(), snaph->bitmap_out->get_size(),
            //              MPI_UINT64_T, MPI_BOR, _analytics_comm);
            MPI_Allreduce(MPI_IN_PLACE, &frontier, 1, MPI_UINT64_T, MPI_SUM, _analytics_comm);
            ++level;
        } while (frontier);
		
        end = mywtime();
        cout << " BFS Time at Batch " << update_count << " = " << end - start << endl;
    } 
   
    if (_rank == 1) { 
        for (int l = 0; l < 10; ++l) {
            vid_t vid_count = 0;
            #pragma omp parallel for reduction (+:vid_count) 
            for (vid_t v = 0; v < v_count; ++v) {
                if (status[v] == l) ++vid_count;
            }
            cout << " Level = " << l << " count = " << vid_count << endl;
        }
        cout << " update_count = " << update_count << endl;
        
    }
}
*/
