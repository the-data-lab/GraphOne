#pragma once
#include "graph_view.h"
#include "communicator.h"

template<class T>
void stream_bfs1d(gview_t<T>* viewh);
template<class T>
void stream_bfs2d(gview_t<T>* viewh);

index_t bfs2d_finalize(Bitmap* bitmap2, uint8_t* lstatus, uint8_t* rstatus,
                     index_t frontier, vid_t v_count)
{
    int col_root = _row_rank;
    int row_root = _col_rank;
    
    //rank should be root
    if (_col_rank == col_root) {
        MPI_Reduce(MPI_IN_PLACE, bitmap2->get_start(), bitmap2->get_size(),
                  MPI_UINT64_T, MPI_BOR, col_root, _col_comm);
        MPI_Reduce(MPI_IN_PLACE, rstatus, v_count,
                  MPI_UINT8_T, MPI_MIN, col_root, _col_comm);
    } else {
        MPI_Reduce(bitmap2->get_start(), bitmap2->get_start(), bitmap2->get_size(),
                  MPI_UINT64_T, MPI_BOR, col_root, _col_comm);
        MPI_Reduce(rstatus, rstatus, v_count,
                  MPI_UINT8_T, MPI_MIN, col_root, _col_comm);
    }

    MPI_Bcast(bitmap2->get_start(), bitmap2->get_size(), MPI_UINT64_T, row_root, _row_comm);
    MPI_Bcast(lstatus, v_count, MPI_UINT8_T, row_root, _row_comm);
    
    MPI_Allreduce(MPI_IN_PLACE, &frontier, 1, MPI_UINT64_T, MPI_SUM, _analytics_comm);
    return frontier;
}

index_t bfs1d_finalize(Bitmap* bitmap, uint8_t* status, index_t& frontier, vid_t v_count)
{
    //Finalize the iteration
    MPI_Allreduce(MPI_IN_PLACE, status, v_count, MPI_UINT8_T, MPI_MIN, _analytics_comm);
    MPI_Allreduce(MPI_IN_PLACE, bitmap->get_start(), bitmap->get_size(), MPI_UINT64_T,
                  MPI_BOR, _analytics_comm);
    MPI_Allreduce(MPI_IN_PLACE, &frontier, 1, MPI_UINT64_T, MPI_SUM, _analytics_comm);
    
    return frontier;
}

template<class T>
void print_bfs2d_summary(gview_t<T>* viewh, uint8_t* status)
{
    sstream_t<T>* sstreamh = dynamic_cast<sstream_t<T>*>(viewh);
    vid_t v_count = sstreamh->get_vcount();
    
    for (int l = 0; l < 10; ++l) {
        uint64_t vid_count = 0;
        #pragma omp parallel for num_threads(THD_COUNT) reduction (+:vid_count) 
        for (vid_t v = 0; v < v_count; ++v) { 
            if (status[v] == l) ++vid_count;
        }
        MPI_Allreduce(MPI_IN_PLACE, &vid_count, 1, MPI_UINT64_T, MPI_SUM, _col_comm);
            
        if (_analytics_rank == 0 && vid_count > 0) { 
            cout << " Level = " << l << " count = " << vid_count << endl;
        }
    }
}

void print_bfs1d_summary(vid_t v_count, uint8_t* status)
{
    if (_analytics_rank == 0) { 
        for (int l = 0; l < 10; ++l) {
            vid_t vid_count = 0;
            #pragma omp parallel for reduction (+:vid_count) 
            for (vid_t v = 0; v < v_count; ++v) {
                if (status[v] == l) ++vid_count;
            }
            if (vid_count > 0) {
                cout << " Level = " << l << " count = " << vid_count << endl;
            }
        }
    }
}

template<class T>
void do_stream_bfs1d(gview_t<T>* viewh, uint8_t* status)
{
    //sstream_t<T>* sstreamh = dynamic_cast<sstream_t<T>*>(viewh);
    scopy1d_client_t<T>* snaph = dynamic_cast<scopy1d_client_t<T>*>(viewh);
    vid_t        v_count = snaph->global_vcount;
    index_t frontier = 0;
    
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
        bfs1d_finalize(snaph->bitmap_out, status, frontier, v_count);
    } while (frontier);
}

template<class T>
void do_stream_bfs2d(gview_t<T>* viewh, uint8_t* lstatus, uint8_t* rstatus, Bitmap* bitmap2)
{
    index_t  frontier = 0;
    sstream_t<T>* sstreamh = dynamic_cast<sstream_t<T>*>(viewh);
    Bitmap* bitmap1 = sstreamh->bitmap_out;
    vid_t v_count = sstreamh->get_vcount();
    
    do {
        frontier = 0;
        #pragma omp parallel num_threads(THD_COUNT) reduction(+:frontier)
        {
        sid_t sid;
        vid_t vid;
        uint8_t level = 0;
        degree_t nebr_count = 0;
        header_t<T> header; 
        T dst;
        #pragma omp for nowait
        for (vid_t v = 0; v < v_count; v++) 
        {
            vid = v;
            if(false == bitmap1->get_bit(v) || lstatus[vid] == 255) {
                bitmap1->reset_bit(v);
                continue;
            }
            bitmap1->reset_bit(v);
            level = lstatus[vid];
            nebr_count = sstreamh->start_out(v, header);
            for (degree_t i = 0; i < nebr_count; ++i) {
                sstreamh->next(header, dst);
                sid = get_sid(dst);
                if (rstatus[sid] > level + 1) {
                    rstatus[sid] = level + 1;
                    bitmap2->set_bit(sid);//dest
                    ++frontier;
                }
            }
        }
        }
        //Finalize the iteration
        frontier = bfs2d_finalize(bitmap2, lstatus, rstatus, frontier, v_count);
        //Swap the bitmaps
        bitmap1->swap(bitmap2);
    } while (frontier);
}

template<class T>
void stream_bfs2d(gview_t<T>* viewh)
{
    sstream_t<T>* sstreamh = dynamic_cast<sstream_t<T>*>(viewh);
    pgraph_t<T>* pgraph  = sstreamh->pgraph;
    vid_t v_count = sstreamh->get_vcount();
    
    sid_t    root   = 1;
    Bitmap bitmap(v_count);
    int update_count = 0;
    
    int row_id = (_analytics_rank)/_part_count;
    int col_id = (_analytics_rank)%_part_count;
    vid_t v_offset = row_id*(_global_vcount/_part_count);
    vid_t dst_offset = col_id*(_global_vcount/_part_count);
    
    uint8_t* lstatus = (uint8_t*)malloc(v_count*sizeof(uint8_t));
    memset(lstatus, 255, v_count);
    uint8_t* rstatus = lstatus;
    
    if (root >= v_offset && root < v_offset + v_count) {
        lstatus[root - v_offset] = 0;
    }

    if (_row_rank != _col_rank) {
        rstatus = (uint8_t*)malloc(v_count*sizeof(uint8_t));
        memset(rstatus, 255, v_count);
        if (root >= dst_offset && root < dst_offset + v_count) {
            rstatus[root - dst_offset] = 0;
        }
    }

    double start = mywtime();
    double end = 0;
    
    while (sstreamh->get_snapmarker() < _edge_count) {
        //update the sstream view
        if (eOK != sstreamh->update_view()) {
            usleep(100);
            continue;
        }
        //cout << _rank << ": update count =" << update_count << endl;
        ++update_count;
	    do_stream_bfs2d(sstreamh, lstatus, rstatus, &bitmap);
    } 
    
    end = mywtime();
    print_bfs2d_summary(sstreamh, lstatus); 
    if(_row_rank == 0 && _col_rank == 0)
    cout << "BFS Batches = " << update_count << " Time = " << end - start << endl;
}


template<class T>
void stream_bfs1d(gview_t<T>* viewh)
{
    sstream_t<T>* sstreamh = dynamic_cast<sstream_t<T>*>(viewh);
    pgraph_t<T>* pgraph  = sstreamh->pgraph;
    vid_t        v_count = sstreamh->global_vcount;

    uint8_t* status = (uint8_t*)malloc(v_count*sizeof(uint8_t));
    memset(status, 255, v_count);

    sid_t    root   = 1;
    index_t  frontier = 0;
    status[root] = 0;
    int update_count = 0;
    
    double start = mywtime();
    double end = 0;
    
    while (sstreamh->get_snapmarker() < _edge_count) {
        //update the sstream view
        if (eOK != sstreamh->update_view()) {
            usleep(100);
            continue;
        }
        ++update_count;
        do_stream_bfs1d(sstreamh, status);
		
        end = mywtime();
        //cout << " BFS Time at Batch " << update_count << " = " << end - start << endl;
    } 
    print_bfs1d_summary(v_count, status);
    cout << " update_count = " << update_count << endl;
}
