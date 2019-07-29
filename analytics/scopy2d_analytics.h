#pragma once
#include "graph_view.h"
#include "communicator.h"

index_t bfs_finalize(Bitmap* bitmap2, uint8_t* lstatus, uint8_t* rstatus,
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

template<class T>
void do_stream_bfs(gview_t<T>* viewh, uint8_t* lstatus, uint8_t* rstatus, Bitmap* bitmap2)
{
    index_t  frontier = 0;
    sstream_t<T>* snaph = dynamic_cast<sstream_t<T>*>(viewh);
    Bitmap* bitmap1 = snaph->bitmap_out;
    vid_t v_count = snaph->get_vcount();
    
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
            nebr_count = snaph->start_out(v, header);
            for (degree_t i = 0; i < nebr_count; ++i) {
                snaph->next(header, dst);
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
        frontier = bfs_finalize(bitmap2, lstatus, rstatus, frontier, v_count);
        //Swap the bitmaps
        bitmap1->swap(bitmap2);
    } while (frontier);
}

template<class T>
void print_bfs_summary(gview_t<T>* viewh, uint8_t* status)
{
    sstream_t<T>* snaph = dynamic_cast<sstream_t<T>*>(viewh);
    vid_t v_count = snaph->get_vcount();
    
    for (int l = 0; l < 10; ++l) {
        uint64_t vid_count = 0;
        #pragma omp parallel for num_threads(THD_COUNT) reduction (+:vid_count) 
        for (vid_t v = 0; v < v_count; ++v) { 
            if (status[v] == l) ++vid_count;
            /*{
                ++vid_count;
                if ((_rank == 1 || _rank == 3) && l < 2) {
                    cout << _rank << ":" << v << endl;
                }
            }*/
            
        }
        MPI_Allreduce(MPI_IN_PLACE, &vid_count, 1, MPI_UINT64_T, MPI_SUM, _col_comm);
            
        if (_rank == 1 && vid_count > 0) { 
            cout << " Level = " << l << " count = " << vid_count << endl;
        }
    }
}

template<class T>
void stream2d_bfs(gview_t<T>* viewh)
{
    sstream_t<T>* snaph = dynamic_cast<sstream_t<T>*>(viewh);
    pgraph_t<T>* pgraph  = snaph->pgraph;
    vid_t v_count = snaph->get_vcount();
    
    sid_t    root   = 1;
    Bitmap bitmap(v_count);
    int update_count = 0;
    
    int row_id = (_rank - 1)/_part_count;
    int col_id = (_rank - 1)%_part_count;
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
    
    while (snaph->get_snapmarker() < _edge_count) {
        //update the sstream view
        if (eOK != snaph->update_view()) {
            usleep(100);
            continue;
        }
        ++update_count;
	    do_stream_bfs(snaph, lstatus, rstatus, &bitmap);
    } 
    
    end = mywtime();
    print_bfs_summary(snaph, lstatus); 
    if(_row_rank == 0 && _col_rank == 0)
    cout << "BFS Batches = " << update_count << " Time = " << end - start << endl;
    
}


template<class T>
void scopy2d_serial_bfs(gview_t<T>* viewh)
{
    cout << " Rank " << _rank <<" SCopy2d Client Started" << endl;
    scopy2d_client_t<T>* snaph = dynamic_cast<scopy2d_client_t<T>*>(viewh);
    pgraph_t<T>* pgraph  = snaph->pgraph;
    vid_t v_count = snaph->get_vcount();
    int update_count = 0;
    
    sstream_t<T>* sstreamh = reg_sstream_view(pgraph, stream2d_bfs, 
                                    STALE_MASK|V_CENTRIC|C_THREAD);
    
    /*
    sid_t    root   = 1;
    vid_t v_offset = _row_rank*(_global_vcount/_part_count);
    vid_t dst_offset = _col_rank*(_global_vcount/_part_count);
    Bitmap bitmap(v_count);
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
    */
    
    while (snaph->get_snapmarker() < _edge_count) {
        //update the sstream view
        while (eOK != snaph->update_view()) {
            usleep(100);
        }
        ++update_count;
	    //do_stream_bfs(snaph, lstatus, rstatus, &bitmap);
    } 
    /*
    end = mywtime();
    print_bfs_summary(snaph, lstatus); 
    if(_row_rank == 0 && _col_rank == 0)
    cout << "BFS Batches = " << update_count << " Time = " << end - start << endl;
    */
    void* ret;
    pthread_join(sstreamh->thread, &ret);
}

template<class T>
void scopy2d_server(gview_t<T>* viewh)
{
    scopy2d_server_t<T>* sstreamh = dynamic_cast<scopy2d_server_t<T>*>(viewh);
    pgraph_t<T>* pgraph  = sstreamh->pgraph;
    vid_t        v_count = sstreamh->get_vcount();
    
    /*index_t beg_marker = 0;//(_edge_count >> 1);
    index_t rest_edges = _edge_count - beg_marker;
    index_t do_count = residue;
    index_t batch_size = rest_edges/do_count;
    index_t marker = 0; 
    */
    int update_count = 1;
    
    cout << " SCopy2d Server Started" << endl;
    
    while (sstreamh->get_snapmarker() < _edge_count) {
        /*
        marker = beg_marker + update_count*batch_size;
        if (update_count == do_count) marker = _edge_count;
        pgraph->create_marker(marker);
        pgraph->create_snapshot();
        cout << " Created snapshot at " << marker << endl;
        */
        //update the sstream view
        while (eOK != sstreamh->update_view()) {
            usleep(100);
        }
        ++update_count;
    }
    //cout << " RANK" << _rank << " update_count = " << update_count << endl;
}
