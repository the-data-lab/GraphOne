#pragma once
#include "graph_view.h"
#include "bfs.h"


template<class T>
void scopy2d_client(gview_t<T>* viewh)
{
    cout << " Rank " << _rank <<" SCopy2d Client Started" << endl;
    scopy2d_client_t<T>* snaph = dynamic_cast<scopy2d_client_t<T>*>(viewh);
    pgraph_t<T>* pgraph  = snaph->pgraph;
    vid_t v_count = snaph->get_vcount();
    int update_count = 0;
    
    sstream_t<T>* sstreamh = reg_sstream_view(pgraph, stream_bfs2d, 
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
