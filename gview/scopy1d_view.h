#pragma once

#include "sstream_view.h"
#include "communicator.h"

#ifdef _MPI

template <class T>
class scopy1d_server_t : public sstream_t<T> {
 public:
    using sstream_t<T>::pgraph;
    using sstream_t<T>::sstream_func;
    int   part_count;
    int   buf_size;
    char* buf;
 protected:
    using sstream_t<T>::degree_in;
    using sstream_t<T>::degree_out;
    using sstream_t<T>::graph_out;
    using sstream_t<T>::graph_in;
    using sstream_t<T>::snapshot;
    using sstream_t<T>::edges;
    using sstream_t<T>::edge_count;
    using sstream_t<T>::new_edges;
    using sstream_t<T>::new_edge_count;
    using sstream_t<T>::v_count;
    using sstream_t<T>::flag;
    using sstream_t<T>::bitmap_in;
    using sstream_t<T>::bitmap_out;

 public:
    inline scopy1d_server_t():sstream_t<T>() {
        part_count = 0;
    }
    inline ~scopy1d_server_t() {}

    void        init_view(pgraph_t<T>* pgraph, index_t a_flag);
    status_t    update_view();
 
 private:   
    void  update_degreesnap();
    void update_degreesnapd();
};

template <class T>
class scopy1d_client_t : public sstream_t<T> {
 public:
    using sstream_t<T>::pgraph;
    using sstream_t<T>::snapshot;
    using sstream_t<T>::sstream_func; 
    using sstream_t<T>::thread;
    using sstream_t<T>::v_count;
    using sstream_t<T>::flag;
    using sstream_t<T>::graph_in;
    using sstream_t<T>::graph_out;
    using sstream_t<T>::degree_in;
    using sstream_t<T>::degree_out;
    using sstream_t<T>::bitmap_in;
    using sstream_t<T>::bitmap_out;
    vid_t    v_offset;
    vid_t    global_vcount;

 
 public:
    inline    scopy1d_client_t():sstream_t<T>() {}
    inline    ~scopy1d_client_t() {}
   
    void      init_view(pgraph_t<T>* ugraph, index_t a_flag); 
    status_t  update_view();
 private:
    void  apply_view();
    void  apply_viewd();
};

template <class T>
void scopy1d_server_t<T>::update_degreesnap()
{
//#pragma omp parallel num_threads(part_count)
for (int tid = 0; tid < part_count; ++tid)
{
    index_t changed_v = 0;
    index_t changed_e = 0;
    degree_t nebr_count = 0;
    degree_t  old_count = 0;
    snapid_t    snap_id = snapshot->snap_id;

    //int tid = omp_get_thread_num();
    vid_t v_local = v_count/part_count;
    vid_t v_start = tid*v_local;
    vid_t v_end =  v_start + v_local;
    if (tid == part_count - 1) v_end = v_count;

    //Header of the package
    uint64_t endian = 0x0123456789ABCDEF;//endian
    uint64_t meta_flag = 1;//directions, prop_id, tid, snap_id, vertex size, edge size (dst vertex +  properties)
    uint64_t  archive_marker = snapshot->marker;
                
    
    //Lets copy the data
    int position = 5*sizeof(uint64_t);
    nebr_count  = 0;
    old_count  = 0;
    degree_t delta_count = 0;
    header_t<T> delta_adjlist;
    T dst;

    for (vid_t v = v_start; v < v_end; ++v) {
        nebr_count = graph_out->get_degree(v, snap_id);
        old_count = degree_out[v];
        degree_out[v] = nebr_count;
        delta_count = nebr_count - old_count;

        if (nebr_count < old_count) {
            cout << v << " " << nebr_count << " " << old_count << endl;
            assert(0);
        }
        if (delta_count == 0) continue;
        ++changed_v;

        graph_out->start(v, delta_adjlist, old_count);
        MPI_Pack(&v, 1, MPI_UINT32_T, buf, buf_size, &position, MPI_COMM_WORLD);
        MPI_Pack(&delta_count, 1, MPI_UINT32_T, buf, buf_size, &position, MPI_COMM_WORLD);
        
        for (degree_t i = 0; i < delta_count; ++i) {
            graph_out->next(delta_adjlist, dst);
            MPI_Pack(&dst, 1, MPI_UINT32_T, buf, buf_size, &position, MPI_COMM_WORLD);
            ++changed_e;
        }
    }
    
    cout << " sending to rank " << tid + 1 << " = "<< v_start << ":"<< v_end 
         << " size "<< position 
         << " changed v " << changed_v
         << " changed e " << changed_e
         << endl;
         
    int t_pos = 0;
    pack_meta(buf, buf_size, t_pos, meta_flag, archive_marker, 
              changed_v, changed_e);
    MPI_Send(buf, position, MPI_PACKED, tid+1, 0, MPI_COMM_WORLD);
}
}

template <class T>
void scopy1d_server_t<T>::update_degreesnapd()
{
    vid_t changed_v = 0;
    index_t changed_e = 0;

    //Lets copy the data

    return;
}

template <class T>
void scopy1d_server_t<T>::init_view(pgraph_t<T>* pgraph, index_t a_flag) 
{
    sstream_t<T>::init_view(pgraph, a_flag);
    part_count = _numtasks - 1;
    buf_size = (1<<29);
    buf = (char*) malloc(sizeof(char)*buf_size);
}

template <class T>
status_t scopy1d_server_t<T>::update_view()
{
    snapshot_t* new_snapshot = pgraph->get_snapshot();
    
    if (new_snapshot == 0|| (new_snapshot == snapshot)) return eNoWork;
    index_t old_marker = snapshot? snapshot->marker: 0;
    index_t new_marker = new_snapshot->marker;
    
    snapshot = new_snapshot;
    
    if (graph_in == graph_out) {
        update_degreesnap();
    } else {
        update_degreesnapd();
    }

    return eOK;
}

template <class T>
status_t scopy1d_client_t<T>::update_view()
{
    if (graph_in == graph_out) {
        apply_view();
    } else {
        apply_viewd();
    }
    return eOK;
}

template <class T>
void scopy1d_client_t<T>::apply_view()
{
    index_t changed_v = 0;
    index_t changed_e = 0;
    bitmap_out->reset();

    //Lets copy the data
    MPI_Status status;
    int buf_size = 0;
    char*    buf = 0;
    MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_CHAR, &buf_size);
    cout << " Rank " << _rank << " MPI_get count = " << buf_size << endl;
    buf = (char*)malloc(buf_size*sizeof(char));

    int position = 0;
    MPI_Recv(buf, buf_size, MPI_PACKED, 0, 0, MPI_COMM_WORLD, &status);

    //Header of the package
    uint64_t endian = 0;//endian
    uint64_t meta_flag = 0;//directions, prop_id, tid, snap_id, vertex size, edge size (dst vertex +  properties)
    uint64_t  archive_marker = 0;

    unpack_meta(buf, buf_size, position, meta_flag, archive_marker, changed_v, changed_e);
    
    cout << "Client Archive Marker = " << archive_marker 
         << " size "<< position 
         << " changed v " << changed_v
         << " changed e " << changed_e
         << endl;
    
    /*
    int i = 0;
    while (i < 20000) {
        usleep(1000);
        ++i;
    }*/

    vid_t            vid = 0;
    degree_t  nebr_count = 0;
    degree_t delta_count = 0;
    degree_t   old_count = 0;
    degree_t    prior_sz = 16384;
    T* local_adjlist = (T*)malloc(prior_sz*sizeof(T));

    for (vid_t v = 0; v < changed_v; ++v) {
#ifdef B32
        MPI_Unpack(buf, buf_size, &position, &vid, 1, MPI_UINT32_T, MPI_COMM_WORLD);
        MPI_Unpack(buf, buf_size, &position, &delta_count, 1, MPI_UINT32_T, MPI_COMM_WORLD);
#else
        MPI_Unpack(buf, buf_size, &position, &vid, 1, MPI_UINT64_T, MPI_COMM_WORLD);
        MPI_Unpack(buf, buf_size, &position, &delta_count, 1, MPI_UINT64_T, MPI_COMM_WORLD);
#endif
        if (delta_count > prior_sz) {
            prior_sz = delta_count;
            free(local_adjlist);
            local_adjlist = (T*)malloc(prior_sz*sizeof(T));
        }
        MPI_Unpack(buf, buf_size, &position, local_adjlist, delta_count, MPI_UINT32_T, MPI_COMM_WORLD);
        
        graph_out->increment_count_noatomic(vid - v_offset, delta_count);
        graph_out->add_nebr_bulk(vid - v_offset, local_adjlist, delta_count);

        //Update the degree of the view
        degree_out[vid - v_offset] += delta_count;
        bitmap_out->set_bit(vid);

    }
    pgraph->new_snapshot(archive_marker);
    snapshot = pgraph->get_snapshot();
         
}

template <class T>
void scopy1d_client_t<T>::apply_viewd()
{

}

template <class T>
void scopy1d_client_t<T>::init_view(pgraph_t<T>* ugraph, index_t a_flag)
{
    snap_t<T>::init_view(ugraph, a_flag);
    global_vcount = _global_vcount;
    vid_t local_vcount = (global_vcount/(_numtasks - 1));
    v_offset = (_rank - 1)*local_vcount;
    
    bitmap_out = new Bitmap(global_vcount);
    if (graph_out == graph_in) {
        bitmap_in = bitmap_out;
    } else {
        bitmap_in = new Bitmap(global_vcount);
    }
}
#endif
