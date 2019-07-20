#pragma once

#include "sstream_view.h"

#ifdef _MPI

template <class T>
struct part_t {
    index_t changed_v;
    index_t changed_e;
    int position;
    int back_position;
    int delta_count;
    int buf_size;
    int rank;
    vid_t v_offset;
    char* buf;

    void init() {
        //header_size + (changed_v<<1)*sizeof(vid_t) + changed_e*sizeof(T);
        buf_size = (1<<20); 
        buf = (char*) malloc(sizeof(char)*buf_size);
        
        position = 0;
        changed_v = 0;
        changed_e = 0;
        back_position = 0;
        delta_count = 0;
        rank = 0;
        v_offset = 0;
    }
    void reset() {
        changed_v = 0;
        changed_e = 0;
        position = 5*sizeof(uint64_t);
        delta_count = 0;
    }    
}; 

template <class T>
class scopy2d_server_t : public sstream_t<T> {
 public:
    using sstream_t<T>::pgraph;
    using sstream_t<T>::sstream_func;
    int   part_count;
    part_t<T>* _part;
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
    inline scopy2d_server_t():sstream_t<T>() {
        part_count = 0;
    }
    inline ~scopy2d_server_t() {}

    void        init_view(pgraph_t<T>* pgraph, index_t a_flag);
    status_t    update_view();
 
 private:   
    void  init_buf();
    void  send_buf(part_t<T>* part);
    void  send_buf_one(part_t<T>* part, int j, int partial = 1);
    void  update_degreesnap();
    void  update_degreesnapd();
};

template <class T>
class scopy2d_client_t : public sstream_t<T> {
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

 public:
    vid_t    global_vcount;
    vid_t    v_offset;
    vid_t    dst_offset;
    part_t<T> _part;

 public:
    inline    scopy2d_client_t():sstream_t<T>() {dst_offset = 0;}
    inline    ~scopy2d_client_t() {}
   
    void      init_view(pgraph_t<T>* ugraph, index_t a_flag); 
    status_t  update_view();
 private:
    void  apply_view();
    void  apply_viewd();
};


template <class T>
void scopy2d_server_t<T>::init_buf()
{
    part_t<T>* part = (part_t<T>*)malloc(sizeof(part_t<T>)*part_count*part_count);
    _part = part;
    int tid = omp_get_thread_num();
    vid_t v_local = v_count/part_count;
    int header_size = 5*sizeof(uint64_t);

    for (int j = 0; j < part_count*part_count; ++j) {
        part[j].init();
        part[j].position = header_size;
        part[j].rank = 1 + j;
        part[j].v_offset = (j%part_count)*v_local;
    }
}

template <class T>
void scopy2d_server_t<T>::send_buf_one(part_t<T>* part, int j, int partial /*= 1*/)
{
    int header_size = 5*sizeof(uint64_t);
    uint64_t  archive_marker = snapshot->marker;
    //directions, prop_id, tid, snap_id, vertex size, edge size(dst vertex+properties)
    uint64_t meta_flag = 1;
    if (partial) {
        meta_flag = partial;
    }
    int t_pos = 0;
    
    cout << " sending to rank " << part[j].rank //<< " = "<< v_start << ":"<< v_end 
         << " size "<< part[j].position 
         << " changed_v " << part[j].changed_v
         << " changed_e " << part[j].changed_e
         << endl;
     
    pack_meta(part[j].buf, part[j].buf_size, t_pos, meta_flag, archive_marker, 
              part[j].changed_v, part[j].changed_e);
    assert(t_pos == header_size);
    MPI_Send(part[j].buf, part[j].position, MPI_PACKED, part[j].rank, 0, MPI_COMM_WORLD);
    part[j].reset();
}

template <class T>
void scopy2d_server_t<T>::send_buf(part_t<T>* part)
{
    for (int j = 0; j < part_count; ++j) {
        send_buf_one(part, j);
    }
}

template <class T>
void scopy2d_server_t<T>::update_degreesnap()
{
#ifdef B32    
    MPI_Datatype mpi_type_vid = MPI_UINT32_T;
#elif B64 
    MPI_Datatype mpi_type_vid = MPI_UINT32_T;
#endif

#pragma omp parallel num_threads(part_count)
{
    int tid = omp_get_thread_num();
    part_t<T>* part = _part + tid*part_count;
    
    vid_t v_local = v_count/part_count;
    vid_t v_start = tid*v_local;
    vid_t v_end =  v_start + v_local;
    //if (tid == part_count - 1) v_end = v_count;
    
    //Lets copy the data
    snapid_t     snap_id = snapshot->snap_id;
    degree_t  nebr_count = 0;
    degree_t   old_count = 0;
    header_t<T> delta_adjlist;
    sid_t sid;
    T dst;
    int j = 0;
    
    for (vid_t v = v_start; v < v_end; ++v) { 
        nebr_count = graph_out->get_degree(v, snap_id);
        old_count = degree_out[v];
        if (old_count == nebr_count) continue;

        degree_out[v] = nebr_count;
        for (int j = 0; j < part_count; j++) {
            if (part[j].position + 2*sizeof(vid_t) + sizeof(T) >= part[j].buf_size) {
                send_buf_one(part, j, 2);
            }
            part[j].back_position = part[j].position;
            part[j].position += 2*sizeof(vid_t);
        }
        
        nebr_count = this->start_wout(v, delta_adjlist, old_count);
        for (degree_t i = 0; i < nebr_count; ++i) {
            this->next(delta_adjlist, dst);
            sid = get_sid(dst);
            for (j = 0;  j < part_count - 1; ++j) {
                 if(sid < part[j+1].v_offset) break;
            }
            //send and reset the data
            if (part[j].position  + sizeof(T)>= part[j].buf_size) {
                part[j].changed_v++;
                part[j].changed_e += part[j].delta_count;
                MPI_Pack(&v, 1, mpi_type_vid, part[j].buf, part[j].buf_size, &part[j].back_position, MPI_COMM_WORLD);
                MPI_Pack(&part[j].delta_count, 1, mpi_type_vid, part[j].buf, part[j].buf_size, &part[j].back_position, MPI_COMM_WORLD);
                send_buf_one(part, j, 2);
                part[j].back_position = part[j].position;
                part[j].position += 2*sizeof(vid_t);
            }
            
            MPI_Pack(&dst, 1, pgraph->data_type, part[j].buf, part[j].buf_size, &part[j].position, MPI_COMM_WORLD);
            part[j].delta_count += 1;
        }

        for (int j = 0; j < part_count; ++j) {
            if (0 == part[j].delta_count) {
                part[j].position -= 2*sizeof(vid_t);
                continue;
            }
            
            part[j].changed_v++;
            part[j].changed_e += part[j].delta_count;
            MPI_Pack(&v, 1, mpi_type_vid, part[j].buf, part[j].buf_size, &part[j].back_position, MPI_COMM_WORLD);
            MPI_Pack(&part[j].delta_count, 1, mpi_type_vid, part[j].buf, part[j].buf_size, &part[j].back_position, MPI_COMM_WORLD);
            part[j].delta_count = 0;
        }
    }
    send_buf(part);    
}
}

template <class T>
void scopy2d_server_t<T>::update_degreesnapd()
{
    vid_t changed_v = 0;
    index_t changed_e = 0;

    //Lets copy the data

    return;
}

template <class T>
void scopy2d_server_t<T>::init_view(pgraph_t<T>* pgraph, index_t a_flag) 
{
    sstream_t<T>::init_view(pgraph, a_flag);
    part_count = _part_count;
    init_buf();
}

template <class T>
status_t scopy2d_server_t<T>::update_view()
{
    blog_t<T>* blog = pgraph->blog;
    index_t  marker = blog->blog_head;
    
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
status_t scopy2d_client_t<T>::update_view()
{
    if (graph_in == graph_out) {
        apply_view();
    } else {
        apply_viewd();
    }
    return eOK;
}

template <class T>
void scopy2d_client_t<T>::apply_view()
{
    //index_t changed_v = 0;
    //index_t changed_e = 0;
    bitmap_out->reset();


    //Lets copy the data
    MPI_Status status;
    //int buf_size = 0;
    //char*    buf = 0;
    int  partial = 0;
    //Header of the package
    uint64_t flag = 0;//directions, prop_id, tid, snap_id, vertex size, edge size (dst vertex +  properties)
    //int position = 0;
    uint64_t  archive_marker = 0;

    do {
        MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_CHAR, &_part.buf_size);
        //cout << " Rank " << _rank << " MPI_get count = " << buf_size << endl;
        //buf = (char*)malloc(buf_size*sizeof(char));


        MPI_Recv(_part.buf, _part.buf_size, MPI_PACKED, 0, 0, MPI_COMM_WORLD, &status);
        _part.position = 0;

        unpack_meta(_part.buf, _part.buf_size, _part.position, flag, archive_marker, _part.changed_v, _part.changed_e);
        partial = flag;

        cout << "Rank " << _rank << ":" << _part.v_offset
             << " Archive Marker = " << archive_marker 
             << " size "<< _part.buf_size 
             << " changed_v " << _part.changed_v
             << " changed_e " << _part.changed_e
             << endl;

        vid_t            vid = 0;
        //degree_t delta_count = 0;
        degree_t      icount = 0;
        degree_t    prior_sz = 16384;
        T* local_adjlist = (T*)malloc(prior_sz*sizeof(T));

        for (vid_t v = 0; v < _part.changed_v; ++v) {
            MPI_Unpack(_part.buf, _part.buf_size, &_part.position, &vid, 1, MPI_UINT32_T, MPI_COMM_WORLD);
            MPI_Unpack(_part.buf, _part.buf_size, &_part.position, &_part.delta_count, 1, MPI_UINT32_T, MPI_COMM_WORLD);
            
            graph_out->increment_count_noatomic(vid - _part.v_offset, _part.delta_count);
            //Update the degree of the view
            degree_out[vid - _part.v_offset] += _part.delta_count;
            bitmap_out->set_bit(vid - _part.v_offset);
            
            while (_part.delta_count > 0) {
                icount = _part.delta_count > prior_sz ? prior_sz : _part.delta_count;
                MPI_Unpack(_part.buf, _part.buf_size, &_part.position, local_adjlist, icount, pgraph->data_type, MPI_COMM_WORLD);
                graph_out->add_nebr_bulk(vid - _part.v_offset, local_adjlist, icount);
                _part.delta_count -= icount;
            }
        }
    } while(2 == partial);

    pgraph->new_snapshot(archive_marker);
    snapshot = pgraph->get_snapshot();
    //cout << "Rank " << _rank << " done" << endl;
}

template <class T>
void scopy2d_client_t<T>::apply_viewd()
{

}

template <class T>
void scopy2d_client_t<T>::init_view(pgraph_t<T>* ugraph, index_t a_flag)
{
    snap_t<T>::init_view(ugraph, a_flag);
    global_vcount = _global_vcount;
    
    bitmap_out = new Bitmap(v_count);
    if (graph_out == graph_in) {
        bitmap_in = bitmap_out;
    } else {
        bitmap_in = new Bitmap(v_count);
    }
    
    int row_id = (_rank - 1)/_part_count;
    int col_id = (_rank - 1)%_part_count;
    _part.init();
    v_offset = row_id*(global_vcount/_part_count);
    _part.v_offset = v_offset;
    dst_offset = col_id*(global_vcount/_part_count);
}
#endif
