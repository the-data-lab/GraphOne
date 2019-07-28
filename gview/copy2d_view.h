#pragma once

#include "stream_view.h"

struct part2d_t {
    int changed_e;
    int position;
    int buf_size;
    int rank;
    vid_t v_offset;
    vid_t dst_offset;
    char* buf;

    void init() {
        buf_size = BUF_TX_SZ; 
        buf = (char*) malloc(sizeof(char)*buf_size);
        changed_e = 0;
        position = 5*sizeof(uint64_t);
        rank = 0;
        v_offset = 0;
        dst_offset = 0;
    }
    void reset() {
        changed_e = 0;
        position = 5*sizeof(uint64_t);
    }
};

template <class T>
class copy2d_server_t : public stream_t<T> {
 public:
    
    using stream_t<T>::pgraph;
    using stream_t<T>::v_count;
    using stream_t<T>::edges;
    using stream_t<T>::edge_count;
 protected:
    part2d_t* _part;
    int part_count;
 public:
    copy2d_server_t(){ }
    status_t update_view();
    void init_view(pgraph_t<T>* pgraph, index_t a_flag);

 private:
    void  send_buf_one(part2d_t* part, int partial = 1);
};

template <class T>
class copy2d_client_t : public stream_t<T> {
 public:
    using stream_t<T>::pgraph;
    using stream_t<T>::v_count;
    int buf_size;
    char* buf;
    
    inline    copy2d_client_t():stream_t<T>() {}
    inline    ~copy2d_client_t() {}
    void      init_view(pgraph_t<T>* ugraph, index_t a_flag); 
    status_t  update_view();
};

template <class T>
void copy2d_server_t<T>::init_view(pgraph_t<T>* ugraph, index_t a_flag)
{
    stream_t<T>::init_view(ugraph, a_flag);
    part_count = _part_count;
    _part = (part2d_t*)malloc(sizeof(part2d_t)*part_count*part_count);
    vid_t v_local = v_count/part_count;
    int header_size = sizeof(int);
    
    for (int j = 0; j < part_count*part_count; ++j) {
        _part[j].init();
        _part[j].position = header_size;
        _part[j].rank = 1 + j;
        _part[j].dst_offset = (j%part_count)*v_local;
    }
}

template <class T>
status_t copy2d_server_t<T>::update_view()
{
    if(stream_t<T>::update_view()) return eNoWork;
    
    vid_t src, dst;
    edgeT_t<T> edge;
    int i, j;
    part2d_t* part;
    for (index_t e = 0; e < edge_count; ++e) {
        edge = edges[e];
        src = get_sid(edge.src_id);
        dst = get_sid(edge.dst_id);
        i = src/v_count;
        j = dst/v_count;
        part = _part + i*part_count + j;
        set_sid(edge.dst_id, dst - part->dst_offset);
        set_sid(edge.src_id, src - part->v_offset);
        if (part->position + sizeof(edge) > part->buf_size) {
            send_buf_one(part);
        }
        MPI_Pack(&edge, 1, pgraph->edge_type, part->buf, part->buf_size, 
                 &part->position, MPI_COMM_WORLD);
        part->changed_e +=1;
    }

    for (int j = 0; j < part_count; ++j) {
        if (0 == part[j].changed_e) {
            continue;
        }
       
        send_buf_one(part + j);
    }
}

template <class T>
void copy2d_server_t<T>::send_buf_one(part2d_t* part, int partial /*= 1*/)
{
    int header_size = 5*sizeof(uint64_t);
    uint64_t  archive_marker = this->get_snapmarker();
    //directions, prop_id, tid, snap_id, vertex size, edge size(dst vertex+properties)
    uint64_t meta_flag = 1;
    if (partial) {
        meta_flag = partial;
    }
    int t_pos = 0;
    pack_meta(part->buf, part->buf_size, t_pos, meta_flag, archive_marker, 
              0, part->changed_e);
    assert(t_pos == header_size);

    MPI_Send(part->buf, part->position, MPI_PACKED, part->rank, 0, MPI_COMM_WORLD);
    part->reset();
}

template <class T>
void copy2d_client_t<T>::init_view(pgraph_t<T>* ugraph, index_t a_flag)
{
    stream_t<T>::init_view(ugraph, a_flag);
    
    buf_size = BUF_TX_SZ; 
    buf = (char*) malloc(sizeof(char)*(buf_size));
}

template <class T>
status_t copy2d_client_t<T>::update_view()
{
    //Lets copy the data
    MPI_Status status;
    int  partial = 0;
    uint64_t  archive_marker = 0;
    uint64_t flag = 0;//directions, prop_id, tid, snap_id, vertex size, edge size (dst vertex +  properties)
    int position = 0;
    index_t changed_e = 0;
    index_t changed_v = 0;
    
    do {
        MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_PACKED, &buf_size);
        //cout << " Rank " << _rank << " MPI_get count = " << buf_size << endl;

        MPI_Recv(buf, buf_size, MPI_PACKED, 0, 0, MPI_COMM_WORLD, &status);
        position = 0;
        
        unpack_meta(buf, buf_size, position, flag, archive_marker, changed_v, changed_e);
        partial = flag;
        buf += position;

        edgeT_t<T> edge;
        for (int e = 0; e < changed_e; ++e) {
            position = e*sizeof(edge);
            MPI_Unpack(buf, buf_size, &position, &edge, 1, pgraph->edge_type, MPI_COMM_WORLD);
            pgraph->batch_edge(edge);
        }
    } while (partial == 2);
    return eOK;
}
