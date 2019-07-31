#pragma once

#ifdef _MPI

#include "sstream_view.h"
#include "communicator.h"

struct part1d_t {
    char* buf;
    char* ebuf;
    int   buf_size;
    int   position;
    int   vposition;
    int   rank;
    int   changed_v;
    int   changed_e;
    int   delta_count;
    void init() {
        buf_size = BUF_TX_SZ;
        buf = (char*) malloc(sizeof(char)*2*buf_size);
        ebuf = (char*) malloc(sizeof(char)*buf_size);
        changed_v = 0;
        changed_e = 0;
        vposition = 5*sizeof(uint64_t);
        position = 0;
        delta_count = 0;
        rank = 0;
    }
    void reset() {
        changed_v = 0;
        changed_e = 0;
        vposition = 5*sizeof(uint64_t);
        position = 0;
        delta_count = 0;
    }
};

void buf_vertex(part1d_t* part, vid_t vid)
{
    part->changed_v++;
    MPI_Pack(&vid, 1, mpi_type_vid, part->buf, 2*part->buf_size, &part->vposition, MPI_COMM_WORLD);
    MPI_Pack(&part->changed_e, 1, mpi_type_vid, part->buf, 2*part->buf_size, &part->vposition, MPI_COMM_WORLD);
    part->changed_e += part->delta_count;
    part->delta_count = 0;
}

void send_buf_one(part1d_t* part, index_t archive_marker, int partial = 1)
{
    int header_size = 5*sizeof(uint64_t);
    //directions, prop_id, tid, snap_id, vertex size, edge size(dst vertex+properties)
    uint64_t meta_flag = 1;
    if (partial) {
        meta_flag = partial;
    }
    int t_pos = 0;
    buf_vertex(part, 0);

    if (part->vposition + part->position > 2*part->buf_size) {
        cout << part->vposition << " " << part->position 
         << " changed_v " << part->changed_v
         << " changed_e " << part->changed_e << endl;
        assert(0);
    }
     
    pack_meta(part->buf, 2*part->buf_size, t_pos, meta_flag, archive_marker, 
              part->changed_v, part->changed_e);
    assert(t_pos == header_size);
    t_pos = part->vposition;
    MPI_Pack(part->ebuf, part->position, MPI_PACKED, part->buf, (part->buf_size<<1), &part->vposition, MPI_COMM_WORLD); 
    
    /*
    cout << " Sending to rank " << part->rank 
         << " size "<< part->vposition 
         << " changed_v " << part->changed_v
         << " changed_e " << part->changed_e
         << " partial " << partial
         << endl;
    */
    MPI_Send(part->buf, part->vposition, MPI_PACKED, part->rank, 0, MPI_COMM_WORLD);
    part->reset();
}

template <class T>
class scopy1d_server_t : public sstream_t<T> {
 public:
    using sstream_t<T>::pgraph;
    using sstream_t<T>::sstream_func;
    int   part_count;
    part1d_t*  _part;
 protected:
    using sstream_t<T>::degree_in;
    using sstream_t<T>::degree_out;
    using sstream_t<T>::graph_out;
    using sstream_t<T>::graph_in;
    using sstream_t<T>::snapshot;
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
    void  prep_buf(degree_t* degree, onegraph_t<T>*graph, int tid);
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
    index_t apply_view(onegraph_t<T>* graph, degree_t* degree, Bitmap* bitmap);
};

template <class T>
void scopy1d_server_t<T>::prep_buf(degree_t* degree, onegraph_t<T>*graph, int tid)
{
    snapid_t    snap_id = snapshot->snap_id;

    //int tid = omp_get_thread_num();
    vid_t v_local = v_count/part_count;
    vid_t v_start = tid*v_local;
    vid_t v_end =  v_start + v_local;
    part1d_t* part = _part + tid;
    uint64_t  archive_marker = snapshot->marker;

    //Lets copy the data
    degree_t delta_count = 0;
    index_t changed_v = 0;
    index_t changed_e = 0;
    degree_t nebr_count = 0;
    degree_t  old_count = 0;
    header_t<T> delta_adjlist;
    T dst;

    for (vid_t v = v_start; v < v_end; ++v) {
        nebr_count = graph->get_degree(v, snap_id);
        old_count = degree[v];
        degree[v] = nebr_count;
        delta_count = nebr_count - old_count;
        if (delta_count == 0) continue;

        if (nebr_count < old_count) {
            cout << v << " " << nebr_count << " " << old_count << endl;
            assert(0);
        }

        if ((part->position + delta_count*sizeof(vid_t) > part->buf_size)
            ||(part->vposition + part->position + delta_count*sizeof(vid_t)
               + 4*sizeof(vid_t) > 2*part->buf_size)) {
            send_buf_one(part, archive_marker, 2);
        }

        graph->start(v, delta_adjlist, old_count);
        for (degree_t i = 0; i < delta_count; ++i) {
            graph->next(delta_adjlist, dst);
            MPI_Pack(&dst, 1, pgraph->data_type, part->ebuf, part->buf_size, &part->position, MPI_COMM_WORLD);
        }
        part->delta_count += delta_count;
        buf_vertex(part, v - v_start);
    }
    send_buf_one(part, archive_marker);
}

template <class T>
void scopy1d_server_t<T>::init_view(pgraph_t<T>* pgraph, index_t a_flag) 
{
    sstream_t<T>::init_view(pgraph, a_flag);
    part_count = _numtasks - 1;
    _part = (part1d_t*)malloc(sizeof(part1d_t)*part_count);
    for (int i =0; i < part_count; ++i) {
        _part[i].init();
        _part[i].rank = i + _numlogs;
    }
}

template <class T>
status_t scopy1d_server_t<T>::update_view()
{
    snapshot_t* new_snapshot = pgraph->get_snapshot();
    
    if (new_snapshot == 0|| (new_snapshot == snapshot)) return eNoWork;
    index_t old_marker = snapshot? snapshot->marker: 0;
    index_t new_marker = new_snapshot->marker;
    
    snapshot = new_snapshot;
    #pragma omp parallel for num_threads(part_count)
    for (int i = 0; i < part_count; ++i) { 
        prep_buf(degree_out, graph_out, i);
        if (graph_in != graph_out && graph_in !=0) {
            prep_buf(degree_in, graph_in, i);
        }
    }

    return eOK;
}

template <class T>
status_t scopy1d_client_t<T>::update_view()
{
    index_t archive_marker;
     archive_marker = apply_view(graph_out, degree_out, bitmap_out);
    if (graph_in != graph_out && graph_in !=0) {
        archive_marker = apply_view(graph_in, degree_in, bitmap_in);
    }
    pgraph->new_snapshot(archive_marker);
    snapshot = pgraph->get_snapshot();
    return eOK;
}

template <class T>
index_t scopy1d_client_t<T>::apply_view(onegraph_t<T>* graph, degree_t* degree, Bitmap* bitmap)
{
    index_t changed_v = 0;
    index_t changed_e = 0;
    uint64_t  archive_marker = 0;
    bitmap->reset();
    uint64_t meta_flag = 0;

    //Lets copy the data
    MPI_Status status;
    int buf_size = 0;
    char*    buf = 0;
    
    do {
    MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_CHAR, &buf_size);
    //cout << " Rank " << _rank << " MPI_get count = " << buf_size << endl;
    buf = (char*)malloc(buf_size*sizeof(char));

    int position = 0;
    MPI_Recv(buf, buf_size, MPI_PACKED, 0, 0, MPI_COMM_WORLD, &status);

    //Header of the package
    uint64_t endian = 0;//endian

    unpack_meta(buf, buf_size, position, meta_flag, archive_marker, changed_v, changed_e);
    
    /*
    cout << "Client Archive Marker = " << archive_marker 
         << " size "<< buf_size 
         << " changed v " << changed_v
         << " changed e " << changed_e
         << endl;
    */
    #pragma omp parallel num_threads(THD_COUNT)
    { 
    vid_t            vid = 0;
    degree_t  nebr_count = 0;
    degree_t delta_count = 0;
    degree_t   old_count = 0;
    int         position = 5*sizeof(uint64_t);
    int        eposition = 0;
    char*           ebuf = 0;
    int          eoffset = 0;
    int   curr, next;
    vid_t next_vid;
    eoffset = position + 2*changed_v*sizeof(vid_t);
    ebuf = buf + eoffset;
    degree_t    prior_sz = 16384;
    T* local_adjlist = (T*)malloc(prior_sz*sizeof(T));
    #pragma omp for
    for (vid_t v = 0; v < changed_v - 1; ++v) {
        position = 5*sizeof(uint64_t) + v*(sizeof(vid_t) << 1);
        MPI_Unpack(buf, buf_size, &position, &vid, 1, mpi_type_vid, MPI_COMM_WORLD);
        MPI_Unpack(buf, buf_size, &position, &curr, 1, mpi_type_vid, MPI_COMM_WORLD);
        MPI_Unpack(buf, buf_size, &position, &next_vid, 1, mpi_type_vid, MPI_COMM_WORLD);
        MPI_Unpack(buf, buf_size, &position, &next, 1, mpi_type_vid, MPI_COMM_WORLD);
        
        delta_count = next - curr;
        if (delta_count > prior_sz) {
            prior_sz = delta_count;
            free(local_adjlist);
            local_adjlist = (T*)malloc(prior_sz*sizeof(T));
        }
        eposition = curr*sizeof(T);
        MPI_Unpack(ebuf, buf_size, &eposition, local_adjlist, delta_count, pgraph->data_type, MPI_COMM_WORLD);
        
        graph->increment_count_noatomic(vid, delta_count);
        graph->add_nebr_bulk(vid, local_adjlist, delta_count);

        //Update the degree of the view
        degree[vid] += delta_count;
        bitmap->set_bit(vid + v_offset);
    }
    }
    } while(2 == meta_flag);
    
    return archive_marker;
         
}

template <class T>
void scopy1d_client_t<T>::init_view(pgraph_t<T>* ugraph, index_t a_flag)
{
    snap_t<T>::init_view(ugraph, a_flag);
    global_vcount = _global_vcount;
    vid_t local_vcount = (global_vcount/(_numtasks - 1));
    v_offset = (_analytics_rank)*local_vcount;
    
    bitmap_out = new Bitmap(global_vcount);
    if (graph_out == graph_in) {
        bitmap_in = bitmap_out;
    } else {
        bitmap_in = new Bitmap(global_vcount);
    }
}
#endif
