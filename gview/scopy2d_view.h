#pragma once

#include "sstream_view.h"

#ifdef _MPI

template <class T>
class scopy2d_server_t : public sstream_t<T> {
 public:
    using sstream_t<T>::pgraph;
    using sstream_t<T>::sstream_func;
    int   part_count;
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
    void  update_degreesnap();
    void update_degreesnapd();
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
    using sstream_t<T>::v_offset;
    using sstream_t<T>::global_vcount;

 public:
   vid_t dst_offset;

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
struct part_t {
    index_t changed_v;
    index_t changed_e;
    int position;
    int delta_count;
    int prior_sz;
    int buf_size;
    int rank;
    vid_t v_offset;
    char* buf;
    T* local_adjlist;

    void reset() {
        changed_v = 0;
        changed_e = 0;
        position = 0;
        delta_count = 0;
    }    
}; 

template <class T>
void scopy2d_server_t<T>::update_degreesnap()
{
#pragma omp parallel num_threads(part_count)
{

    part_t<T>* part = (part_t<T>*)calloc(sizeof(part_t<T>), part_count);
    int tid = omp_get_thread_num();
    vid_t v_local = v_count/part_count;
    vid_t v_start = tid*v_local;
    vid_t v_end =  v_start + v_local;
    if (tid == part_count - 1) v_end = v_count;

    
    //Lets copy the data
    int header_size = 5*sizeof(uint64_t);
    uint64_t  archive_marker = snapshot->marker;
    uint64_t meta_flag = 1;
    //directions, prop_id, tid, snap_id, vertex size, edge size(dst vertex+properties)
    int j = 0;

    for (j = 0; j < part_count; ++j) {
        part[j].buf_size = (1<<29); //header_size + (changed_v<<1)*sizeof(vid_t) + changed_e*sizeof(T);
        part[j].buf = (char*) malloc(sizeof(char)*part[j].buf_size);
        part[j].position = header_size;
        part[j].rank = 1 + tid*part_count + j;
        part[j].v_offset = j*v_local;
        part[j].prior_sz = 16384*256;
        part[j].local_adjlist = (T*)malloc(part[j].prior_sz*sizeof(T));
    }
    
    degree_t  nebr_count = 0;
    degree_t   old_count = 0;
    snapid_t     snap_id = snapshot->snap_id;
    degree_t delta_count = 0;
    degree_t    prior_sz = 16384;
    T*     local_adjlist = (T*)malloc(prior_sz*sizeof(T));
    sid_t sid;

    for (vid_t v = v_start; v < v_end; ++v) {
        nebr_count = graph_out->get_degree(v, snap_id);
        old_count = degree_out[v];
        if (old_count == nebr_count) continue;

        degree_out[v] = nebr_count;
        delta_count = nebr_count - old_count;
        if (delta_count > prior_sz) {
            prior_sz = delta_count;
            free(local_adjlist);
            local_adjlist = (T*)malloc(prior_sz*sizeof(T));
        }
        graph_out->get_wnebrs(v, local_adjlist, old_count, delta_count);
        for (degree_t i = 0; i < delta_count; ++i) {
            sid = get_sid(local_adjlist[i]);
            for (j = 0;  j < part_count - 1; ++j) {
                 if(sid < part[j+1].v_offset) break;
            }
            
            assert(part[j].delta_count < part[j].prior_sz); 
            part[j].local_adjlist[part[j].delta_count] = local_adjlist[i];
            part[j].delta_count += 1;
        }

        for (int j = 0; j < part_count; ++j) {
            if (0 == part[j].delta_count) continue;
            
            if (part[j].position + part[j].delta_count*sizeof(T) + sizeof(vid_t) 
                > part[j].buf_size) {
                assert(0);
                //send and reset the data
                int t_pos = 0;
                pack_meta(part[j].buf, part[j].buf_size, t_pos, meta_flag, 
                          archive_marker, part[j].changed_v, part[j].changed_e);
                assert(t_pos == header_size);
                MPI_Send(part[j].buf, part[j].position, MPI_PACKED, part[j].rank,
                         0, MPI_COMM_WORLD);
                part[j].reset();
                cout << " reset once" << endl;
            }

            //cout << "V = " << v << endl;
            part[j].changed_v++;
            part[j].changed_e += part[j].delta_count;
#ifdef B32
            MPI_Pack(&v, 1, MPI_UINT32_T, part[j].buf, part[j].buf_size, &part[j].position, MPI_COMM_WORLD);
            MPI_Pack(&part[j].delta_count, 1, MPI_UINT32_T, part[j].buf, part[j].buf_size, &part[j].position, MPI_COMM_WORLD);
#else
            MPI_Pack(&v, 1, MPI_UINT64_T, part[j].buf, part[j].buf_size, &part[j].position, MPI_COMM_WORLD);
            MPI_Pack(&part[j].delta_count, 1, MPI_UINT64_T, part[j].buf, part[j].buf_size, &part[j].position, MPI_COMM_WORLD);
#endif
            MPI_Pack(part[j].local_adjlist, part[j].delta_count, MPI_UINT32_T, part[j].buf, part[j].buf_size, &part[j].position, MPI_COMM_WORLD);
            part[j].delta_count = 0;
        }
    }
    
    for (int j = 0; j < part_count; ++j) {
        cout << " sending to rank " << part[j].rank << " = "<< v_start << ":"<< v_end 
             << " size "<< part[j].position 
             << " changed_v " << part[j].changed_v
             << " changed_e " << part[j].changed_e
             << endl;
         
        int t_pos = 0;
        pack_meta(part[j].buf, part[j].buf_size, t_pos, meta_flag, archive_marker, 
                  part[j].changed_v, part[j].changed_e);
        assert(t_pos == header_size);
        MPI_Send(part[j].buf, part[j].position, MPI_PACKED, part[j].rank, 0, MPI_COMM_WORLD);
    }
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
    
    //Get the edge copies for edge centric computation
    if (IS_E_CENTRIC(flag)) { 
        new_edge_count = new_marker - old_marker;
        if (new_edges!= 0) free(new_edges);
        new_edges = (edgeT_t<T>*)malloc (new_edge_count*sizeof(edgeT_t<T>));
        memcpy(new_edges, blog->blog_beg + (old_marker & blog->blog_mask), new_edge_count*sizeof(edgeT_t<T>));
    }
    snapshot = new_snapshot;
    
    //for stale
    edges = blog->blog_beg + (new_marker & blog->blog_mask);
    edge_count = marker - new_marker;
    
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
    index_t changed_v = 0;
    index_t changed_e = 0;
    bitmap_out->reset();

    //Lets copy the data
    MPI_Status status;
    int buf_size = 0;
    char*    buf = 0;
    MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_CHAR, &buf_size);
    //cout << " Rank " << _rank << " MPI_get count = " << buf_size << endl;
    buf = (char*)malloc(buf_size*sizeof(char));

    int position = 0;
    MPI_Recv(buf, buf_size, MPI_PACKED, 0, 0, MPI_COMM_WORLD, &status);

    //Header of the package
    uint64_t flag = 0;//directions, prop_id, tid, snap_id, vertex size, edge size (dst vertex +  properties)
    uint64_t  archive_marker = 0;
    unpack_meta(buf, buf_size, position, flag, archive_marker, changed_v, changed_e);
    
    cout << "Rank " << _rank << ":" << v_offset
         << " Archive Marker = " << archive_marker 
         << " size "<< buf_size 
         << " changed_v " << changed_v
         << " changed_e " << changed_e
         << endl;

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
        bitmap_out->set_bit(vid - v_offset);

    }
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
    v_offset = row_id*(global_vcount/_part_count);
    dst_offset = col_id*(global_vcount/_part_count);
}
#endif
