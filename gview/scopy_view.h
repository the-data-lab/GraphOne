#pragma once

#include "sstream_view.h"

template <class T>
class scopy_server_t : public sstream_t<T> {
 public:
    using sstream_t<T>::pgraph;

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
    using sstream_t<T>::sstream_func;

 public:
    inline scopy_server_t():sstream_t<T>() {}
    inline ~scopy_server_t() {}
    
    status_t    update_view();
    void  update_degreesnap();
    void update_degreesnapd();
};

template <class T>
class scopy_client_t : public gview_t<T> {
 public:
    pgraph_t<T> pgraph;
    onegraph_t<T>* graph_in;
    onegraph_t<T>* graph_out;
    index_t flag;
 
 public:
    status_t    update_view();
 private:
    void  apply_view();
    void  apply_viewd();
};

template <class T>
void scopy_server_t<T>::update_degreesnap()
{
    index_t changed_v = 0;
    index_t changed_e = 0;

    #pragma omp parallel num_threads(THD_COUNT) reduction(+:changed_v) reduction(+:changed_e)
    {
        degree_t nebr_count = 0;
        degree_t  old_count = 0;
        snapid_t    snap_id = 0;
        if (snapshot) {
            snap_id = snapshot->snap_id;

            #pragma omp for 
            for (vid_t v = 0; v < v_count; ++v) {
                nebr_count = graph_out->get_degree(v, snap_id);
                old_count = degree_out[v];
                if (degree_out[v] != nebr_count) {
                    ++changed_v;
                    changed_e += (nebr_count - old_count);
                    bitmap_out->set_bit(v);
                } else {
                    bitmap_out->reset_bit(v);
                }
            }
        }
    }
#ifdef _MPI
    //Lets copy the data
    int position = 0;
    int buf_size = 0;
    char*    buf = 0;

    buf_size = 4*sizeof(uint64_t) + changed_v*sizeof(vid_t) + changed_e*sizeof(T);

    //Header of the package
    uint64_t endian = 0x0123456789ABCDEF;//endian
    MPI_Pack(&endian, 1, MPI_UINT64_T, buf, buf_size, &position, MPI_COMM_WORLD);
    uint64_t flag = 1;//directions, prop_id, tid, snap_id, vertex size, edge size (dst vertex +  properties)
    MPI_Pack(&flag, 1, MPI_UINT64_T, buf, buf_size, &position, MPI_COMM_WORLD);
    MPI_Pack(&changed_v, 1, MPI_UINT64_T, buf, buf_size, &position, MPI_COMM_WORLD);
    MPI_Pack(&changed_e, 1, MPI_UINT64_T, buf, buf_size, &position, MPI_COMM_WORLD);
    
    degree_t nebr_count  = 0;
    degree_t delta_count = 0;
    degree_t  old_count  = 0;
    degree_t    prior_sz = 16384;
    T* local_adjlist = (T*)malloc(prior_sz*sizeof(T));

    for (vid_t v = 0; v < v_count; ++v) {
        if (false == bitmap_out->has_vertex_changed_out(v)) {
            continue;
        }

        nebr_count = graph_out->get_degree(v);
        old_count = degree_out[v];
        degree_out[v] = nebr_count;
        delta_count = nebr_count - old_count;
        if (delta_count > prior_sz) {
            prior_sz = delta_count;
            free(local_adjlist);
            local_adjlist = (T*)malloc(prior_sz*sizeof(T));
        }
        graph_out->get_wnebrs(v, local_adjlist, old_count, delta_count);
#ifdef B32
        MPI_Pack(&v, 1, MPI_UINT32_T, buf, buf_size, &position, MPI_COMM_WORLD);
        MPI_Pack(&delta_count, 1, MPI_UINT32_T, buf, buf_size, &position, MPI_COMM_WORLD);
#else
        MPI_Pack(&v, 1, MPI_UINT64_T, buf, buf_size, &position, MPI_COMM_WORLD);
        MPI_Pack(&delta_count, 1, MPI_UINT64_T, buf, buf_size, &position, MPI_COMM_WORLD);
#endif
        MPI_Pack(local_adjlist, delta_count, pgraph->data_type, buf, buf_size, &position, MPI_COMM_WORLD);
    }
    MPI_Send(buf, position, MPI_PACKED, 1, 0, MPI_COMM_WORLD);
#endif
}

template <class T>
void scopy_server_t<T>::update_degreesnapd()
{
    vid_t changed_v = 0;
    index_t changed_e = 0;

    #pragma omp parallel num_threads(THD_COUNT) reduction(+:changed_v) reduction(+:changed_e)
    {
        degree_t      nebr_count = 0;
        snapid_t snap_id = 0;
        if (snapshot) {
            snap_id = snapshot->snap_id;

            vid_t   vcount_out = v_count;
            vid_t   vcount_in  = v_count;

            #pragma omp for nowait 
            for (vid_t v = 0; v < vcount_out; ++v) {
                nebr_count = graph_out->get_degree(v, snap_id);
                if (degree_out[v] != nebr_count) {
                    degree_out[v] = nebr_count;
                    bitmap_out->set_bit(v);
                } else {
                    bitmap_out->reset_bit(v);
                }
            }
            
            #pragma omp for nowait 
            for (vid_t v = 0; v < vcount_in; ++v) {
                nebr_count = graph_in->get_degree(v, snap_id);;
                if (degree_in[v] != nebr_count) {
                    degree_in[v] = nebr_count;
                    bitmap_in->set_bit(v);
                } else {
                    bitmap_in->reset_bit(v);
                }
            }
        
        }
    }

    //Lets copy the data

    return;
}

template <class T>
status_t scopy_server_t<T>::update_view()
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
status_t scopy_client_t<T>::update_view()
{
    if (graph_in == graph_out) {
        apply_view();
    } else {
        apply_viewd();
    }
    return eOK;
}

template <class T>
void scopy_client_t<T>::apply_view()
{
    index_t changed_v = 0;
    index_t changed_e = 0;

#ifdef _MPI
    //Lets copy the data
    MPI_Status status;
    int buf_size = 0;
    char*    buf = 0;
    MPI_Probe(0, 0, MPI_COMM_WORLD, status);
    MPI_Get_count(&status, MPI_CHAR, &buf_size);
    buf = (char*)malloc(buf_size, sizeof(char));

    int position = 0;
    MPI_Recv(buf, buf_size, MPI_PACKED, 0, 0, MPI_COMM_WORLD, status);

    //Header of the package
    uint64_t endian = 0;//endian
    MPI_Unpack(buf, buf_size, &position, &endian, 1, MPI_UINT64_T, MPI_COMM_WORLD);
    assert(endian == 0x0123456789ABCDEF);
    uint64_t flag = 0;//directions, prop_id, tid, snap_id, vertex size, edge size (dst vertex +  properties)
    MPI_Unpack(buf, buf_size, &position, &flag, 1, MPI_UINT64_T, MPI_COMM_WORLD);
    MPI_Unpack(buf, buf_size, &position, &changed_v, 1, MPI_UINT64_T, MPI_COMM_WORLD);
    MPI_Unpack(buf, buf_size, &position, &changed_e, 1, MPI_UINT64_T, MPI_COMM_WORLD);
    
    vid_t            vid = 0
    degree_t  nebr_count = 0;
    degree_t delta_count = 0;
    degree_t   old_count = 0;
    degree_t    prior_sz = 16384;
    T* local_adjlist = (T*)malloc(prior_sz*sizeof(T));

    for (vid_t v = 0; v < changed_v; ++v) {
#ifdef B32
        MPI_Pack(&vid, 1, MPI_UINT32_T, buf, buf_size, &position, MPI_COMM_WORLD);
        MPI_Pack(&delta_count, 1, MPI_UINT32_T, buf, buf_size, &position, MPI_COMM_WORLD);
#else
        MPI_Pack(&vid, 1, MPI_UINT64_T, buf, buf_size, &position, MPI_COMM_WORLD);
        MPI_Pack(&delta_count, 1, MPI_UINT64_T, buf, buf_size, &position, MPI_COMM_WORLD);
#endif
        if (delta_count > prior_sz) {
            prior_sz = delta_count;
            free(local_adjlist);
            local_adjlist = (T*)malloc(prior_sz*sizeof(T));
        }
        MPI_Pack(local_adjlist, delta_count, pgraph->data_type, buf, buf_size, &position, MPI_COMM_WORLD);

        graph_out->increment_count_noatomic(vid, delta_count);
        graph_out->add_nebr_bulk(vid, local_adjlist, delta_count);
    }
#endif
}

template <class T>
void scopy_client_t<T>::apply_viewd()
{

}
