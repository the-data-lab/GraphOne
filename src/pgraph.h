#pragma once

#include <omp.h>
#include <sys/mman.h>
#include <asm/mman.h>
#include <unistd.h>
#include <fcntl.h>

#include "type.h"
#include "graph_base.h"
#include "cf_info.h"
#include "log.h"
#include "graph.h"
#include "wtime.h"
#include "edge_sharding.h"

template <class T>
class pgraph_t: public cfinfo_t {
    public:
        union {
            onekv_t<T>** skv_out;
            onegraph_t<T>** sgraph_out;
            onegraph_t<T>** sgraph; 
        };
        union {
            onekv_t<T>** skv_in;
            onegraph_t<T>** sgraph_in;
        };
        
        //circular edge log buffer
        blog_t<T>*  blog;

        //edge sharding unit
        edge_shard_t<T>* edge_shard;

#ifdef _MPI
        MPI_Datatype data_type;//For T
        MPI_Datatype edge_type;//For edgeT_t<T>
#endif

 public:    
    inline pgraph_t(): cfinfo_t(egraph){ 
        sgraph = 0;
        sgraph_in = 0;
        edge_shard = 0;
        blog = new blog_t<T>;
#ifdef _MPI
        T* type = 0;
        create_MPI_datatype(type, data_type, edge_type);
#endif
    }

    inline void alloc_edgelog(index_t count) {
        blog->alloc_edgelog(count);
    }
    status_t write_edgelog(); 
    
    virtual status_t batch_update(const string& src, const string& dst, propid_t pid = 0) {
        edgeT_t<T> edge; 
        edge.src_id = g->get_sid(src.c_str());
        set_dst(&edge, g->get_sid(dst.c_str()));
        return batch_edge(edge);
    }
    virtual status_t batch_update(const string& src, const string& dst, const char* property_str) {
        assert(0);
    } ;
        
    status_t batch_edge(edgeT_t<T>& edge);
    status_t batch_edge(tmp_blog_t<T>* tmp, edgeT_t<T>& edge);
    status_t batch_edges(tmp_blog_t<T>* tmp);
    
    index_t create_marker(index_t marker); 
    //called from snap thread 
    status_t move_marker(index_t& snap_marker); 
    
    //Wait for make graph. Be careful on why you calling.
    void waitfor_archive(index_t marker) {
        while (blog->blog_tail < marker) {
            usleep(1);
        }
    }
    
    index_t update_marker() { 
        return blog->update_marker();
    }

    inline index_t get_archived_marker() {
        return blog->blog_tail;
    }
    
 protected:
    void prep_sgraph(sflag_t ori_flag, onegraph_t<T>** a_sgraph, egraph_t egraph_type);
    void prep_skv(sflag_t ori_flag, onekv_t<T>** a_skv);
    void prep_sgraph_internal(onegraph_t<T>** sgraph);
    
    void make_graph_d(); 
    void make_graph_u();
    void make_graph_uni();
    
    void store_sgraph(onegraph_t<T>** sgraph, bool clean = false);
    void store_skv(onekv_t<T>** skv);
    
    void read_sgraph(onegraph_t<T>** sgraph);
    void read_skv(onekv_t<T>** skv);
    
    void file_open_edge(const string& dir, bool trunc);
    void file_open_sgraph(onegraph_t<T>** sgraph, const string& odir, const string& postfix, bool trunc);
    void file_open_skv(onekv_t<T>** skv, const string& odir, const string& postfix, bool trunc);
  
#ifdef BULK    
    void calc_edge_count(onegraph_t<T>** sgraph_out, onegraph_t<T>** sgraph_in); 
    void calc_edge_count_out(onegraph_t<T>** p_sgraph_out);
    void calc_edge_count_in(onegraph_t<T>** sgraph_in);

    void fill_adj_list(onegraph_t<T>** sgraph_out, onegraph_t<T>** sgraph_in);
    void fill_adj_list_in(onekv_t<T>** skv_out, onegraph_t<T>** sgraph_in); 
    void fill_adj_list_out(onegraph_t<T>** sgraph_out, onekv_t<T>** skv_in); 
#endif    
    void fill_skv_in(onekv_t<T>** skv, global_range_t<T>* global_range, vid_t j_start, vid_t j_end);
    void fill_skv(onekv_t<T>** skv_out, onekv_t<T>** skv_in);
  
    //compress the graph
    void compress_sgraph(onegraph_t<T>** sgraph);

 public:
    //Making Queries easy
    degree_t get_degree_out(sid_t sid);
    degree_t get_degree_in(sid_t sid);
    degree_t get_nebrs_out(sid_t sid, T* ptr);
    degree_t get_nebrs_in(sid_t sid, T* ptr);
    degree_t get_wnebrs_out(sid_t sid, T* ptr, sdegree_t start_offset, sdegree_t end_offset);
    degree_t get_wnebrs_in(sid_t sid, T* ptr, sdegree_t start_offset, sdegree_t end_offset);

    //making historic views
    void create_degree(sdegree_t* degree_out, sdegree_t* degree_in, index_t start_offset, index_t end_offset);
    edgeT_t<T>* get_prior_edges(index_t start_offset, index_t end_offset);

    //status_t query_adjlist_td(onegraph_t<T>** sgraph, srset_t* iset, srset_t* oset);
    //status_t query_kv_td(onekv_t<T>** skv, srset_t* iset, srset_t* oset);
    //status_t query_adjlist_bu(onegraph_t<T>** sgraph, srset_t* iset, srset_t* oset);
    //status_t query_kv_bu(onekv_t<T>** skv, srset_t* iset, srset_t* oset);
    //
    //status_t extend_adjlist_td(onegraph_t<T>** skv, srset_t* iset, srset_t* oset);
    //status_t extend_kv_td(onekv_t<T>** skv, srset_t* iset, srset_t* oset);
};

template <class T>
status_t pgraph_t<T>::batch_edge(edgeT_t<T>& edge) 
{
    status_t ret = eOK;
    
    index_t index = __sync_fetch_and_add(&blog->blog_head, 1L);
    bool rewind = !((index >> BLOG_SHIFT) & 0x1);

    //Check if we are overwritting the unarchived data, if so sleep
    while (index + 1 - blog->blog_tail > blog->blog_count) {
        //cout << "Sleeping for edge log" << endl;
        //assert(0);
        usleep(10);
    }
    
    index_t index1 = (index & blog->blog_mask);
    if (rewind) {
        blog->blog_beg[index1] = edge;
        set_dst(blog->blog_beg[index1], DEL_SID(get_dst(edge)));
    } else {
        blog->blog_beg[index1] = edge;
    }

    index += 1;
    index_t size = ((index - blog->blog_marker) & BATCH_MASK);
    
    //inform archive thread about threshold being crossed
    if ((0 == size) && (index != blog->blog_marker)) {
        create_marker(index);
        //cout << "Will create a snapshot now " << endl;
        ret = eEndBatch;
    } 
    return ret; 
}

template <class T>
status_t pgraph_t<T>::batch_edge(tmp_blog_t<T>* tmp, edgeT_t<T>& edge)
{
    status_t ret = eOK;
    if (tmp->count == tmp->max_count) {
        batch_edges(tmp);
    }
    
    tmp->edges[tmp->count++] = edge;
    return ret; 
}
    
template <class T>
status_t pgraph_t<T>::batch_edges(tmp_blog_t<T>* tmp)
{
    status_t ret = eOK;
    index_t count = tmp->count;
    edgeT_t<T>* edges = tmp->edges;
    tmp->count = 0;

    index_t index = __sync_fetch_and_add(&blog->blog_head, count);

    //Check if we are overwritting the unarchived data, if so sleep
    while (index + count - blog->blog_tail > blog->blog_count) {
        //cout << "Sleeping for edge log" << endl;
        //assert(0);
        usleep(10);
    }
    
    index_t index1;
    for (index_t i = 0; i < count; ++i) {
        index1 = ((index+i) & blog->blog_mask);
        blog->blog_beg[index1] = edges[i];
    }
    index += count;
    index_t size = ((index - blog->blog_marker) & BATCH_MASK);
    
    //inform archive thread about threshold being crossed
    if ((0 == size) && (index != blog->blog_marker)) {
        create_marker(index);
        //cout << "Will create a snapshot now " << endl;
        ret = eEndBatch;
    }
    return ret; 
}
    
template <class T>
index_t pgraph_t<T>::create_marker(index_t marker) 
{
    if (marker ==0) {
        marker = blog->blog_head;
    } else {
        while (blog->blog_head < marker) {
            //cout << "in loop " << blog->blog_head << " " << marker <<endl;
            usleep(100);
            continue;
        }
    }
    pthread_mutex_lock(&snap_mutex);
    index_t m_index = __sync_fetch_and_add(&q_head, 1L);
    q_beg[m_index % q_count] = marker;
    pthread_cond_signal(&snap_condition);
    pthread_mutex_unlock(&snap_mutex);
    //cout << "Marker queued. position = " << m_index % q_count << " " << marker << endl;
    return marker;
} 
    
//called from snap thread 
template <class T>
status_t pgraph_t<T>::move_marker(index_t& snap_marker) 
{
    pthread_mutex_lock(&snap_mutex);
    index_t head = q_head;
    //Need to read marker and set the blog_marker;
    if (q_tail == head) {
        pthread_mutex_unlock(&snap_mutex);
        //cout << "Marker NO dequeue. Position = " << head <<  endl;
        return eNoWork;
    }
    
    index_t m_index = head - 1;
    index_t marker = q_beg[m_index % q_count];
    q_tail = head;
    blog->blog_marker = marker;
    snap_marker = blog->blog_marker;
    
    pthread_mutex_unlock(&snap_mutex);
    //cout << "Marker dequeue. Position = " << m_index % q_count << " " << marker << endl;
    return eOK;
}

//called from w thread 
template <class T>
status_t pgraph_t<T>::write_edgelog() 
{
    index_t w_marker = blog->blog_head;
    index_t w_tail = blog->blog_wtail;
    index_t w_count = w_marker - w_tail;
    if (w_count == 0) return eNoWork;

    index_t actual_tail = w_tail & blog->blog_mask;
    index_t actual_marker = w_marker & blog->blog_mask;
    
    if (actual_tail < actual_marker) {
        //write and update tail
        //fwrite(blog->blog_beg + w_tail, sizeof(edgeT_t<T>), w_count, wtf);
        write(wtf, blog->blog_beg + actual_tail, sizeof(edgeT_t<T>)*w_count);
    }
    else {
        write(wtf, blog->blog_beg + actual_tail, sizeof(edgeT_t<T>)*(blog->blog_count - actual_tail));
        write(wtf, blog->blog_beg, sizeof(edgeT_t<T>)*actual_marker);
    }
    blog->blog_wtail = w_marker;

    //Write the string weights if any
    this->mem.handle_write();
    
    //fsync();
    return eOK;
}
#include "sgraph2.h"
