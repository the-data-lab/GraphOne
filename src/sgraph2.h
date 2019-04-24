#pragma once

#include "sgraph.h"


template <class T>
void pgraph_t<T>::calc_degree_noatomic(onegraph_t<T>** sgraph, global_range_t<T>* global_range, vid_t j_start, vid_t j_end) 
{
    index_t total = 0;
    edgeT_t<T>* edges = 0;
    tid_t src_index;
    sid_t src;
    vid_t vert1_id;

    for (vid_t j = j_start; j < j_end; ++j) {
        total = global_range[j].count;
        edges = global_range[j].edges;
        
        for (index_t i = 0; i < total; ++i) {
            src = edges[i].src_id;
            src_index = TO_TID(src);
            vert1_id = TO_VID(src);

            if (!IS_DEL(src)) { 
                sgraph[src_index]->increment_count_noatomic(vert1_id);
            } else { 
                sgraph[src_index]->decrement_count_noatomic(vert1_id);
            }
        }
    }
}

template <class T>
void pgraph_t<T>::fill_adjlist_noatomic(onegraph_t<T>** sgraph, global_range_t<T>* global_range, vid_t j_start, vid_t j_end) 
{
    index_t total = 0;
    edgeT_t<T>* edges = 0;
    tid_t src_index;
    sid_t src;
    T dst;
    vid_t vert1_id;

    for (vid_t j = j_start; j < j_end; ++j) {
        total = global_range[j].count;
        edges = global_range[j].edges;
        
        for (index_t i = 0; i < total; ++i) {
            src = edges[i].src_id;
            dst = edges[i].dst_id;
            src_index = TO_TID(src);
            vert1_id = TO_VID(src);

            sgraph[src_index]->add_nebr_noatomic(vert1_id, dst);
        }
    }
}

template <class T>
void pgraph_t<T>::make_on_classify(onegraph_t<T>** sgraph, global_range_t<T>* global_range, vid_t j_start, vid_t j_end, vid_t bit_shift)
{
    //degree count
    this->calc_degree_noatomic(sgraph, global_range, j_start, j_end);

    //Adj list
    #ifdef BULK 
    tid_t src_index = TO_TID(blog->blog_beg[0].src_id);
    vid_t v_count = g->get_type_vcount(src_index);
    
    vid_t vid_start = (j_start << bit_shift);
    vid_t vid_end = (j_end << bit_shift);
    if (vid_end > v_count) vid_end = v_count;
    sgraph[0]->setup_adjlist_noatomic(vid_start, vid_end);
    #endif
    
    //fill adj-list
    this->fill_adjlist_noatomic(sgraph, global_range, j_start, j_end);
}

inline void print(const char* str, double start_time) 
{
	//#pragma omp master
	//{
	//	double end = mywtime();
	//	cout << str << end - start_time << endl;
	//}
}

template <class T>
void pgraph_t<T>::make_graph_uni() 
{
    if (blog->blog_tail >= blog->blog_marker) return;
    
    #pragma omp parallel num_threads (THD_COUNT) 
    {
        edge_shard->classify_uni();

        //Now get the division of work
        vid_t     j_start;
        vid_t     j_end;
        vid_t tid = omp_get_thread_num();
        
        if (tid == 0) { 
            j_start = 0; 
        } else { 
            j_start = edge_shard->thd_local[tid - 1].range_end;
        }
        j_end = edge_shard->thd_local[tid].range_end;
        
        //actual work
        //degree count
        this->calc_degree_noatomic(sgraph, edge_shard->global_range, j_start, j_end);
        //fill adj-list
        this->fill_adjlist_noatomic(sgraph, edge_shard->global_range, j_start, j_end);
        //make_on_classify(sgraph_out, global_range, j_start, j_end, bit_shift); 
        
        #pragma omp barrier 
        edge_shard->cleanup();
    }

    blog->blog_tail = blog->blog_marker;  
}
template <class T>
void pgraph_t<T>::make_graph_d() 
{
    if (blog->blog_tail >= blog->blog_marker) return;

    #pragma omp parallel num_threads (THD_COUNT) 
    {
        edge_shard->classify_d();
        
        //Now get the division of work
        vid_t     j_start, j_start_in;
        vid_t     j_end, j_end_in;
        int tid = omp_get_thread_num();
        
        if (tid == 0) { 
            j_start = 0; 
        } else { 
            j_start = edge_shard->thd_local[tid - 1].range_end;
        }
        j_end = edge_shard->thd_local[tid].range_end;
        
        if (tid == THD_COUNT - 1) j_start_in = 0;
        else {
            j_start_in = edge_shard->thd_local_in[THD_COUNT - 2 - tid].range_end;
        }
        j_end_in = edge_shard->thd_local_in[THD_COUNT - 1 - tid].range_end;

        //actual work
        
        //degree count
        this->calc_degree_noatomic(sgraph, edge_shard->global_range, j_start, j_end);
        this->calc_degree_noatomic(sgraph_in, edge_shard->global_range_in, j_start_in, j_end_in);
        //fill adj-list
        this->fill_adjlist_noatomic(sgraph, edge_shard->global_range, j_start, j_end);
        this->fill_adjlist_noatomic(sgraph_in, edge_shard->global_range_in, j_start_in, j_end_in);
        
        //make_on_classify(sgraph_out, edge_shard->global_range, j_start, j_end, 0); 
        //make_on_classify(sgraph_in, edge_shard->global_range_in, j_start_in, j_end_in, 0); 
        
        #pragma omp barrier 
        edge_shard->cleanup();
        
    }
    blog->blog_tail = blog->blog_marker;  
}


template <class T>
void pgraph_t<T>::make_graph_u() 
{
    index_t blog_tail = blog->blog_tail;
    index_t blog_marker = blog->blog_marker;
    //cout << blog_tail << " " << blog_marker << endl; 
    if (blog_tail >= blog_marker) return;
    
    //double start = mywtime();
    #pragma omp parallel num_threads(THD_COUNT)
    {
        edge_shard->classify_u();
        //Now get the division of work
        vid_t     j_start;
        vid_t     j_end;
        int tid = omp_get_thread_num();
        
        if (tid == 0) { 
            j_start = 0; 
        } else { 
            j_start = edge_shard->thd_local[tid - 1].range_end;
        }
        j_end = edge_shard->thd_local[tid].range_end;
        
        //degree count
        this->calc_degree_noatomic(sgraph, edge_shard->global_range, j_start, j_end);
        //fill adj-list
        this->fill_adjlist_noatomic(sgraph, edge_shard->global_range, j_start, j_end);
        
        //make_on_classify(sgraph, edge_shard->global_range, j_start, j_end, bit_shift); 
        
        #pragma omp barrier 
        edge_shard->cleanup();
    }
    blog->blog_tail = blog->blog_marker;  
    //cout << "setting the tail to" << blog->blog_tail << endl;
}
