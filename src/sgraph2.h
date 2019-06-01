#pragma once

#include "sgraph.h"

template <class T>
void pgraph_t<T>::archive_sgraph(onegraph_t<T>** sgraph, global_range_t<T>* global_range, vid_t j_start, vid_t j_end) 
{
    index_t total = 0;
    edgeT_t<T>* edges = 0;
    tid_t src_index;
    sid_t src;
    vid_t vert1_id;

    for (vid_t j = j_start; j < j_end; ++j) {
        total = global_range[j].count;
        edges = global_range[j].edges;
        if (total != 0) {
            src_index = TO_TID(edges[0].src_id);
            sgraph[src_index]->archive(edges, total, snap_id + 1);
        }
    }
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
#ifndef BULK
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
        this->archive_sgraph(sgraph, edge_shard->global_range, j_start, j_end);
        /*
        //degree count
        this->calc_degree_noatomic(sgraph, edge_shard->global_range, j_start, j_end);
        //fill adj-list
        this->fill_adjlist_noatomic(sgraph, edge_shard->global_range, j_start, j_end);
        */
        #pragma omp barrier 
        edge_shard->cleanup();
#endif
    }

    blog->blog_tail = blog->blog_marker;  
}
template <class T>
void pgraph_t<T>::make_graph_d() 
{
    if (blog->blog_tail >= blog->blog_marker) return;

    #pragma omp parallel num_threads (THD_COUNT) 
    {
#ifndef BULK
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
        this->archive_sgraph(sgraph, edge_shard->global_range, j_start, j_end);
        this->archive_sgraph(sgraph_in, edge_shard->global_range_in, j_start_in, j_end_in);
        #pragma omp barrier 
        edge_shard->cleanup();

#else
        this->calc_edge_count(sgraph_out, sgraph_in);
        prep_sgraph_internal(sgraph_out);
        prep_sgraph_internal(sgraph_in);
        this->fill_adj_list(sgraph_out, sgraph_in);
#endif
        
    }
    blog->blog_tail = blog->blog_marker;  
}

template <class T>
void pgraph_t<T>::make_graph_u() 
{
    if (blog->blog_tail >= blog->blog_marker) return;
    
    //double start = mywtime();
    #pragma omp parallel num_threads(THD_COUNT)
    {
#ifndef BULK
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
        
        this->archive_sgraph(sgraph, edge_shard->global_range, j_start, j_end);
        #pragma omp barrier 
        edge_shard->cleanup();
#else
        
        this->calc_edge_count(sgraph, sgraph);
        prep_sgraph_internal(sgraph);
        this->fill_adj_list(sgraph, sgraph);
#endif
    }

    blog->blog_tail = blog->blog_marker;  
    //cout << "setting the tail to" << blog->blog_tail << endl;
}
/*
#ifndef BULK
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
#endif
*/
