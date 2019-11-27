#pragma once

#include "sgraph.h"
#include "onegraph.h"

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
        if (egtype == eADJ) edge_shard->classify_uni(this);
        else if (egtype == eSNB) edge_shard->classify_snb(this);
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
        if (egtype == eADJ) edge_shard->classify_d(this);
        else if (egtype == eSNB) edge_shard->classify_snb(this);
        
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
        if (egtype == eADJ) edge_shard->classify_u(this);
        else if (egtype == eSNB) edge_shard->classify_snb(this);
#else
        this->calc_edge_count(sgraph, sgraph);
        prep_sgraph_internal(sgraph);
        this->fill_adj_list(sgraph, sgraph);
#endif
    }

    blog->blog_tail = blog->blog_marker;  
    //cout << "setting the tail to" << blog->blog_tail << endl;
}

template <class T>
void pgraph_t<T>::prep_sgraph(sflag_t ori_flag, onegraph_t<T>** sgraph, egraph_t egraph_type)
{
    sflag_t flag = ori_flag;
    vid_t   max_vcount;
    egtype = egraph_type;
    edge_shard = new edge_shard_t<T>(blog);
    
    if (flag == 0) {
        flag1_count = g->get_total_types();
        for(tid_t i = 0; i < flag1_count; i++) {
            if (0 == sgraph[i]) {
                max_vcount = g->get_type_scount(i);
                if (egraph_type == eADJ) {
                    sgraph[i] = new onegraph_t<T>;
                } else if (egraph_type == eSNB) {
                    sgraph[i] = new onesnb_t<T>;
                }
                sgraph[i]->setup(i, max_vcount);
            }
        } 
        return;
    } 
    
    tid_t   pos = 0;//it is tid
    tid_t  flag_count = __builtin_popcountll(flag);
    
    for(tid_t i = 0; i < flag_count; i++) {
        pos = __builtin_ctzll(flag);
        flag ^= (1L << pos);//reset that position
        if (0 == sgraph[pos]) {
            if (egraph_type == eADJ) {
                sgraph[pos] = new onegraph_t<T>;
            } else if (egraph_type == eSNB) {
                sgraph[pos] = new onesnb_t<T>;
            }
        }
        max_vcount = g->get_type_scount(i);
        sgraph[pos]->setup(pos, max_vcount);
    }
}

template <class T>
void pgraph_t<T>::compress_sgraph(onegraph_t<T>** sgraph)
{
    if (sgraph == 0) return;
    
    tid_t    t_count = g->get_total_types();
    
    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        if (sgraph[i] == 0) continue;
		sgraph[i]->compress();
    }
}

template <class T>
void pgraph_t<T>::store_sgraph(onegraph_t<T>** sgraph, bool clean /*= false*/)
{
    if (sgraph == 0) return;
    
    tid_t    t_count = g->get_total_types();
    
    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        if (sgraph[i] == 0) continue;
		sgraph[i]->handle_write(clean);
    }
}

template <class T>
void pgraph_t<T>::file_open_sgraph(onegraph_t<T>** sgraph, const string& dir, const string& postfix, bool trunc)
{
    if (sgraph == 0) return;
    
    char name[16];
    string  basefile = dir + col_info[0]->p_name;
    string  filename;
    string  wtfile; 

    // For each file.
    tid_t    t_count = g->get_total_types();
    for (tid_t i = 0; i < t_count; ++i) {
        if (0 == sgraph[i]) continue;
        sprintf(name, "%d", i);
        filename = basefile + name + postfix ; 
        sgraph[i]->file_open(filename, trunc);
    }
}

template <class T>
void pgraph_t<T>::read_sgraph(onegraph_t<T>** sgraph)
{
    if (sgraph == 0) return;
    
    tid_t    t_count = g->get_total_types();
    
    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        if (sgraph[i] == 0) continue;
        sgraph[i]->read_vtable();
        //sgraph[i]->read_stable(stfile);
        //sgraph[i]->read_etable();
    }
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
