#pragma once

#include "sgraph.h"

extern vid_t RANGE_COUNT;

template <class T>
void pgraph_t<T>::estimate_classify(vid_t* vid_range, vid_t* vid_range_in, vid_t bit_shift, vid_t bit_shift_in) 
{
    sid_t src, dst;
    vid_t vert1_id, vert2_id;
    vid_t range;
    edgeT_t<T>* edges = blog->blog_beg;
    index_t index;

    #pragma omp for schedule(static)
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        src = edges[index].src_id;
        dst = get_sid(edges[index].dst_id);
        
        vert1_id = TO_VID(src);
        vert2_id = TO_VID(dst);

        //gather high level info for 1
        range = (vert1_id >> bit_shift);
        assert(range < RANGE_COUNT);
        vid_range[range] += 1;
        
        //gather high level info for 2
        range = (vert2_id >> bit_shift_in);
        assert(range < RANGE_COUNT);
        vid_range_in[range] += 1;
    }
}

template <class T>
void pgraph_t<T>::estimate_classify_uni(vid_t* vid_range, vid_t bit_shift) 
{
    sid_t src;
    vid_t vert1_id;
    vid_t range;
    edgeT_t<T>* edges = blog->blog_beg;
    index_t index;

    #pragma omp for
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        src = edges[index].src_id;
        vert1_id = TO_VID(src);

        //gather high level info for 1
        range = (vert1_id >> bit_shift);
        vid_range[range] += 1;
    }
}

template <class T>
void pgraph_t<T>::estimate_classify_runi(vid_t* vid_range, vid_t bit_shift) 
{
    sid_t dst;
    vid_t vert_id;
    vid_t range;
    edgeT_t<T>* edges = blog->blog_beg;
    index_t index;

    #pragma omp for
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        dst = get_sid(edges[index].dst_id);
        vert_id = TO_VID(dst);

        //gather high level info for 1
        range = (vert_id >> bit_shift);
        vid_range[range]++;
    }
}

template <class T>
void pgraph_t<T>::prefix_sum(global_range_t<T>* global_range, thd_local_t* thd_local,
                             vid_t range_count, vid_t thd_count, edgeT_t<T>* edge_buf)
{
    index_t total = 0;
    index_t value = 0;
    //index_t alloc_start = 0;

    //#pragma omp master
    #pragma omp for schedule(static) nowait
    for (vid_t i = 0; i < range_count; ++i) {
        total = 0;
        //global_range[i].edges = edge_buf + alloc_start;//good for larger archiving batch
        for (vid_t j = 0; j < thd_count; ++j) {
            value = thd_local[j].vid_range[i];
            thd_local[j].vid_range[i] = total;
            total += value;
        }

        //alloc_start += total;
        global_range[i].count = total;
        global_range[i].edges = 0;
        if (total != 0) {
            global_range[i].edges = (edgeT_t<T>*)calloc(total, sizeof(edgeT_t<T>));
            if (0 == global_range[i].edges) {
                cout << total << " bytes of allocation failed" << endl;
                assert(0);
            }
        }
    }
}

template <class T>
void pgraph_t<T>::work_division(global_range_t<T>* global_range, thd_local_t* thd_local,
                        vid_t range_count, vid_t thd_count, index_t equal_work)
{
    index_t my_portion = global_range[0].count;
    index_t value;
    vid_t j = 0;
    
    for (vid_t i = 1; i < range_count && j < thd_count; ++i) {
        value = global_range[i].count;
        if (my_portion + value > equal_work && my_portion != 0) {
            //cout << j << " " << my_portion << endl;
            thd_local[j++].range_end = i;
            my_portion = 0;
        }
        my_portion += value;
    }

    if (j == thd_count)
        thd_local[j -1].range_end = range_count;
    else 
        thd_local[j++].range_end = range_count;
    
    /*
    my_portion = 0;
    vid_t i1 = thd_local[j - 2].range_end;
    for (vid_t i = i1; i < range_count; i++) {
        my_portion += global_range[i1].count;
    }
    cout << j - 1 << " " << my_portion << endl;
    */
}


template <class T>
void pgraph_t<T>::classify(vid_t* vid_range, vid_t* vid_range_in, vid_t bit_shift, vid_t bit_shift_in, 
            global_range_t<T>* global_range, global_range_t<T>* global_range_in)
{
    sid_t src, dst;
    vid_t vert1_id, vert2_id;
    vid_t range = 0;
    edgeT_t<T>* edge;
    edgeT_t<T>* edges = blog->blog_beg;
    index_t index;

    #pragma omp for schedule(static)
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        src = edges[index].src_id;
        dst = get_sid(edges[index].dst_id);
       
        vert1_id = TO_VID(src);
        vert2_id = TO_VID(dst);
        
        range = (vert1_id >> bit_shift);
        edge = global_range[range].edges + vid_range[range];
        vid_range[range] += 1;
        //assert(edge - global_range[range].edges < global_range[range].count);
        edge->src_id = src;
        edge->dst_id = edges[index].dst_id;
        
        range = (vert2_id >> bit_shift_in);
        edge = global_range_in[range].edges + vid_range_in[range];
        vid_range_in[range] += 1;
        //assert(edge - global_range_in[range].edges < global_range_in[range].count);
        edge->src_id = dst;
        set_dst(edge, src);
        set_weight(edge, edges[index].dst_id);
    }
}

template <class T>
void pgraph_t<T>::classify_runi(vid_t* vid_range, vid_t bit_shift, global_range_t<T>* global_range)
{
    sid_t src, dst;
    vid_t vert_id;
    vid_t range = 0;
    edgeT_t<T>* edge;
    edgeT_t<T>* edges = blog->blog_beg;
    index_t index;

    #pragma omp for
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        src = edges[index].src_id;
        dst = get_sid(edges[index].dst_id);
        vert_id = TO_VID(dst);
        
        range = (vert_id >> bit_shift);
        edge = global_range[range].edges + vid_range[range]++;
        
        edge->src_id = dst;
        set_dst(edge, src);
        set_weight(edge, edges[index].dst_id);
    }
}

template <class T>
void pgraph_t<T>::classify_uni(vid_t* vid_range, vid_t bit_shift, global_range_t<T>* global_range)
{
    sid_t src;
    vid_t vert1_id;
    vid_t range = 0;
    edgeT_t<T>* edge;
    edgeT_t<T>* edges = blog->blog_beg;
    index_t index;

    #pragma omp for
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        src = edges[index].src_id;
        vert1_id = TO_VID(src);
        
        range = (vert1_id >> bit_shift);
        edge = global_range[range].edges + vid_range[range]++;
        edge->src_id = src;
        edge->dst_id = edges[index].dst_id;
    }
}

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
    
    tid_t src_index = TO_TID(blog->blog_beg[0].src_id);
    vid_t v_count = g->get_type_vcount(src_index);
    vid_t range_count = 1024;
    vid_t thd_count = THD_COUNT;
    vid_t  base_vid = ((v_count -1)/range_count);
    
    //find the number of bits to do shift to find the range
#if B32    
    vid_t bit_shift = __builtin_clz(base_vid);
    bit_shift = 32 - bit_shift; 
#else
    vid_t bit_shift = __builtin_clzl(base_vid);
    bit_shift = 64 - bit_shift; 
#endif

    global_range_t<T>* global_range = (global_range_t<T>*)calloc(
                            range_count, sizeof(global_range_t<T>));
    
    thd_local_t* thd_local = (thd_local_t*) calloc(thd_count, sizeof(thd_local_t));  
   
    index_t total_edge_count = blog->blog_marker - blog->blog_tail;
    //alloc_edge_buf(total_edge_count);
    
    index_t edge_count = (total_edge_count*1.15)/(thd_count);
    

    #pragma omp parallel num_threads (thd_count) 
    {
        vid_t tid = omp_get_thread_num();
        vid_t* vid_range = (vid_t*)calloc(range_count, sizeof(vid_t)); 
        thd_local[tid].vid_range = vid_range;

        //double start = mywtime();

        //Get the count for classification
        this->estimate_classify_uni(vid_range, bit_shift);
        
        this->prefix_sum(global_range, thd_local, range_count, thd_count, edge_buf_out);
        #pragma omp barrier 
        
        //Classify
        this->classify_uni(vid_range, bit_shift, global_range);
        
        #pragma omp master 
        {
            //double end = mywtime();
            //cout << " classify " << end - start << endl;
            this->work_division(global_range, thd_local, range_count, thd_count, edge_count);
        }
        
        #pragma omp barrier 
        
        //Now get the division of work
        vid_t     j_start;
        vid_t     j_end;
        
        if (tid == 0) { 
            j_start = 0; 
        } else { 
            j_start = thd_local[tid - 1].range_end;
        }
        j_end = thd_local[tid].range_end;
        
        //actual work
        make_on_classify(sgraph_out, global_range, j_start, j_end, bit_shift); 
        
        free(vid_range);
        #pragma omp barrier 
        
        //free the memory
        #pragma omp for schedule (static)
        for (vid_t i = 0; i < range_count; ++i) {
            if (global_range[i].edges) {
                free(global_range[i].edges);
                global_range[i].edges = 0;
            }
        }
    }

    free(global_range);
    free(thd_local);
    
    //blog->blog_tail = blog->blog_marker;  
}

template <class T>
void pgraph_t<T>::make_graph_d() 
{
    if (blog->blog_tail >= blog->blog_marker) return;
    vid_t thd_count = THD_COUNT;
    vid_t range_count = 1024;
    
    tid_t src_index = TO_TID(blog->blog_beg[0].src_id);
    vid_t v_count = g->get_type_vcount(src_index);
    vid_t  base_vid = ((v_count -1)/range_count);
    if (base_vid == 0) {
        base_vid = 1024;
    }
    
    tid_t dst_index = TO_TID(get_dst(&blog->blog_beg[0]));
    vid_t v_count_in = g->get_type_vcount(dst_index);
    vid_t  base_vid_in = ((v_count_in -1)/range_count);
    if (base_vid_in == 0) {
        base_vid_in = 1024;
    }
    
    //find the number of bits to do shift to find the range
#if B32    
    vid_t bit_shift = __builtin_clz(base_vid);
    bit_shift = 32 - bit_shift; 
    vid_t bit_shift_in = __builtin_clz(base_vid_in);
    bit_shift_in = 32 - bit_shift_in; 
#else
    vid_t bit_shift = __builtin_clzl(base_vid);
    bit_shift = 64 - bit_shift; 
    vid_t bit_shift_in = __builtin_clzl(base_vid_in);
    bit_shift_in = 64 - bit_shift_in; 
#endif

    global_range_t<T>* global_range = (global_range_t<T>*)calloc(
                            sizeof(global_range_t<T>), range_count);
    
    global_range_t<T>* global_range_in = (global_range_t<T>*)calloc(
                            sizeof(global_range_t<T>), range_count);
    
    thd_local_t* thd_local = (thd_local_t*) calloc(sizeof(thd_local_t), thd_count);  
    thd_local_t* thd_local_in = (thd_local_t*) calloc(sizeof(thd_local_t), thd_count);  
   
    index_t total_edge_count = blog->blog_marker - blog->blog_tail;
    //alloc_edge_buf(total_edge_count);
    
    index_t edge_count = (total_edge_count*1.15)/(thd_count);
    

    #pragma omp parallel num_threads (thd_count) 
    {
        vid_t tid = omp_get_thread_num();
        vid_t* vid_range = (vid_t*)calloc(sizeof(vid_t), range_count); 
        vid_t* vid_range_in = (vid_t*)calloc(sizeof(vid_t), range_count); 
        thd_local[tid].vid_range = vid_range;
        thd_local_in[tid].vid_range = vid_range_in;

        //double start = mywtime();

        //Get the count for classification
        this->estimate_classify(vid_range, vid_range_in, bit_shift, bit_shift_in);
        
        this->prefix_sum(global_range, thd_local, range_count, thd_count, edge_buf_out);
        this->prefix_sum(global_range_in, thd_local_in, range_count, thd_count, edge_buf_in);
        #pragma omp barrier 
        
        //Classify
        this->classify(vid_range, vid_range_in, bit_shift, bit_shift_in, global_range, global_range_in);
        
        #pragma omp master 
        {
            //double end = mywtime();
            //cout << " classify " << end - start << endl;
            this->work_division(global_range, thd_local, range_count, thd_count, edge_count);
        }
        
        if (tid == 1) {
            this->work_division(global_range_in, thd_local_in, range_count, thd_count, edge_count);
        }
        #pragma omp barrier 
        
        //Now get the division of work
        vid_t     j_start, j_start_in;
        vid_t     j_end, j_end_in;
        
        if (tid == 0) { 
            j_start = 0; 
        } else { 
            j_start = thd_local[tid - 1].range_end;
        }
        j_end = thd_local[tid].range_end;
        
        if (tid == thd_count - 1) j_start_in = 0;
        else {
            j_start_in = thd_local_in[thd_count - 2 - tid].range_end;
        }
        j_end_in = thd_local_in[thd_count - 1 - tid].range_end;

        //actual work
        make_on_classify(sgraph_out, global_range, j_start, j_end, bit_shift); 
        make_on_classify(sgraph_in, global_range_in, j_start_in, j_end_in, bit_shift_in); 
        
        free(vid_range);
        free(vid_range_in);
        #pragma omp barrier 
        
        //free the memory
        #pragma omp for schedule (static)
        for (vid_t i = 0; i < range_count; ++i) {
            if (global_range[i].edges)
                free(global_range[i].edges);
            
            if (global_range_in[i].edges)
                free(global_range_in[i].edges);
        }
    }

    free(global_range);
    free(thd_local);
    
    free(global_range_in);
    free(thd_local_in);
    //blog->blog_tail = blog->blog_marker;  
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

template <class T>
void pgraph_t<T>::make_graph_u() 
{
    index_t blog_tail = blog->blog_tail;
    index_t blog_marker = blog->blog_marker;
    //index_t blog_head = blog->blog_head;
    //edgeT_t<T> edge = blog->blog_beg[(blog_head-1) & blog->blog_mask];
    if (blog_tail >= blog_marker) return;
    //cout << blog->blog_tail << " " << blog->blog_marker << endl; 
    tid_t src_index = TO_TID(blog->blog_beg[0].src_id);
    vid_t v_count = g->get_type_vcount(src_index);
    
    vid_t range_count = 1024;
    vid_t thd_count = THD_COUNT;
    vid_t  base_vid = ((v_count -1)/range_count);
    
    //find the number of bits to do shift to find the range
#if B32    
    vid_t bit_shift = __builtin_clz(base_vid);
    bit_shift = 32 - bit_shift; 
#else
    vid_t bit_shift = __builtin_clzl(base_vid);
    bit_shift = 64 - bit_shift; 
#endif

    global_range_t<T>* global_range = (global_range_t<T>*)calloc(
                            range_count, sizeof(global_range_t<T>));
    
    thd_local_t* thd_local = (thd_local_t*) calloc(thd_count, sizeof(thd_local_t));  
    index_t total_edge_count = blog->blog_marker - blog->blog_tail ;
    index_t edge_count = ((total_edge_count << 1)*1.15)/(thd_count);
    
    //alloc_edge_buf(total_edge_count);
    double start = mywtime();

    #pragma omp parallel num_threads(thd_count)
    {
        int tid = omp_get_thread_num();
        vid_t* vid_range = (vid_t*)calloc(range_count, sizeof(vid_t)); 
        //memset(vid_range, 0, range_count*sizeof(vid_t));
        thd_local[tid].vid_range = vid_range;

        //Get the count for classification
        this->estimate_classify(vid_range, vid_range, bit_shift, bit_shift);
        
        this->prefix_sum(global_range, thd_local, range_count, thd_count, edge_buf_out);
        #pragma omp barrier 
        
        //Classify
        this->classify(vid_range, vid_range, bit_shift, bit_shift, global_range, global_range);
        #pragma omp master 
        {
            print(" classify = ", start);
            //double end = mywtime();
            //cout << " classify " << end - start << endl;
            this->work_division(global_range, thd_local, range_count, thd_count, edge_count);
        }
        #pragma omp barrier 
        
        //Now get the division of work
        vid_t     j_start;
        vid_t     j_end; 
        
        if (tid == 0) { 
            j_start = 0; 
        } else { 
            j_start = thd_local[tid - 1].range_end;
        }
        j_end = thd_local[tid].range_end;
        
        make_on_classify(sgraph, global_range, j_start, j_end, bit_shift); 
        
        #pragma omp barrier 
        //free the memory
        free(vid_range);
        #pragma omp for schedule (static)
        for (vid_t i = 0; i < range_count; ++i) {
            if (global_range[i].edges) {
                free(global_range[i].edges);
                global_range[i].edges = 0;
            }
        }
    }
    //free_edge_buf();
    free(global_range);
    free(thd_local);
    blog->blog_tail = blog->blog_marker;  
    //cout << "setting the tail to" << blog->blog_tail << endl;
}
