#pragma once

#include "type.h" 

extern vid_t RANGE_COUNT;
extern vid_t RANGE_2DSHIFT;
/*
template <class T>
void pgraph_t<T>::alloc_edge_buf(index_t total) 
{
    index_t total_edge_count = 0;
    if (0 == sgraph_in) {
        total_edge_count = (total << 1);
        if (0 == edge_buf_count) {
            edge_buf_out = (edgeT_t<T>*)malloc(total_edge_count*sizeof(edgeT_t<T>));
            edge_buf_count = total_edge_count;
        } else if (edge_buf_count < total_edge_count) {
            free(edge_buf_out);
            edge_buf_out = (edgeT_t<T>*)malloc(total_edge_count*sizeof(edgeT_t<T>));
            edge_buf_count = total_edge_count;
        }
    } else {
        total_edge_count = total;
        if (0 == edge_buf_count) {
            edge_buf_out = (edgeT_t<T>*)malloc(total_edge_count*sizeof(edgeT_t<T>));
            edge_buf_in = (edgeT_t<T>*)malloc(total_edge_count*sizeof(edgeT_t<T>));
            edge_buf_count = total_edge_count;
        } else if (edge_buf_count < total_edge_count) {
            free(edge_buf_out);
            free(edge_buf_in);
            edge_buf_out = (edgeT_t<T>*)malloc(total_edge_count*sizeof(edgeT_t<T>));
            edge_buf_in = (edgeT_t<T>*)malloc(total_edge_count*sizeof(edgeT_t<T>));
            edge_buf_count = total_edge_count;
        }
    }
}

template <class T>
void pgraph_t<T>::free_edge_buf() 
{
    if (edge_buf_out) {    
        free(edge_buf_out);
        edge_buf_out = 0;
    }
    if (edge_buf_in) {
        free(edge_buf_in);
        edge_buf_in = 0;
    }
    edge_buf_count = 0;
}
*/

//for each range
template <class T>
class global_range_t {
  public:
      index_t count;
      edgeT_t<T>* edges;
};

//for each thread 
class thd_local_t {
  public:
      vid_t* vid_range; //For each range
      vid_t  range_end;
};

template <class T>
class edge_shard_t {
 public:
    blog_t<T>* blog;
    
    //intermediate classification buffer
    edgeT_t<T>* edge_buf_out;
    edgeT_t<T>* edge_buf_in;
    index_t edge_buf_count;
    
    global_range_t<T>* global_range;
    global_range_t<T>* global_range_in;
    
    thd_local_t* thd_local;
    thd_local_t* thd_local_in;

 public:
    ~edge_shard_t();
    edge_shard_t(blog_t<T>* binary_log);
    void classify_u(pgraph_t<T>* pgraph);
    void classify_d(pgraph_t<T>* pgraph);
    void classify_uni(pgraph_t<T>* pgraph);
    void classify_snb(pgraph_t<T>* pgraph);
    void classify_runi(pgraph_t<T>* pgraph);
    void cleanup();

 private:
    void archive(onegraph_t<T>** sgraph, global_range_t<T>* global_range, 
                 vid_t j_start, vid_t j_end, snapid_t snap_id);
    void estimate_classify(vid_t* vid_range, vid_t* vid_range_in, vid_t bit_shift, vid_t bit_shift_in);
    void estimate_classify_uni(vid_t* vid_range, vid_t bit_shift);
    void estimate_classify_runi(vid_t* vid_range, vid_t bit_shift);

    void prefix_sum(global_range_t<T>* global_range, thd_local_t* thd_local,
                             vid_t range_count, vid_t thd_count, edgeT_t<T>* edge_buf);

    void work_division(global_range_t<T>* global_range, thd_local_t* thd_local,
                        vid_t range_count, vid_t thd_count, index_t equal_work);

    void classify(vid_t* vid_range, vid_t* vid_range_in, vid_t bit_shift, vid_t bit_shift_in, 
            global_range_t<T>* global_range, global_range_t<T>* global_range_in);
    void classify_runi(vid_t* vid_range, vid_t bit_shift, global_range_t<T>* global_range);
    void classify_uni(vid_t* vid_range, vid_t bit_shift, global_range_t<T>* global_range);
    
    void estimate_classify_snb(vid_t* vid_range, vid_t bit_shift);
    void classify_snb(vid_t* vid_range, vid_t bit_shift, global_range_t<T>* global_range);
};

template <class T>
edge_shard_t<T>::edge_shard_t(blog_t<T>* binary_log)
{
    blog = binary_log;
    //alloc_edge_buf(total_edge_count);
    global_range = (global_range_t<T>*)calloc(RANGE_COUNT, 
                                        sizeof(global_range_t<T>));
    global_range_in = (global_range_t<T>*)calloc(RANGE_COUNT, 
                                        sizeof(global_range_t<T>));
    
    thd_local = (thd_local_t*) calloc(THD_COUNT, sizeof(thd_local_t));  
    thd_local_in = (thd_local_t*) calloc(THD_COUNT, sizeof(thd_local_t));  
}

template <class T>
edge_shard_t<T>::~edge_shard_t()
{
    //free_edge_buf();
    free(global_range);
    free(thd_local);
    free(global_range_in);
    free(thd_local_in);
}

template <class T>
void edge_shard_t<T>::cleanup()
{
    #pragma omp for schedule (static)
    for (vid_t i = 0; i < RANGE_COUNT; ++i) {
        if (global_range[i].edges) {
            free(global_range[i].edges);
            global_range[i].edges = 0;
            global_range[i].count = 0;
        }
        if (global_range_in[i].edges) {
            free(global_range_in[i].edges);
            global_range_in[i].edges = 0;
            global_range_in[i].count = 0;
        }
    }
    #pragma omp for schedule (static)
    for (int i = 0; i < THD_COUNT; ++i) {
        thd_local[i].range_end = 0;
        thd_local_in[i].range_end = 0;
    }
}

template <class T>
void edge_shard_t<T>::classify_u(pgraph_t<T>* pgraph)
{
    index_t total_edge_count = blog->blog_marker - blog->blog_tail ;
    index_t edge_count = ((total_edge_count << 1)*1.15)/(THD_COUNT);
    
    tid_t src_index = TO_TID(blog->blog_beg[0].src_id);
    vid_t v_count = g->get_type_vcount(src_index);
    
    vid_t  base_vid = ((v_count -1)/RANGE_COUNT);
    
    //find the number of bits to do shift to find the range
#if B32    
    vid_t bit_shift = __builtin_clz(base_vid);
    bit_shift = 32 - bit_shift; 
#else
    vid_t bit_shift = __builtin_clzl(base_vid);
    bit_shift = 64 - bit_shift; 
#endif
    
    int tid = omp_get_thread_num();
    vid_t* vid_range = (vid_t*)calloc(RANGE_COUNT, sizeof(vid_t)); 
    thd_local[tid].vid_range = vid_range;

    //Get the count for classification
    this->estimate_classify(vid_range, vid_range, bit_shift, bit_shift);
    
    this->prefix_sum(global_range, thd_local, RANGE_COUNT, THD_COUNT, edge_buf_out);
    #pragma omp barrier 
    
    //Classify
    this->classify(vid_range, vid_range, bit_shift, bit_shift, global_range, global_range);
    #pragma omp master 
    {
        //print(" classify = ", start);
        this->work_division(global_range, thd_local, RANGE_COUNT, THD_COUNT, edge_count);
    }
    #pragma omp barrier 
    free(vid_range);
        
    //Now get the division of work
    vid_t     j_start;
    vid_t     j_end;
    
    if (tid == 0) { 
        j_start = 0; 
    } else { 
        j_start = thd_local[tid - 1].range_end;
    }
    j_end = thd_local[tid].range_end;
    
    archive(pgraph->sgraph, global_range, j_start, j_end, pgraph->snap_id);
    #pragma omp barrier 
    cleanup();
}

template <class T>
void edge_shard_t<T>::classify_uni(pgraph_t<T>* pgraph)
{
    index_t total_edge_count = blog->blog_marker - blog->blog_tail ;
    index_t edge_count = ((total_edge_count << 1)*1.15)/(THD_COUNT);
    
    tid_t src_index = TO_TID(blog->blog_beg[0].src_id);
    vid_t v_count = g->get_type_vcount(src_index);
    
    vid_t  base_vid = ((v_count -1)/RANGE_COUNT);
    
    //find the number of bits to do shift to find the range
#if B32    
    vid_t bit_shift = __builtin_clz(base_vid);
    bit_shift = 32 - bit_shift; 
#else
    vid_t bit_shift = __builtin_clzl(base_vid);
    bit_shift = 64 - bit_shift; 
#endif
    
    int tid = omp_get_thread_num();
    vid_t* vid_range = (vid_t*)calloc(RANGE_COUNT, sizeof(vid_t)); 
    thd_local[tid].vid_range = vid_range;

    //Get the count for classification
    this->estimate_classify_uni(vid_range, bit_shift);
    
    this->prefix_sum(global_range, thd_local, RANGE_COUNT, THD_COUNT, edge_buf_out);
    #pragma omp barrier 
    
    //Classify
    this->classify_uni(vid_range, bit_shift, global_range);
    #pragma omp master 
    {
        //print(" classify = ", start);
        this->work_division(global_range, thd_local, RANGE_COUNT, THD_COUNT, edge_count);
    }
    #pragma omp barrier 
    free(vid_range);
    
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
    archive(pgraph->sgraph, global_range, j_start, j_end, pgraph->snap_id);
    #pragma omp barrier 
    cleanup();
}

template <class T>
void edge_shard_t<T>::classify_snb(pgraph_t<T>* pgraph)
{
    index_t total_edge_count = blog->blog_marker - blog->blog_tail ;
    index_t edge_count = ((total_edge_count << 1)*1.15)/(THD_COUNT);
    
    tid_t src_index = TO_TID(blog->blog_beg[0].src_id);
    vid_t v_count = g->get_type_vcount(src_index);
    
    vid_t  base_vid = ((v_count -1) >> RANGE_2DSHIFT);
    
    //find the number of bits to do shift to find the range
#if B32    
    vid_t bit_shift = __builtin_clz(base_vid);
    bit_shift = 32 - bit_shift; 
#else
    vid_t bit_shift = __builtin_clzl(base_vid);
    bit_shift = 64 - bit_shift; 
#endif
    
    int tid = omp_get_thread_num();
    vid_t* vid_range = (vid_t*)calloc(RANGE_COUNT, sizeof(vid_t)); 
    thd_local[tid].vid_range = vid_range;

    //Get the count for classification
    this->estimate_classify_snb(vid_range, bit_shift);
    
    this->prefix_sum(global_range, thd_local, RANGE_COUNT, THD_COUNT, edge_buf_out);
    #pragma omp barrier 
    
    //Classify
    this->classify_snb(vid_range, bit_shift, global_range);
    #pragma omp master 
    {
        //print(" classify = ", start);
        this->work_division(global_range, thd_local, RANGE_COUNT, THD_COUNT, edge_count);
    }
    #pragma omp barrier 
    free(vid_range);
    
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
    archive(pgraph->sgraph, global_range, j_start, j_end, pgraph->snap_id);
    #pragma omp barrier 
    cleanup();
}

template <class T>
void edge_shard_t<T>::classify_runi(pgraph_t<T>* pgraph)
{
    index_t total_edge_count = blog->blog_marker - blog->blog_tail ;
    index_t edge_count = ((total_edge_count << 1)*1.15)/(THD_COUNT);
    
    tid_t src_index = TO_TID(blog->blog_beg[0].src_id);
    vid_t v_count = g->get_type_vcount(src_index);
    
    vid_t  base_vid = ((v_count -1)/RANGE_COUNT);
    
    //find the number of bits to do shift to find the range
#if B32    
    vid_t bit_shift = __builtin_clz(base_vid);
    bit_shift = 32 - bit_shift; 
#else
    vid_t bit_shift = __builtin_clzl(base_vid);
    bit_shift = 64 - bit_shift; 
#endif
    
    int tid = omp_get_thread_num();
    vid_t* vid_range = (vid_t*)calloc(RANGE_COUNT, sizeof(vid_t)); 
    thd_local[tid].vid_range = vid_range;

    //Get the count for classification
    this->estimate_classify_runi(vid_range, bit_shift);
    
    this->prefix_sum(global_range, thd_local, RANGE_COUNT, THD_COUNT, edge_buf_out);
    #pragma omp barrier 
    
    //Classify
    this->classify_runi(vid_range, bit_shift, global_range);
    #pragma omp master 
    {
        //print(" classify = ", start);
        this->work_division(global_range, thd_local, RANGE_COUNT, THD_COUNT, edge_count);
    }
    #pragma omp barrier 
    free(vid_range);
}

template <class T>
void edge_shard_t<T>::classify_d(pgraph_t<T>* pgraph)
{
    tid_t src_index = TO_TID(blog->blog_beg[0].src_id);
    vid_t v_count = g->get_type_vcount(src_index);
    vid_t  base_vid = ((v_count -1)/RANGE_COUNT);
    if (base_vid == 0) {
        base_vid = RANGE_COUNT;
    }
    
    tid_t dst_index = TO_TID(get_dst(&blog->blog_beg[0]));
    vid_t v_count_in = g->get_type_vcount(dst_index);
    vid_t  base_vid_in = ((v_count_in -1)/RANGE_COUNT);
    if (base_vid_in == 0) {
        base_vid_in = RANGE_COUNT;
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

    index_t total_edge_count = blog->blog_marker - blog->blog_tail;
    //alloc_edge_buf(total_edge_count);
    
    index_t edge_count = (total_edge_count*1.15)/(THD_COUNT);

    vid_t tid = omp_get_thread_num();
    vid_t* vid_range = (vid_t*)calloc(sizeof(vid_t), RANGE_COUNT); 
    vid_t* vid_range_in = (vid_t*)calloc(sizeof(vid_t), RANGE_COUNT); 
    thd_local[tid].vid_range = vid_range;
    thd_local_in[tid].vid_range = vid_range_in;

    //double start = mywtime();

    //Get the count for classification
    this->estimate_classify(vid_range, vid_range_in, bit_shift, bit_shift_in);
    
    this->prefix_sum(global_range, thd_local, RANGE_COUNT, THD_COUNT, edge_buf_out);
    this->prefix_sum(global_range_in, thd_local_in, RANGE_COUNT, THD_COUNT, edge_buf_in);
    #pragma omp barrier 
    
    //Classify
    this->classify(vid_range, vid_range_in, bit_shift, bit_shift_in, global_range, global_range_in);
    
    #pragma omp master 
    {
        //double end = mywtime();
        //cout << " classify " << end - start << endl;
        this->work_division(global_range, thd_local, RANGE_COUNT, THD_COUNT, edge_count);
    }
    
    if (tid == 1 || (tid == 0 && THD_COUNT == 1)) {
        this->work_division(global_range_in, thd_local_in, RANGE_COUNT, THD_COUNT, edge_count);
    }
    #pragma omp barrier 
    free(vid_range);
    free(vid_range_in);

    //Now get the division of work
    vid_t     j_start, j_start_in;
    vid_t     j_end, j_end_in;
    
    if (tid == 0) { 
        j_start = 0; 
    } else { 
        j_start = thd_local[tid - 1].range_end;
    }
    j_end = thd_local[tid].range_end;
    
    if (tid == THD_COUNT - 1) j_start_in = 0;
    else {
        j_start_in = thd_local_in[THD_COUNT - 2 - tid].range_end;
    }
    j_end_in = thd_local_in[THD_COUNT - 1 - tid].range_end;

    //actual work
    archive(pgraph->sgraph, global_range, j_start, j_end, pgraph->snap_id);
    archive(pgraph->sgraph_in, global_range_in, j_start_in, j_end_in, pgraph->snap_id);
    #pragma omp barrier 
    cleanup();
}

template <class T>
void edge_shard_t<T>::estimate_classify(vid_t* vid_range, vid_t* vid_range_in, vid_t bit_shift, vid_t bit_shift_in) 
{
    sid_t src, dst;
    vid_t vert1_id, vert2_id;
    vid_t range;
    edgeT_t<T>* edges = blog->blog_beg;
    index_t index;
    bool rewind1, rewind2;

    #pragma omp for schedule(static)
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        rewind1 = !((i >> BLOG_SHIFT) & 0x1);
        rewind2 = IS_DEL(get_dst(edges[index]));
	while (rewind1 != rewind2) {
	    usleep(10);
            rewind2 = IS_DEL(get_dst(edges[index]));
	}
	
        src = edges[index].src_id;
        dst = TO_SID(get_dst(edges[index]));

        
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
void edge_shard_t<T>::estimate_classify_uni(vid_t* vid_range, vid_t bit_shift) 
{
    sid_t src;
    vid_t vert1_id;
    vid_t range;
    edgeT_t<T>* edges = blog->blog_beg;
    index_t index;
    bool rewind1, rewind2;

    #pragma omp for
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        rewind1 = !((i >> BLOG_SHIFT) & 0x1);
        rewind2 = IS_DEL(get_dst(edges[index]));
	while (rewind1 != rewind2) {
	    usleep(10);
            rewind2 = IS_DEL(get_dst(edges[index]));
	}
	
        src = edges[index].src_id;
        vert1_id = TO_VID(src);

        //gather high level info for 1
        range = (vert1_id >> bit_shift);
        vid_range[range] += 1;
    }
}

template <class T>
void edge_shard_t<T>::estimate_classify_snb(vid_t* vid_range, vid_t bit_shift) 
{
    sid_t src, dst;
    vid_t vert1_id, vert2_id;
    vid_t m, n;
    edgeT_t<T>* edges = blog->blog_beg;
    index_t index;
    vid_t   snb_index;
    #pragma omp for schedule(static)
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        src = edges[index].src_id;
        dst = get_dst(edges[index]);
        
        vert1_id = TO_VID(src);
        vert2_id = TO_VID(dst);

        //gather high level info for 1
        m = (vert1_id >> bit_shift);
        n = (vert2_id >> bit_shift);
        
        snb_index = (m << RANGE_2DSHIFT) + n;
        assert(snb_index < RANGE_COUNT);
        vid_range[snb_index] += 1;
    }
}

template <class T>
void edge_shard_t<T>::estimate_classify_runi(vid_t* vid_range, vid_t bit_shift) 
{
    sid_t dst;
    vid_t vert_id;
    vid_t range;
    edgeT_t<T>* edges = blog->blog_beg;
    index_t index;
    bool rewind1, rewind2;

    #pragma omp for
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        rewind1 = !((i >> BLOG_SHIFT) & 0x1);
        rewind2 = IS_DEL(get_dst(edges[index]));
	while (rewind1 != rewind2) {
	    usleep(10);
            rewind2 = IS_DEL(get_dst(edges[index]));
	}
        dst = TO_SID(get_dst(edges[index]));
        vert_id = TO_VID(dst);

        //gather high level info for 1
        range = (vert_id >> bit_shift);
        vid_range[range]++;
    }
}

template <class T>
void edge_shard_t<T>::prefix_sum(global_range_t<T>* global_range, thd_local_t* thd_local,
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
void edge_shard_t<T>::work_division(global_range_t<T>* global_range, thd_local_t* thd_local,
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
void edge_shard_t<T>::classify(vid_t* vid_range, vid_t* vid_range_in, vid_t bit_shift, vid_t bit_shift_in, 
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
        dst = TO_SID(get_dst(edges[index]));
       
        vert1_id = TO_VID(src);
        vert2_id = TO_VID(dst);
        
        range = (vert1_id >> bit_shift);
        edge = global_range[range].edges + vid_range[range];
        vid_range[range] += 1;
        //assert(edge - global_range[range].edges < global_range[range].count);
        edge->src_id = src;
        set_dst(edge, dst);
        set_weight(edge, edges[index].dst_id);
        
        range = (vert2_id >> bit_shift_in);
        edge = global_range_in[range].edges + vid_range_in[range];
        vid_range_in[range] += 1;
        //assert(edge - global_range_in[range].edges < global_range_in[range].count);
	if (!IS_DEL(src)) {
            edge->src_id = dst;
            set_dst(edge, src);
	} else {
            edge->src_id = DEL_SID(dst);
            set_dst(edge, UNDEL_SID(src));
	}
        set_weight(edge, edges[index].dst_id);
    }
}

template <class T>
void edge_shard_t<T>::classify_runi(vid_t* vid_range, vid_t bit_shift, global_range_t<T>* global_range)
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
        dst = TO_SID(get_dst(edges[index]));
        vert_id = TO_VID(dst);
        
        range = (vert_id >> bit_shift);
        edge = global_range[range].edges + vid_range[range]++;
       	
        if (!IS_DEL(src)) {
            edge->src_id = dst;
            set_dst(edge, src);
	} else {	
            edge->src_id = DEL_SID(dst);
            set_dst(edge, UNDEL_SID(src));
	}
        set_weight(edge, edges[index].dst_id);
    }
}

template <class T>
void edge_shard_t<T>::classify_uni(vid_t* vid_range, vid_t bit_shift, global_range_t<T>* global_range)
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
	set_dst(edge, TO_SID(get_dst(edge)));
    }
}

template <class T>
void edge_shard_t<T>::classify_snb(vid_t* vid_range, vid_t bit_shift, global_range_t<T>* global_range)
{
    sid_t src, dst;
    vid_t vert1_id, vert2_id;
    vid_t m, n, range = 0;
    edgeT_t<T>* edge;
    edgeT_t<T>* edges = blog->blog_beg;
    index_t index;

    #pragma omp for schedule(static)
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & blog->blog_mask);
        src = edges[index].src_id;
        dst = get_dst(edges[index]);
       
        vert1_id = TO_VID(src);
        vert2_id = TO_VID(dst);
        
        m = (vert1_id >> bit_shift);
        n = (vert2_id >> bit_shift);
        range = (m << RANGE_2DSHIFT) + n;
        
        edge = global_range[range].edges + vid_range[range];
        vid_range[range] += 1;
        //assert(edge - global_range[range].edges < global_range[range].count);
        edge->src_id = src;
        edge->dst_id = edges[index].dst_id;
        set_weight(edge, edges[index].dst_id);
    }
}

template <class T>
void edge_shard_t<T>::archive(onegraph_t<T>** sgraph, global_range_t<T>* global_range, vid_t j_start, vid_t j_end, snapid_t snap_id) 
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
