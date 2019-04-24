#pragma once
#include "graph.h"
#include "prop_encoder.h"
#include "sgraph.h"

//edge properties are more dynamic, and are stored separately. Adj-list contains edge id now.
//T is only weight type here.
template <class T>
class weight_graph_t : public cfinfo_t {
 
 public: 
    
     //graphs with edge id
     union {
        onekv_t<lite_edge_t>** skv_out;
        onegraph_t<lite_edge_t>** sgraph_out;
    };
    union {
        onekv_t<lite_edge_t>** skv_in;
        onegraph_t<lite_edge_t>** sgraph_in;
    };
    
    //weight store, T is the weight structure
    onegraph_t<T>**  wgraph;
    
    //circular edge log buffer
    blog_t<dst_weight_t<T>>*  blog;

 public:
    weight_graph_t() {
        sgraph_out = 0;
        sgraph_in  = 0;
        blog = new blog_t<T>;
    }
    
    inline void alloc_edgelog(index_t count) {
        blog->alloc_edgelog(count);
    }

    status_t batch_edge(edgeT_t<dst_weight_t<T> edge) {
        status_t ret = eOK;
        index_t index = blog->log(edge);
        index_t size = ((index - blog->blog_marker) & BATCH_MASK);
        
        //inform archive thread about threshold being crossed
        if ((0 == size) && (index != blog->blog_marker)) {
            create_marker(index + 1);
            //cout << "Will create a snapshot now " << endl;
            ret = eEndBatch;
        } 
        return ret; 
    }
    
    void create_marker(index_t marker) {
        pthread_mutex_lock(&g->snap_mutex);
        index_t m_index = __sync_fetch_and_add(&q_head, 1L);
        q_beg[m_index % q_count] = marker;
        pthread_cond_signal(&g->snap_condition);
        pthread_mutex_unlock(&g->snap_mutex);
        //cout << "Marker queued. position = " << m_index % q_count << " " << marker << endl;
    } 
    
    //called from snap thread 
    status_t move_marker(index_t& snap_marker) {
        pthread_mutex_lock(&g->snap_mutex);
        index_t head = q_head;
        //Need to read marker and set the blog_marker;
        if (q_tail == head) {
            pthread_mutex_unlock(&g->snap_mutex);
            //cout << "Marker NO dequeue. Position = " << head <<  endl;
            return eNoWork;
        }
        
        index_t m_index = head - 1;
        index_t marker = q_beg[m_index % q_count];
        q_tail = head;
        blog->blog_marker = marker;
        snap_marker = blog->blog_marker;
        
        pthread_mutex_unlock(&g->snap_mutex);
        //cout << "Marker dequeue. Position = " << m_index % q_count << " " << marker << endl;
        return eOK;
    }

    index_t update_marker() { 
        return blog->update_marker();
    }
 
 public:
    void prep_sgraph(sflag_t ori_flag, onegraph_t<lite_edge_t>** a_sgraph);
    void prep_skv(sflag_t ori_flag, onekv_t<lite_edge_t>** a_skv);
    void prep_weight_store(sflag_t ori_flag);
    
    void make_graph_d(); 
    void make_graph_u();

    void estimate_classify (vid_t* vid_range, vid_t* vid_range_in, vid_t bit_shift);
    void prefix_sum (global_range_t<weight_dst_t<T>>* global_range, thd_local_t* thd_local,
                    vid_t range_count, vid_t thd_count, edgeT_t<T>* edge_buf);
    void work_division (global_range_t<weight_dst_t<T>>* global_range, thd_local_t* thd_local,
                    vid_t range_count, vid_t thd_count, index_t equal_work);
    void classify (vid_t* vid_range, vid_t* vid_range_in, vid_t bit_shift, 
            global_range_t<weight_dst_t<T>>* global_range, global_range_t<weight_dst_t<T>>* global_range_in);
    
    void calc_degree_noatomic (onegraph_t<lite_edge_t>** sgraph, global_range_t<weight_dst_t<T>>* global_range, 
                      vid_t j_start, vid_t j_end);
    virtual void fill_adjlist_noatomic (onegraph_t<lite_edge_t>** sgraph, global_range_t<weight_dst_t<T>>* global_range, 
                      vid_t j_start, vid_t j_end);

    void store_sgraph(onegraph_t<T>** sgraph, bool clean = false);
    void store_skv(onekv_t<T>** skv);
    
    void read_sgraph(onegraph_t<T>** sgraph);
    void read_skv(onekv_t<T>** skv);
    
    void file_open_edge(const string& dir, bool trunc);
    void file_open_sgraph(onegraph_t<T>** sgraph, const string& odir, const string& postfix, bool trunc);
    void file_open_skv(onekv_t<T>** skv, const string& odir, const string& postfix, bool trunc);
};

template <class T>
class weight_ugraph: public weight_graph_t<T> {
 public:
    void prep_graph_baseline();
    void make_graph_baseline();
    void store_graph_baseline();
    void read_graph_baseline();
};

template <class T>
class weight_dgraph: public weight_graph_t<T> {
 public:
    void prep_graph_baseline();
    void make_graph_baseline();
    void store_graph_baseline();
    void read_graph_baseline();
};

template <class T>
void weight_graph_t<T>::prep_sgraph(sflag_t ori_flag, onegraph_t<lite_edge_t>** sgraph)
{
    tid_t   pos = 0;//it is tid
    
    sflag_t      flag = ori_flag;
    tid_t  flag_count = __builtin_popcountll(flag);
    
    for(tid_t i = 0; i < flag_count; i++) {
        pos = __builtin_ctzll(flag);
        flag ^= (1L << pos);//reset that position
        if (0 == sgraph[pos]) {
            sgraph[pos] = new onegraph_t<lite_edge_t>;
        }
        sgraph[pos]->setup(pos);
    }
}

template <class T>
void weight_graph_t<T>::prep_weight_store(sflag_t ori_flag)
{
    tid_t   pos = 0;//it is tid
    
    sflag_t      flag = ori_flag;
    tid_t  flag_count = __builtin_popcountll(flag);
    
    for(tid_t i = 0; i < flag_count; i++) {
        pos = __builtin_ctzll(flag);
        flag ^= (1L << pos);//reset that position
        if (0 == wgraph[pos]) {
            wgraph[pos] = new onegraph_t<T>;
        }
        wgraph[pos]->setup_wstore(pos);
    }
}

template <class T>
void weight_graph_t<T>::store_sgraph(onegraph_t<lite_edge_t>** sgraph, bool clean /*= false*/)
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
void weight_graph_t<T>::file_open_edge(const string& dir, bool trunc)
{
    string filename = dir + col_info[0]->p_name; 
    string wtfile = filename + ".elog";
    if (trunc) {
        wtf = open(wtfile.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
    } else {
        wtf = open(wtfile.c_str(), O_RDWR|O_CREAT, S_IRWXU);
    }
}

template <class T>
void weight_graph_t<T>::file_open_sgraph(onegraph_t<lite_edge_t>** sgraph, const string& dir, const string& postfix, bool trunc)
{
    if (sgraph == 0) return;
    
    char name[8];
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

        filename = basefile + name + "w" + postfix;
        wgraph[i]->file_open(filename, trunc);
    }
}

template <class T>
void weight_graph_t<T>::read_sgraph(onegraph_t<lite_edge_t>** sgraph)
{
    if (sgraph == 0) return;
    
    tid_t    t_count = g->get_total_types();
    
    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        if (sgraph[i] == 0) continue;
        sgraph[i]->read_vtable();
        //sgraph[i]->read_stable(stfile);
        //sgraph[i]->read_etable(etfile);
    }
}

template <class T>
void weight_graph_t<T>::estimate_classify(vid_t* vid_range, vid_t* vid_range_in, vid_t bit_shift) 
{
    sid_t src, dst;
    vid_t vert1_id, vert2_id;
    vid_t range;
    edgeT_t<weight_dst_t<T>>* edges = blog->blog_beg;
    index_t index;

    #pragma omp for
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & BLOG_MASK);
        src = edges[index].src_id;
        dst = get_sid(edges[index].dst_id);
        
        vert1_id = TO_VID(src);
        vert2_id = TO_VID(dst);

        //gather high level info for 1
        range = (vert1_id >> bit_shift);
        vid_range[range]++;
        
        //gather high level info for 2
        range = (vert2_id >> bit_shift);
        vid_range_in[range]++;
    }
}


template <class T>
void weight_graph_t<T>::prefix_sum(global_range_t<weight_dst_t<T>>* global_range, thd_local_t* thd_local,
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
        if (total != 0)
            global_range[i].edges = (edgeT_t<T>*)malloc(sizeof(edgeT_t<weight_dst_t<T>>)*total);
    }
}

template <class T>
void weight_graph_t<T>::work_division(global_range_t<weight_dst_t<T>>* global_range, thd_local_t* thd_local,
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
void weight_graph_t<T>::classify(vid_t* vid_range, vid_t* vid_range_in, vid_t bit_shift, 
            global_range_t<weight_dst_t<T>>* global_range, global_range_t<weight_dst_t<T>>* global_range_in)
{
    sid_t src, dst;
    vid_t vert1_id, vert2_id;
    vid_t range = 0;
    edgeT_t<weight_dst_t<T>>* edge;
    edgeT_t<weight_dst_t<T>>* edges = blog->blog_beg;
    index_t index;

    #pragma omp for
    for (index_t i = blog->blog_tail; i < blog->blog_marker; ++i) {
        index = (i & BLOG_MASK);
        src = edges[index].src_id;
        dst = get_sid(edges[index].dst_id);
        
        vert1_id = TO_VID(src);
        vert2_id = TO_VID(dst);
        
        range = (vert1_id >> bit_shift);
        edge = global_range[range].edges + vid_range[range]++;
        edge->src_id = src;
        edge->dst_id = edges[index].dst_id;
        
        range = (vert2_id >> bit_shift);
        edge = global_range_in[range].edges + vid_range_in[range]++;
        edge->src_id = dst;
        set_dst(edge, src);
        set_weight(edge, edges[index].dst_id);
    }
}

template <class T>
void weight_graph_t<T>::calc_degree_noatomic(onegraph_t<T>** wgraph, global_range_t<weight_dst_t<T>>* global_range, vid_t j_start, vid_t j_end) 
{
    index_t total = 0;
    edgeT_t<weight_dst_t<T>>* edges = 0;
    tid_t src_index;
    sid_t src;
    vid_t vert1_id;

    for (vid_t j = j_start; j < j_end; ++j) {
        total = global_range[j].count;
        edges = global_range[j].edges;
        
        for (index_t i = 0; i < total; ++ i) {
            src = edges[i].src_id;
            src_index = TO_TID(src);
            vert1_id = TO_VID(src);

            //XXX search its uniqueness

            if (!IS_DEL(src)) { 
                sgraph[src_index]->increment_count_noatomic(vert1_id);
            } else { 
                sgraph[src_index]->decrement_count_noatomic(vert1_id);
            }
        }
    }
}

template <class T>
void weight_graph_t<T>::fill_adjlist_noatomic(onegraph_t<lite_edge_t>** sgraph, global_range_t<weight_dst_t<T>>* global_range, vid_t j_start, vid_t j_end) 
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
        
        for (index_t i = 0; i < total; ++ i) {
            src = edges[i].src_id;
            dst = edges[i].dst_id;
            src_index = TO_TID(src);
            vert1_id = TO_VID(src);

            sgraph[src_index]->add_nebr_noatomic(vert1_id, dst);
        }
    }
}

template <class T>
void weight_graph_t<T>::make_graph_d() 
{
    if (blog->blog_tail >= blog->blog_marker) return;
    
    vid_t v_count = sgraph_out[0]->get_vcount();
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

    global_range_t<weight_dst_t<T>>* global_range = (global_range_t<weight_dst_t<T>>*)calloc(
                            sizeof(global_range_t<weight_dst_t<T>>), range_count);
    
    global_range_t<weight_dst_t<T>>* global_range_in = (global_range_t<weight_dst_t<T>>*)calloc(
                            sizeof(global_range_t<weight_dst_t<T>>), range_count);
    
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

        double start = mywtime();

        //Get the count for classification
        this->estimate_classify(vid_range, vid_range_in, bit_shift);
        
        this->prefix_sum(global_range, thd_local, range_count, thd_count, edge_buf_out);
        this->prefix_sum(global_range_in, thd_local_in, range_count, thd_count, edge_buf_in);
        #pragma omp barrier 
        
        //Classify
        this->classify(vid_range, vid_range_in, bit_shift, global_range, global_range_in);
        
        #pragma omp master 
        {
            //double end = mywtime();
            //cout << " classify " << end - start << endl;
            this->work_division(global_range, thd_local, range_count, thd_count, edge_count);
            //this->work_division(global_range_in, thd_local_in, range_count, thd_count, edge_count);
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

        //degree count
        this->calc_degree_noatomic(sgraph_out, global_range, j_start, j_end);
        this->calc_degree_noatomic(sgraph_in, global_range_in, j_start_in, j_end_in);
		print(" Degree = ", start);
        
        #ifdef BULK
        vid_t vid_start = (j_start << bit_shift);
        vid_t vid_end = (j_end << bit_shift);
        if (vid_end > v_count) vid_end = v_count;
        sgraph_out[0]->setup_adjlist_noatomic(vid_start, vid_end);

        vid_t vid_start_in = (j_start_in << bit_shift);
        vid_t vid_end_in = (j_end_in << bit_shift);
        if (vid_end_in > v_count) vid_end_in = v_count;
        sgraph_in[0]->setup_adjlist_noatomic(vid_start_in, vid_end_in);
		print(" adj-list setup =", start);
        #endif
        
        //fill adj-list
        this->fill_adjlist_noatomic(sgraph_out, global_range, j_start, j_end);
        print(" adj-list filled = ", start);
        this->fill_adjlist_noatomic(sgraph_in, global_range_in, j_start_in, j_end_in);
        print(" adj-list in filled = ", start);
        
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
void weight_graph_t<T>::make_graph_u() 
{
    if (blog->blog_tail >= blog->blog_marker) return;
    
    tid_t src_index = TO_TID(blog->blog_beg[0].src_id);
    vid_t v_count = sgraph[src_index]->get_vcount();

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

    global_range_t<weight_dst_t<T>>* global_range = (global_range_t<weight_dst_t<T>>*)calloc(
                            sizeof(global_range_t<weight_dst_t<T>>), range_count);
    
    thd_local_t* thd_local = (thd_local_t*) calloc(sizeof(thd_local_t), thd_count);  
    index_t total_edge_count = blog->blog_marker - blog->blog_tail ;
    index_t edge_count = ((total_edge_count << 1)*1.15)/(thd_count);
    
    //alloc_edge_buf(total_edge_count);
    double start = mywtime();

    #pragma omp parallel num_threads(thd_count)
    {
        int tid = omp_get_thread_num();
        vid_t* vid_range = (vid_t*)calloc(sizeof(vid_t), range_count); 
        thd_local[tid].vid_range = vid_range;

        //Get the count for classification
        this->estimate_classify(vid_range, vid_range, bit_shift);
        
        this->prefix_sum(global_range, thd_local, range_count, thd_count, edge_buf_out);
        #pragma omp barrier 
        
        //Classify
        this->classify(vid_range, vid_range, bit_shift, global_range, global_range);
        #pragma omp master 
        {
            print(" classify = ", start);
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
        
        //free the memory
        free(vid_range);
        #pragma omp barrier 
        print(" adj-list filled = ", start);
        
        //free the memory
        #pragma omp for schedule (static)
        for (vid_t i = 0; i < range_count; ++i) {
            if (global_range[i].edges)
                free(global_range[i].edges);
        }
    }
    //free_edge_buf();
    free(global_range);
    free(thd_local);
    blog->blog_tail = blog->blog_marker;  
    //cout << "setting the tail to" << blog->blog_tail << endl;
}

template <class T>
void weight_graph_t<T>::make_on_classify(onegraph_t<T>** wgraph, global_range_t<weight_dst_t<T>>* global_range, vid_t j_start, vid_t j_end, vid_t bit_shift)
{
    //degree count
    this->calc_degree_noatomic(wgraph, global_range, j_start, j_end);
    print(" Degree = ", start);

    //fill adj-list
    this->fill_adjlist_noatomic(wgraph, sgraph, global_range, j_start, j_end);
}
/*
class p_many2one_t: public weight_graph_t {
 public:
    //lite_skv_t**     skv_out;
    //lite_sgraph_t**  sgraph_in;

 public:
    static cfinfo_t* create_instance();
    
    void incr_count(sid_t src, sid_t dst, int del = 0);
    void prep_graph_baseline();
    void calc_degree();
    void make_graph_baseline();
    void store_graph_baseline();
    void read_graph_baseline();
    
    status_t transform(srset_t* iset, srset_t* oset, direction_t direction);
    virtual status_t extend(srset_t* iset, srset_t* oset, direction_t direction);
};

class p_one2one_t: public weight_graph_t {
 public:
    //lite_skv_t**   skv_in;
    //lite_skv_t**   skv_out;

 public:
    static cfinfo_t* create_instance();
    
    void incr_count(sid_t src, sid_t dst, int del = 0);
    void prep_graph_baseline();
    void calc_degree();
    void make_graph_baseline();
    void store_graph_baseline();
    void read_graph_baseline();
    
    status_t transform(srset_t* iset, srset_t* oset, direction_t direction);
    virtual status_t extend(srset_t* iset, srset_t* oset, direction_t direction);
};

class p_one2many_t: public weight_graph_t {
 public:
    //lite_sgraph_t**   sgraph_out;
    //lite_skv_t**      skv_in;

 public:
    static cfinfo_t* create_instance();
    
    void incr_count(sid_t src, sid_t dst, int del = 0);
    void prep_graph_baseline();
    void calc_degree();
    void make_graph_baseline();
    void store_graph_baseline();
    void read_graph_baseline();
    
    status_t transform(srset_t* iset, srset_t* oset, direction_t direction);
    virtual status_t extend(srset_t* iset, srset_t* oset, direction_t direction);
};
*/
/************* Semantic graphs  *****************/
template <class T> 
void weight_dgraph<T>::prep_graph_baseline()
{
    alloc_edgelog(BLOG_SIZE);
    flag1_count = __builtin_popcountll(flag1);
    flag2_count = __builtin_popcountll(flag2);

    //super bins memory allocation
    tid_t   t_count = g->get_total_types();
    
    if (0 == sgraph_out) {
        sgraph_out  = (onegraph_t<T>**) calloc (sizeof(onegraph_t<T>*), t_count);
    }
    prep_sgraph(flag1, sgraph_out);    
    
    if (0 == sgraph_in) {
        sgraph_in  = (onegraph_t<T>**) calloc (sizeof(onegraph_t<T>*), t_count);
    }
    prep_sgraph(flag2, sgraph_in);
}

//We assume that no new vertex type is defined
template <class T> 
void weight_dgraph<T>::make_graph_baseline()
{
    this->make_graph_d();
}

template <class T> 
void weight_dgraph<T>::store_graph_baseline(bool clean)
{
    //#pragma omp parallel num_threads(THD_COUNT)
    {
    store_sgraph(sgraph_out, clean);
    store_sgraph(sgraph_in, clean);
    }
}

template <class T> 
void weight_dgraph<T>::file_open(const string& odir, bool trunc)
{
    this->file_open_edge(odir, trunc);
    string postfix = "out";
    file_open_sgraph(sgraph_out, odir, postfix, trunc);
    postfix = "in";
    file_open_sgraph(sgraph_in, odir, postfix, trunc);
}

template <class T> 
void weight_dgraph<T>::read_graph_baseline()
{
    tid_t   t_count    = g->get_total_types();
    
    if (0 == sgraph_out) {
        sgraph_out  = (onegraph_t<T>**) calloc (sizeof(onegraph_t<T>*), t_count);
    }
    read_sgraph(sgraph_out);
    
    if (0 == sgraph_in) {
        sgraph_in  = (onegraph_t<T>**) calloc (sizeof(onegraph_t<T>*), t_count);
    }
    read_sgraph(sgraph_in);
}

/*******************************************/
template <class T> 
void weight_ugraph<T>::prep_graph_baseline()
{
    flag1 = flag1 | flag2;
    flag2 = flag1;

    flag1_count = __builtin_popcountll(flag1);
    flag2_count = flag1_count;

    //super bins memory allocation
    tid_t   t_count = g->get_total_types();
    
    if (0 == sgraph) {
        sgraph  = (onegraph_t<T>**) calloc (sizeof(onegraph_t<T>*), t_count);
    }
    prep_sgraph(flag1, sgraph); 
}

template <class T> 
void weight_ugraph<T>::make_graph_baseline()
{
    this->make_graph_u();
}

template <class T> 
void weight_ugraph<T>::store_graph_baseline(bool clean)
{
    double start, end;
    start = mywtime(); 
    store_sgraph(sgraph, clean);
    end = mywtime();
    cout << "store graph time = " << end - start << endl;
}

template <class T> 
void weight_ugraph<T>::file_open(const string& odir, bool trunc)
{
    string postfix = "";
    file_open_sgraph(sgraph, odir, postfix, trunc);
}

template <class T> 
void weight_ugraph<T>::read_graph_baseline()
{
    tid_t   t_count = g->get_total_types();
    
    if (0 == sgraph) {
        sgraph  = (onegraph_t<T>**) calloc (sizeof(onegraph_t<T>*), t_count);
    }
    read_sgraph(sgraph);
}
