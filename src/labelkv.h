#pragma once
#include "type.h"

//generic classes for label.

template <class T>
class lkv_t {
 public:
    T* kv;
    sid_t super_id;
    vid_t max_vcount;

    inline lkv_t() {
        kv = 0;
        super_id = 0;
        max_vcount = 0;
    }
    
    inline void setup(tid_t tid) {
        if ( 0 == super_id ) {
            super_id = g->get_type_scount(tid);
            vid_t v_count = TO_VID(super_id);
            max_vcount = (v_count << 1);
            kv = (T*)calloc(sizeof(T), max_vcount);
        } else {
            super_id = g->get_type_scount(tid);
            vid_t v_count = TO_VID(super_id);
            if (max_vcount < v_count) {
                assert(0);
            }
        }
    }
};

//lgraph doesn't need super id stuff
//as this graphs' src id may be an enum (e.g.)
typedef beg_pos_t lgraph_t;

//base class for label property store.
template <class T>
class pkv_t: public cfinfo_t {
 public:
    lkv_t<T>** lkv_out;
    lgraph_t*  lgraph_in;
    vid_t*     nebr_count;
    
 public:
    inline pkv_t() {
        lkv_out = 0;
        lgraph_in = 0;
        nebr_count = 0;
    }
    lgraph_t* prep_lgraph(index_t ecount);
    lkv_t<T>** prep_lkv();
    void fill_kv_out();
    void fill_adj_list_kv(lkv_t<T>** lkv_out, lgraph_t* lgraph_in);
    
    void prep_lgraph_internal(lgraph_t* lgraph_in, index_t ecount);
    void store_lgraph(lgraph_t* lgraph_in, string dir, string postfix);
    void calc_edge_count(lgraph_t* lgraph_in);
    void make_graph_baseline();

    void print_raw_dst(tid_t tid, vid_t vid, propid_t pid = 0);
};


/**************/
//super bins memory allocation
template<class T>
lkv_t<T>** pkv_t<T>::prep_lkv()
{
    sflag_t    flag = flag1;
    tid_t      pos  = 0;
    tid_t   t_count = g->get_total_types();
    
    if (0 == lkv_out) {
        lkv_out = (lkv_t<T>**) calloc (sizeof(lkv_t<T>*), t_count);
    }

    for(tid_t i = 0; i < flag1_count; i++) {
        pos = __builtin_ctz(flag);
        flag ^= (1L << pos);//reset that position
        if (0 == lkv_out[pos]) {
            lkv_out[pos] = new lkv_t<T>;
            lkv_out[pos]->setup(pos);
        }
    }
    return lkv_out;
}

template <class T>
lgraph_t* pkv_t<T>::prep_lgraph(index_t enumcount)
{
    lgraph_t* lgraph  = (beg_pos_t*) calloc (sizeof(beg_pos_t), enumcount);
    nebr_count  = (vid_t*) calloc (sizeof(vid_t), enumcount);
    return lgraph;
}

template <class T>
void pkv_t<T>::prep_lgraph_internal(lgraph_t* lgraph_in, index_t ecount)
{
    index_t     prefix = 0;
    beg_pos_t*  beg_pos = lgraph_in;
    
    for (vid_t j = 0; j < ecount; ++j) {
        beg_pos[j].setup(nebr_count[j]);
    }
}

template <class T>
void pkv_t<T>::calc_edge_count(lgraph_t* lgraph_in)
{
    sid_t dst;
    edgeT_t<T>*   edges;
    index_t   count;
    
    for (int j = 0; j <= batch_count; ++j) { 
        edges = (edgeT_t<T>*)batch_info[j].buf;
        count = batch_info[j].count;
    
        for (index_t i = 0; i < count; ++i) {
            dst = edges[i].dst_id;
            nebr_count[dst] += 1;
        }
    }
}

template<class T>
void pkv_t<T>::fill_kv_out()
{
    sid_t src;
    T dst;
    vid_t     vert1_id;
    tid_t     src_index;
    edgeT_t<T>*   edges;
    index_t   count;

    for (int j = 0; j <= batch_count; ++j) { 
        edges = (edgeT_t<T>*)batch_info[j].buf;
        count = batch_info[j].count;
    
        for (index_t i = 0; i < count; ++i) {
            src = edges[i].src_id;
            dst = edges[i].dst_id;
            
            vert1_id = TO_VID(src);
            src_index = TO_TID(src);
            lkv_out[src_index]->kv[vert1_id] = dst;
        }
    }
}

template<class T>
void pkv_t<T>::fill_adj_list_kv(lkv_t<T>** lkv_out, lgraph_t* lgraph_in)
{
    sid_t src;
    T         dst;
    vid_t     vert1_id;
    tid_t     src_index;
    beg_pos_t* beg_pos_in = lgraph_in;
    edgeT_t<T>*   edges;
    index_t   count;

    for (int j = 0; j <= batch_count; ++j) { 
        edges = (edgeT_t<T>*)batch_info[j].buf;
        count = batch_info[j].count;
    
        for (index_t i = 0; i < count; ++i) {
            src = edges[i].src_id;
            dst = edges[i].dst_id;
            vert1_id = TO_VID(src);
            src_index = TO_TID(src); 
            
            lkv_out[src_index]->kv[vert1_id] = dst;
            beg_pos_in[dst].add_nebr(src);
        }
    }
}

template<class T>
void pkv_t<T>::make_graph_baseline()
{
    if (batch_info[batch_count].count == 0) return;

    flag1_count = __builtin_popcountll(flag1);
    
    //super bins memory allocation
    prep_lkv();

    //populate and get the original count back
    //handle kv_out as well.
    fill_kv_out();

    cleanup();
}

template <class T>
void pkv_t<T>::store_lgraph(lgraph_t* lgraph_in, string dir, string postfix)
{
    //base name using relationship type
    /*
    string basefile = dir + p_name;
    string file = basefile + "beg_pos";
    FILE* f;
    */
    
}
template <class T>
void pkv_t<T>::print_raw_dst(tid_t tid, vid_t vid, propid_t pid /* = 0 */)
{
    cout << lkv_out[tid]->kv[vid];
}

#include "numkv.h"
#include "enumkv.h"


typedef enumkv_t<uint8_t> enum8kv_t;
typedef numkv_t<uint8_t>  uint8kv_t;
typedef numkv_t<uint64_t> uint64kv_t;


