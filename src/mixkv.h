#pragma once
#include "oneblob.h"

template <class T>
class mixkv_t : public cfinfo_t {
    blobkv_t<T>**   kv_out;
    
 public:
    inline mixkv_t() {
        kv_out = 0;
    }
    
    status_t batch_edge(edgeT_t<T>& edge);
    status_t batch_update(const string& src, const string& dst, propid_t pid = 0);
    
    void prep_graph_baseline();
    void make_graph_baseline();
    void store_graph_baseline(bool clean = true);
    void read_graph_baseline();
    void file_open(const string& dir, bool trunc);
    inline T* get_value(sid_t sid) {
        vid_t vid = TO_VID(sid);
        tid_t tid = TO_TID(sid);
        return kv_out[tid]->get_value(vid);
    }
    char* alloc_mem(sid_t sid, index_t sz, index_t& offset) {
        vid_t vid = TO_VID(sid);
        tid_t tid = TO_TID(sid);
        return kv_out[tid]->alloc_mem(vid, offset);
    }
};

template <class T>
status_t mixkv_t<T>::batch_edge(edgeT_t<T>& edge)
{
    vid_t vid = TO_VID(edge.src_id);
    tid_t tid = TO_TID(edge.src_id);
    kv_out[tid]->set_value(vid, edge.dst_id);
    return eOK;
}

template <class T>
status_t mixkv_t<T>::batch_update(const string& src, const string& dst, propid_t pid /* = 0*/)
{
    /*edgeT_t<char*> edge; 
    edge.src_id = g->get_sid(src.c_str());
    edge.dst_id = const_cast<char*>(src.c_str());
    batch_edge(edge);
    */
    assert(0);
    return eOK;
}

template <class T>
void mixkv_t<T>::make_graph_baseline()
{
}

template <class T>
void mixkv_t<T>::prep_graph_baseline()
{
    sflag_t    flag = flag1;
    tid_t   t_count = g->get_total_types();
    
    if (0 == kv_out) {
        kv_out = (blobkv_t<T>**) calloc (sizeof(blobkv_t<T>*), t_count);
    }

    if (flag == 0) {
        flag1_count = g->get_total_types();
        for(tid_t i = 0; i < flag1_count; i++) {
            if (0 == kv_out[i]) {
                kv_out[i] = new blobkv_t<T>;
                kv_out[i]->setup(i);
            }
        } 
        return;
    } 
    
    tid_t      pos  = 0;
    flag1_count = __builtin_popcountll(flag);
    for(tid_t i = 0; i < flag1_count; i++) {
        pos = __builtin_ctzll(flag);
        flag ^= (1L << pos);//reset that position
        if (0 == kv_out[pos]) {
           kv_out[pos] = new blobkv_t<T>;
           kv_out[pos]->setup(pos);
        }
    }
}

template <class T>
void mixkv_t<T>::file_open(const string& dir, bool trunc)
{
    if (kv_out == 0) return;
    
    tid_t       t_count = g->get_total_types();
    
    //base name using relationship type
    string basefile;
    if (col_count) {
        basefile = dir + col_info[0]->p_name;
    } else {
        basefile = dir;
    }

    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        if (kv_out[i] == 0) continue;
        kv_out[i]->file_open(basefile, trunc);
    }
}

template <class T>
void mixkv_t<T>::store_graph_baseline(bool clean /*=false*/)
{
    if (kv_out == 0) return;
    
    tid_t       t_count = g->get_total_types();
    
    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        if (kv_out[i] == 0) continue;

        kv_out[i]->handle_write();
    }
}

template <class T>
void mixkv_t<T>::read_graph_baseline()
{
    tid_t   t_count = g->get_total_types();
    if (0 == kv_out) {
        kv_out  = (blobkv_t<T>**) calloc (sizeof(blobkv_t<T>*), t_count);
    }
    
    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        if (kv_out[i] == 0) continue;
        kv_out[i]->handle_read();
    }
}
