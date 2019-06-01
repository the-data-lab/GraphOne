#pragma once
#include "onenum.h"
#include "graph.h"

//generic number class
//
template <class T>
class numberkv_t : public cfinfo_t 
{
    kvT_t<T>** kv_out; 
    
  public:
    inline numberkv_t() {
        kv_out = 0;
    }
    static cfinfo_t* create_instance();
    status_t batch_edge(edgeT_t<T>& edge);
    status_t batch_update(const string& src, const string& dst, propid_t pid = 0);
    void make_graph_baseline();
    void store_graph_baseline(bool clean = false);
    void read_graph_baseline();
    void file_open(const string& dir, bool trunc);

    void prep_graph_baseline();
    void fill_kv_out();

    //inline const char* get_value(tid_t tid, vid_t vid) 
    inline T get_value(sid_t sid) {
        vid_t vid = TO_VID(sid);
        tid_t tid = TO_TID(sid);
        return kv_out[tid]->get_value(vid);
    }

    inline void print_raw_dst(tid_t tid, vid_t vid, propid_t pid = 0) {
        //cout << kv_out[tid]->kv[vid];
    }

};

template<class T>
status_t numberkv_t<T>::batch_update(const string& src, const string& dst, propid_t pid /*= 0*/)
{
    assert(0);
    /*
    edge<T> edge;
    edge.src_id = g->get_sid(src.c_str());
    edge.dst_id = strtol(dst.c_str(), NULL, 0);
    */
    return eOK;
}

template<class T>
status_t numberkv_t<T>::batch_edge(edgeT_t<T>& edge)
{
    vid_t vid = TO_VID(edge.src_id);
    tid_t tid = TO_TID(edge.src_id);
    kv_out[tid]->set_value(vid, edge.dst_id);
    return eOK;
}

template<class T>
void numberkv_t<T>::make_graph_baseline()
{
}

template<class T>
void numberkv_t<T>::prep_graph_baseline()
{
    sflag_t    flag = flag1;
    vid_t   max_vcount;
    tid_t   t_count = g->get_total_types();
    
    if (0 == kv_out) {
        kv_out = (kvT_t<T>**) calloc (sizeof(kvT_t<T>*), t_count);
    }
    if (flag == 0) {
        flag1_count = g->get_total_types();
        for(tid_t i = 0; i < flag1_count; i++) {
            if (0 == kv_out[i]) {
                max_vcount = g->get_type_scount(i);
                kv_out[i] = new kvT_t<T>;
                kv_out[i]->setup(i, max_vcount);
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
            max_vcount = g->get_type_scount(pos);
            kv_out[pos] = new kvT_t<T>;
            kv_out[pos]->setup(pos, max_vcount);
        }
    }
}

template<class T>
void numberkv_t<T>::file_open(const string& dir, bool trunc)
{
    if (kv_out == 0) return;
    
    tid_t       t_count = g->get_total_types();
    
    //base name using relationship type
    string basefile;
    if (this->col_count) {
        basefile = dir + col_info[0]->p_name;
    } else {
        basefile = dir;
    }

    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        if (kv_out[i] == 0) continue;
        kv_out[i]->file_open(basefile, trunc);
    }
    string etfile = basefile + ".str";
    this->mem.file_open(etfile.c_str(), trunc);
}

template<class T>
void numberkv_t<T>::store_graph_baseline(bool clean /*=false*/)
{
    if (kv_out == 0) return;
    
    tid_t       t_count = g->get_total_types();
    
    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        if (kv_out[i] == 0) continue;

        kv_out[i]->persist_vlog();
    }
    this->mem.handle_write();
}

template<class T>
void numberkv_t<T>::read_graph_baseline()
{
    tid_t   t_count = g->get_total_types();
    if (0 == kv_out) {
        kv_out  = (kvT_t<T>**) calloc (sizeof(kvT_t<T>*), t_count);
    }
    
    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        if (kv_out[i] == 0) continue;
        kv_out[i]->read_vtable();
    }
    this->mem.handle_read();
}
