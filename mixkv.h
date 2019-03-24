#pragma once
#include "type.h"

template <class T>
class blobkv_t {
    T*    kv;
    tid_t tid;
    
    //edgetable file related log
    char*    log_beg;  //memory log pointer
    sid_t    log_count;//size of memory log
    sid_t    log_head; // current log write position
    sid_t    log_tail; //current log cleaning position
    sid_t    log_wpos; //Write this pointer for write persistency

    int    vtf;   //vertex table file
    int    etf;   //edge table file
    
public: 
    blobkv_t(); 
    void setup(tid_t tid);

    void set_value(vid_t vid, T& dst) {kv[vid] = dst;};
    T*   get_value(vid_t vid) {return kv + vid;};
    
    void handle_read();
    void handle_write();
    void file_open(const string& filename, bool trunc);
    char* alloc_mem(index_t sz, index_t& offset) {
        char* ptr = log_beg +  log_head;
        offset = log_head;
        log_head += sz;
        return ptr;
    }
};

template <class T>
blobkv_t<T>::blobkv_t()
{
    kv = 0;
    tid = 0;
    //super_id = 0;
    log_head = 0;
    log_tail = 0;
    log_wpos = 0;
    vtf = -1;
    etf = -1;
}

template <class T>
void blobkv_t<T>::setup(tid_t t) 
{
    tid = t;
    vid_t v_count = g->get_type_scount(tid);
    
    kv = (T*)calloc(sizeof(T), v_count);
    
    //everything is in memory
    log_count = (1L << 28);//256*8 MB
    if (posix_memalign((void**)&log_beg, 2097152, log_count*sizeof(char))) {
        //log_beg = (sid_t*)calloc(sizeof(sid_t), log_count);
        perror("posix memalign edge log");
    }
    log_head = 0;
    log_tail = 0;
    log_wpos = 0;
}

template <class T>
void blobkv_t<T>::handle_write()
{
    //Make a copy
    sid_t wpos = log_wpos;
    //Update the mark
    log_wpos = log_head;

    vid_t v_count = g->get_type_vcount(tid);
    if (log_head != 0) {
        write(etf, log_beg + wpos, log_head-wpos);
        //fwrite(log_beg+wpos, sizeof(char), log_head-wpos, etf);
        pwrite(vtf, kv, v_count*sizeof(T), 0);
    }
}

template <class T>
void blobkv_t<T>::file_open(const string& filename, bool trunc)
{
    char  file_ext[16];
    sprintf(file_ext,"%u",tid);
    
    string vtfile = filename + file_ext + ".vtable";
    string etfile = filename + file_ext + ".etable";
    if (trunc) {
		etf = open(etfile.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
        //etf = fopen(etfile.c_str(), "wb");//append/write + binary
        //vtf = fopen(vtfile.c_str(), "wb");
		vtf = open(vtfile.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);

    } else {
	    etf = open(etfile.c_str(), O_RDWR|O_CREAT, S_IRWXU);
        //etf = fopen(etfile.c_str(), "r+b");//append/write + binary
	    vtf = open(vtfile.c_str(), O_RDWR|O_CREAT, S_IRWXU);
        //vtf = fopen(vtfile.c_str(), "r+b");
    }
        
    assert(etf != -1);
    assert(vtf != -1);
}

template <class T>
void blobkv_t<T>::handle_read()
{
    //read etf
    off_t size = fsize(etf);
    if (size == -1L) { assert(0); }
    
    if (size != 0) {
        sid_t edge_count = size/sizeof(char);
        read(etf, log_beg, sizeof(char)*edge_count);

        log_head = edge_count;
        log_wpos = log_head;
    }
    
    //read vtf 
    size = fsize(vtf);
    if (size == -1L) { assert(0); }
    
    if (size != 0) {
        vid_t vcount = size/sizeof(sid_t);
        assert(vcount == g->get_type_vcount(tid));
        vid_t max_vcount = g->get_type_scount(tid);
        kv = (T*)calloc(sizeof(T), max_vcount);
        read(vtf, kv, sizeof(T)*vcount);
    }
}

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
