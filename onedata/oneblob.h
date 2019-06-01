
#pragma once
#include "type.h"

template <class T>
class blobkv_t {
    T*    kv;
    tid_t tid;
    vid_t max_vcount;
    
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
    void setup(tid_t tid, vid_t v_count);

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
void blobkv_t<T>::setup(tid_t t, vid_t v_count) 
{
    tid = t;
    max_vcount = v_count;
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
