#pragma once

#include <string>
#include "type.h"
#include "graph.h"

using std::string;


template <class T>
class kvT_t {
 public:
    T* kv;
    tid_t  tid;
    vid_t  max_vcount;
    int    vtf;   //vertex table file

 public:
    kvT_t(); 
 
 public: 
    void setup(tid_t tid, vid_t v_count); 
    void set_value(vid_t vid, T& value); 
    T get_value(vid_t vid);
    void persist_vlog();
    void read_vtable();
    void read_etable();
    void file_open(const string& filename, bool trunc);
};

template <class T>
kvT_t<T>::kvT_t()
{
    kv = 0;
    tid = 0;
    vtf = -1;
}

template <class T>
void kvT_t<T>::set_value(vid_t vid, T& value)
{
    kv[vid] = value;
}

template <class T>
T kvT_t<T>::get_value(vid_t vid)
{
    return kv[vid];
}

template <class T>
void kvT_t<T>::setup(tid_t t, vid_t v_count) 
{
    tid = t;
    max_vcount = v_count;
    kv = (T*)calloc(sizeof(T), v_count);
}

template <class T>
void kvT_t<T>::persist_vlog()
{
    vid_t v_count = g->get_type_vcount(tid);
    if (v_count != 0) {
        pwrite(vtf, kv, v_count*sizeof(sid_t), 0);
    }
}

template <class T>
void kvT_t<T>::file_open(const string& filename, bool trunc)
{
    char  file_ext[16];
    sprintf(file_ext,"%u",tid);
    
    string vtfile = filename + file_ext + ".vtable";
    if (trunc) {
        //vtf = fopen(vtfile.c_str(), "wb");
		vtf = open(vtfile.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);

    } else {
	    vtf = open(vtfile.c_str(), O_RDWR|O_CREAT, S_IRWXU);
        //vtf = fopen(vtfile.c_str(), "r+b");
    }
        
    assert(vtf != -1);
}

template <class T>
void kvT_t<T>::read_vtable()
{
    //read vtf 
    off_t size = fsize(vtf);
    if (size == -1L) { assert(0); }
    
    if (size != 0) {
        vid_t vcount = size/sizeof(sid_t);
        assert(vcount == g->get_type_vcount(tid));
        read(vtf, kv, sizeof(T)*vcount);
    }
}
