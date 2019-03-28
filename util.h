#pragma once
#include <dirent.h>
#include "type.h"
#include "new_func.h"


inline index_t upper_power_of_two(index_t v)
{
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v++;
    return v;
}

inline
index_t alloc_mem_dir(const string& idirname, char** buf, bool alloc)
{
    struct dirent *ptr;
    DIR *dir;
    string filename;
        
    index_t size = 0;
    index_t total_size = 0;
    

    //allocate accuately
    dir = opendir(idirname.c_str());
    
    while (NULL != (ptr = readdir(dir))) {
        if (ptr->d_name[0] == '.') continue;
        filename = idirname + "/" + string(ptr->d_name);
        size = fsize(filename);
        total_size += size;
    }
    closedir(dir);

    void* local_buf = mmap(0, total_size, PROT_READ|PROT_WRITE,
                MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB|MAP_HUGE_2MB, 0, 0);

    if (MAP_FAILED == local_buf) {
        cout << "huge page alloc failed while reading input dir" << endl;
        local_buf =  malloc(total_size);
    }
    *buf = (char*)local_buf;
    return total_size;
}


inline 
index_t read_text_dir(const string& idirname, char* edges)
{
    //Read graph files
    struct dirent *ptr;
    FILE* file = 0;
    int file_count = 0;
    string filename;
    
    index_t size = 0;
    index_t total_size = 0;
    char* edge;
    
    double start = mywtime();
    DIR* dir = opendir(idirname.c_str());
    while (NULL != (ptr = readdir(dir))) {
        if (ptr->d_name[0] == '.') continue;
        filename = idirname + "/" + string(ptr->d_name);
        file_count++;
        
        file = fopen((idirname + "/" + string(ptr->d_name)).c_str(), "rb");
        assert(file != 0);
        size = fsize(filename);
        edge = edges + total_size;
        if (size!= fread(edge, sizeof(char), size, file)) {
            assert(0);
        }
        total_size += size;
    }
    closedir(dir);
    double end = mywtime();
    cout << "Reading "  << file_count  << " file time = " << end - start << endl;
    cout << "total size = " << total_size << endl;
    return total_size;
}

template <class T>
index_t read_bin_dir(const string& idirname, edgeT_t<T>* edges)
{
    //Read graph files
    struct dirent *ptr;
    FILE* file = 0;
    int file_count = 0;
    string filename;
    
    index_t size = 0;
    index_t edge_count = 0;
    index_t total_edge_count = 0;
    edgeT_t<T>* edge;
    
    double start = mywtime();
    DIR* dir = opendir(idirname.c_str());
    while (NULL != (ptr = readdir(dir))) {
        if (ptr->d_name[0] == '.') continue;
        filename = idirname + "/" + string(ptr->d_name);
        file_count++;
        
        file = fopen((idirname + "/" + string(ptr->d_name)).c_str(), "rb");
        assert(file != 0);
        size = fsize(filename);
        edge_count = size/sizeof(edgeT_t<T>);
        edge = edges + total_edge_count;
        if (edge_count != fread(edge, sizeof(edgeT_t<T>), edge_count, file)) {
            assert(0);
        }
        total_edge_count += edge_count;
    }
    closedir(dir);
    double end = mywtime();
    cout << "Reading "  << file_count  << " file time = " << end - start << endl;
    cout << "End marker = " << total_edge_count << endl;
    return total_edge_count;
}

//------- The APIs to use by higher level function -------//
template <class T>
index_t read_idir(const string& idirname, edgeT_t<T>** pedges, bool alloc)
{
    //allocate accuately
    char* buf = 0;
    index_t total_size = alloc_mem_dir(idirname, &buf, alloc);
    index_t total_edge_count = total_size/sizeof(edgeT_t<T>);
    *pedges = (edgeT_t<T>*)buf;

    if (total_edge_count != read_bin_dir(idirname, *pedges)) {
        assert(0);
    }
    
    return total_edge_count;
}

template <class T>
void read_idir_text(const string& idirname, const string& odirname, pgraph_t<T>* pgraph,
                    typename callback<T>::parse_fn_t parsefile_and_insert)
{
    struct dirent *ptr;
    DIR *dir;
    int file_count = 0;
    string filename;
    string ofilename;

    vid_t vid = 0;
        
    //Read graph files
    double start = mywtime();
    dir = opendir(idirname.c_str());
    while (NULL != (ptr = readdir(dir))) {
        if (ptr->d_name[0] == '.') continue;
        filename = idirname + "/" + string(ptr->d_name);
        ofilename = odirname + "/" + string(ptr->d_name);
        cout << "ifile= "  << filename << endl 
                <<" ofile=" << ofilename << endl;

        file_count++;
        parsefile_and_insert(filename, ofilename, pgraph);
        double end = mywtime();
        cout <<" Time = "<< end - start;
        vid = g->get_type_scount();
        cout << " vertex count" << vid << endl;
    }
    closedir(dir);
    vid = g->get_type_scount();
    cout << "vertex count" << vid << endl;
    return;
}

template <class T>
void read_idir_text2(const string& idirname, const string& odirname, pgraph_t<T>* pgraph,
                    typename callback<T>::parse_fn2_t parsebuf_and_insert)
{
    
    //allocate accuately
    char* buf = 0;
    index_t total_size = alloc_mem_dir(idirname, &buf, true);
    if (total_size != read_text_dir(idirname, buf)) {
        assert(0);
    }
    
    parsebuf_and_insert(buf, pgraph);
    vid_t vid = g->get_type_scount();
    cout << "vertex count" << vid << endl;
    return;
}

