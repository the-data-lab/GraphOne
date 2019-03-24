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
index_t read_idir(const string& idirname, edgeT_t<T>** pedges, bool alloc)
{
    struct dirent *ptr;
    DIR *dir;
    int file_count = 0;
    string filename;
        
    FILE* file = 0;
    index_t size = 0;
    index_t edge_count = 0;
    index_t total_edge_count = 0;
    
    edgeT_t<T>* edges = 0;

    //allocate accuately
    dir = opendir(idirname.c_str());
    if (alloc) {
        while (NULL != (ptr = readdir(dir))) {
            if (ptr->d_name[0] == '.') continue;
            filename = idirname + "/" + string(ptr->d_name);
            size = fsize(filename);
            edge_count = size/sizeof(edgeT_t<T>);
            total_edge_count += edge_count;
        }
        closedir(dir);

        edges = (edgeT_t<T>*)mmap(0, sizeof(edgeT_t<T>)*total_edge_count,
                    PROT_READ|PROT_WRITE,
                    MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB|MAP_HUGE_2MB, 0, 0);
    
        if (MAP_FAILED == edges) {
            cout << "huge page alloc failed while reading input dir" << endl;
            edges =  (edgeT_t<T>*)calloc(sizeof(edgeT_t<T>),total_edge_count);
        }
        *pedges = edges;
    } else {
        //edges = *pedges;
        //
        while (NULL != (ptr = readdir(dir))) {
            if (ptr->d_name[0] == '.') continue;
            filename = idirname + "/" + string(ptr->d_name);
            size = fsize(filename);
            edge_count = size/sizeof(edgeT_t<T>);
            total_edge_count += edge_count;
        }
        closedir(dir);
        //upper align the memory allocation
        edges = (edgeT_t<T>*)mmap(0, sizeof(edgeT_t<T>)*total_edge_count,
                    PROT_READ|PROT_WRITE,
                    MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB|MAP_HUGE_2MB, 0, 0);
        
        if (edges == MAP_FAILED) {
            cout << "huge page alloc failed while reading input dir" << endl;
            edges =  (edgeT_t<T>*)calloc(sizeof(edgeT_t<T>), total_edge_count);
        }
        *pedges = edges;
    }
    
    //Read graph files
    total_edge_count = 0;
    double start = mywtime();
    dir = opendir(idirname.c_str());
    edgeT_t<T>* edge = edges;
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

