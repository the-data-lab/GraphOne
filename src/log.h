#pragma once

#include <atomic>

template <class T>
class tmp_blog_t {
 public:
    edgeT_t<T>* edges;
    int32_t     count;
    int32_t     max_count;

    inline tmp_blog_t(int32_t icount) {
        edges = (edgeT_t<T>*)calloc(icount, sizeof(edgeT_t<T>));
        max_count = icount;
        count = 0;
    }
    inline ~tmp_blog_t() {
        delete edges;
    }
};

//edge batching buffer
template <class T>
class blog_t {
 public:
    edgeT_t<T>* blog_beg;
    //In memory size
    index_t     blog_count;
    //MASK
    index_t     blog_mask;

    //current batching position
    index_t     blog_head;
    //Make adj list from this point
    std::atomic<index_t>   blog_tail;
    //Make adj list upto this point
    index_t     blog_marker;
    //Make edge durable from this point
    index_t     blog_wtail;
    //Make edge durable upto this point
    index_t     blog_wmarker;

    blog_t() {
        blog_beg = 0;
        blog_count = 0;
        blog_head = 0;
        blog_tail = 0;
        blog_marker = 0;
        blog_wtail = 0;
        blog_wmarker = 0;
    }

    void alloc_edgelog(index_t count);
    inline index_t update_marker() {
        blog_tail = blog_marker;
        return blog_tail;
    }

    inline void readfrom_snapshot(snapshot_t* global_snapshot) {
        blog_head = global_snapshot->marker;
        blog_tail = global_snapshot->marker;
        blog_marker = global_snapshot->marker;
        blog_wtail = global_snapshot->durable_marker;
        blog_wmarker = global_snapshot->durable_marker; 
    }
};
    
template <class T>
void blog_t<T>::alloc_edgelog(index_t count) {
    if (blog_beg) {
        free(blog_beg);
        blog_beg = 0;
    }

    blog_count = count;
    blog_mask = count - 1;
    //blog->blog_beg = (edgeT_t<T>*)mmap(0, sizeof(edgeT_t<T>)*blog->blog_count, 
    //PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB|MAP_HUGE_2MB, 0, 0);
    //if (MAP_FAILED == blog->blog_beg) {
    //    cout << "Huge page alloc failed for edge log" << endl;
    //}
    
    /*
    if (posix_memalign((void**)&blog_beg, 2097152, blog_count*sizeof(edgeT_t<T>))) {
        perror("posix memalign batch edge log");
    }*/
    blog_beg = (edgeT_t<T>*)calloc(blog_count, sizeof(edgeT_t<T>));
    assert(blog_beg);
}

