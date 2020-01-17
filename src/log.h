#pragma once

#include <atomic>
#include <algorithm>

using std::min;

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

template <class T>
class blog_t;

template <class T>
class blog_reader_t {
 public:
    blog_t<T>* blog;
    //from tail to marker in blog_beg
    index_t tail;
    index_t marker;
    inline blog_reader_t() {
        blog = 0;
        tail = 0;
        marker = 0;
    }
};

//edge batching buffer
template <class T>
class blog_t {
 public:
    edgeT_t<T>* blog_beg;
    //In memory size
    index_t         blog_count;
    index_t         blog_shift;
    //MASK
    index_t     blog_mask;

    //current batching position
    index_t     blog_head;
    //Make adj list from this point
    std::atomic<index_t>  blog_tail;
    //Make adj list upto this point
    index_t     blog_marker;
    //Due to rewind, head should not go beyond
    index_t     blog_free;

    blog_reader_t<T>* reader[VIEW_COUNT];

    //Make edge durable from this point
    //index_t     blog_wtail;
    //Make edge durable upto this point
    //index_t     blog_wmarker;

    blog_t() {
        blog_beg = 0;
        blog_count = 0;
        blog_head = 0;
        blog_tail = 0;
        blog_marker = 0;
        blog_free = 0;
        //blog_wtail = 0;
        //blog_wmarker = 0;
        
        memset(reader, 0, VIEW_COUNT*sizeof(blog_reader_t<T>*));
    }

    inline int register_reader(blog_reader_t<T>* a_reader) {
        int reg_id = 0;
        for (; reg_id < VIEW_COUNT; ++reg_id) { 
            if (reader[reg_id] == 0) {
                reader[reg_id] = a_reader;
                return reg_id;
            }
        }
        assert(0);
        return reg_id;
    }
    inline void unregister_reader(int reg_id) {
        reader[reg_id] = 0;
    }

    void alloc_edgelog(index_t count);
    inline index_t update_marker() {
        blog_tail = blog_marker;
        return blog_tail;
    }

    inline index_t batch_edge(edgeT_t<T>& edge) {
        
        index_t index = __sync_fetch_and_add(&blog_head, 1L);
        bool rewind = !((index >> blog_shift) & 0x1);

        while (index + 1 - blog_free > blog_count) {
            //cout << "Sleeping for edge log" << endl;
            //assert(0);
            usleep(10);
        }
        
        index_t index1 = (index & blog_mask);
        blog_beg[index1] = edge;
        if (rewind) {
            set_dst(blog_beg[index1], DEL_SID(get_dst(edge)));
        }
        return index;
    }

    inline void readfrom_snapshot(snapshot_t* global_snapshot) {
        blog_head = global_snapshot->marker;
        blog_tail = global_snapshot->marker;
        blog_marker = global_snapshot->marker;
        //blog_wtail = global_snapshot->durable_marker;
        //blog_wmarker = global_snapshot->durable_marker; 
    }

    inline void free_blog() {
        index_t min_marker = blog_tail;
        for (int reg_id = 0; reg_id < VIEW_COUNT; ++reg_id) {
            if (reader[reg_id] == 0) continue;
            min_marker = min(min_marker, reader[reg_id]->tail);
        }
        blog_free = min_marker;
    }
};

template <class T>
void blog_t<T>::alloc_edgelog(index_t bit_shift) {
    if (blog_beg) {
        free(blog_beg);
        blog_beg = 0;
    }
    blog_shift = bit_shift;
    blog_count = (1L << blog_shift);
    blog_mask = blog_count - 1;
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

