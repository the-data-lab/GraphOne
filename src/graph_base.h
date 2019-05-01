#pragma once
#include <omp.h>
#include <iostream>
#include <libaio.h>
#include <sys/mman.h>
#include <asm/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <algorithm>
#include <stdio.h>
#include <errno.h>
#include "type.h"

using std::cout;
using std::endl;
using std::max;

inline void* alloc_huge(index_t size)
{   
    //void* buf = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB|MAP_HUGE_2MB, 0, 0);
    //if (buf== MAP_FAILED) {
    //    cout << "huge page failed" << endl;
    //}
    //return buf;
    
    return MAP_FAILED;
}

template <class T>
class thd_mem_t;

//One vertex's neighbor information
template <class T>
class vert_table_t {
 public:
    //nebr list of one vertex. First member is a spl member
    //count, flag for snapshot, XXX: smart pointer count
    snapT_t<T>*   snap_blob;

	vunit_t<T>*   v_unit;

 public:   
    inline vert_table_t() { snap_blob = 0; v_unit = 0;}
    
    inline degree_t get_degree() {
        if (snap_blob) return snap_blob->degree - snap_blob->del_count;
        else  return 0; 
    }
    
    inline degree_t get_delcount() {
        if (snap_blob) return snap_blob->del_count;
        else  return 0; 
    }
    
    inline index_t get_offset() { return v_unit->offset; }
	inline void set_offset(index_t adj_list1) { 
		v_unit->offset = adj_list1; 
	}
    
	inline delta_adjlist_t<T>* get_delta_adjlist() {
        if (v_unit) {
            return v_unit->delta_adjlist;
        }
        return 0;
    }
    
	inline void set_delta_adjlist(delta_adjlist_t<T>* adj_list) {
        assert(v_unit);
        if (v_unit->adj_list) {
			v_unit->adj_list->add_next(adj_list);
			v_unit->adj_list = adj_list;
		} else {
			v_unit->delta_adjlist = adj_list;
			v_unit->adj_list = adj_list;
		}
	}
	inline vunit_t<T>* get_vunit() {return v_unit;}
	inline vunit_t<T>* set_vunit(vunit_t<T>* v_unit1) {
        //prev value will be cleaned later
		vunit_t<T>* v_unit2 = v_unit;
		v_unit = v_unit1;//Atomic XXX
		return v_unit2;
	}

	inline snapT_t<T>* get_snapblob() { return snap_blob; } 
    
    //The incoming is simple, called from read_stable
    inline void set_snapblob1(snapT_t<T>* snap_blob1) { 
        if (0 == snap_blob) {
            snap_blob1->prev  = 0;
            //snap_blob1->next = 0;
        } else {
            snap_blob1->prev = snap_blob;
            //snap_blob1->next = 0;
            //snap_blob->next = snap_blob1;
        }
        snap_blob = snap_blob1; 
    } 
    
    inline snapT_t<T>* recycle_snapblob(snapid_t snap_id) { 
        if (0 == snap_blob || 0 == snap_blob->prev) return 0;
        
        index_t snap_count = 2;
        snapT_t<T>* snap_blob1 = snap_blob;
        snapT_t<T>* snap_blob2 = snap_blob->prev;

        while (snap_blob2->prev != 0) {
            snap_blob1 = snap_blob2;
            snap_blob2 = snap_blob2->prev;
            ++snap_count;
        }
        if (snap_count < SNAP_COUNT) {
            return 0;
        }

        snap_blob1->prev = 0;
        snap_blob2->snap_id = snap_id;
        snap_blob2->del_count = snap_blob->del_count;
        snap_blob2->degree = snap_blob->degree;
        set_snapblob1(snap_blob2);
        return snap_blob2;
    } 
};


//one type's graph
template <class T>
class onegraph_t {

#ifdef BULK
 public:
    nebrcount_t*   nebr_count;//Only being used in BULK, remove it in future
#endif
 
 private:
    //type id and vertices count together
    tid_t     tid;
    pgraph_t<T>* pgraph;

    //array of adj list of vertices
    vert_table_t<T>* beg_pos;
	
	//Thread local memory data structures
	thd_mem_t<T>* thd_mem;

 private:
    //vertex table file related log
    write_seg_t  write_seg[3];
    vid_t    dvt_max_count;
    //durable adj list, for writing to disk
    index_t    log_tail; //current log cleaning position
    index_t    log_count;//size of memory log

    int      vtf;   //vertex table file
    FILE*    stf;   //snapshot table file

    string   file;
public:
    int    etf;   //edge table file

public:
   onegraph_t(); 
    void setup(pgraph_t<T>* pgraph1, tid_t tid);
    
    degree_t get_degree(vid_t v, snapid_t snap_id);
    inline degree_t get_degree(vid_t vid) {
        return beg_pos[vid].get_degree();
    }
    
    inline degree_t get_delcount(vid_t vid) {
        return beg_pos[vid].get_delcount();
    }

    degree_t get_nebrs(vid_t vid, T* ptr, degree_t count = -1);
    degree_t get_wnebrs(vid_t vid, T* ptr, degree_t start, degree_t count);

	inline delta_adjlist_t<T>* get_delta_adjlist(vid_t v) {
        return beg_pos[v].get_delta_adjlist();
    }
    inline void set_delta_adjlist(vid_t v, delta_adjlist_t<T>* adj_list) {
        return beg_pos[v].set_delta_adjlist(adj_list);
    } 
    void     compress();
    status_t compress_nebrs(vid_t vid);

    vid_t get_vcount();
	void increment_count_noatomic(vid_t vid, degree_t count = 1);
    void decrement_count_noatomic(vid_t vid, degree_t count = 1);
    
    void del_nebr_noatomic(vid_t vid, T sid);
    void add_nebr_noatomic(vid_t vid, T sid);
    void add_nebr_bulk(vid_t vid, T* adj_list2, degree_t count);
    degree_t find_nebr(vid_t vid, sid_t sid); 

#ifdef BULK    
    void setup_adjlist_noatomic(vid_t vid_start, vid_t vid_end);
    void setup_adjlist();
    inline void increment_count(vid_t vid, degree_t count = 1) { 
        __sync_fetch_and_add(&nebr_count[vid].add_count, count);
    }

    inline void decrement_count(vid_t vid) { 
        __sync_fetch_and_add(&nebr_count[vid].del_count, 1L);
    }
    inline void reset_count(vid_t vid) {
        nebr_count[vid].add_count = 0;
        nebr_count[vid].del_count = 0;
    }
    inline void del_nebr(vid_t vid, T sid) {
        sid_t actual_sid = TO_SID(get_sid(sid)); 
        degree_t location = find_nebr(vid, actual_sid);
        if (INVALID_DEGREE != location) {
            set_sid(sid, location);
            beg_pos[vid].v_unit->adj_list->add_nebr(sid);
        }
    }
    inline void add_nebr(vid_t vid, T sid) {
        if (IS_DEL(get_sid(sid))) { 
            return del_nebr(vid, sid);
        }
        beg_pos[vid].v_unit->adj_list->add_nebr(sid);
    }
#endif
    
    //------------------------ local allocation-------
	inline vunit_t<T>* new_vunit() {
		return thd_mem[omp_get_thread_num()].alloc_vunit();
	}
    
	inline snapT_t<T>* new_snapdegree() {
		return thd_mem[omp_get_thread_num()].alloc_snapdegree();
	}

	inline delta_adjlist_t<T>* new_delta_adjlist(degree_t count) {
        return thd_mem[omp_get_thread_num()].alloc_delta_adjlist(count);
	}

    inline void free_delta_adjlist(delta_adjlist_t<T>* adj_list, bool chain = false) {
        if(chain) {
            delta_adjlist_t<T>* adj_list1 = adj_list;
            while (adj_list != 0) {
                adj_list1 = adj_list->get_next();
                free(adj_list);
                adj_list = adj_list1;
            }
        } else {
            free(adj_list);
        }
    }
    
    //-----------durability thing------------
    //durable adj list	
	inline durable_adjlist_t<T>* new_adjlist(write_seg_t* seg,  degree_t count) {
        degree_t new_count = count*sizeof(T)+sizeof(durable_adjlist_t<T>);
        //index_t index_log = __sync_fetch_and_add(&seg->log_head, new_count);
        //assert(index_log  < log_count); 
        index_t index_log = seg->log_head;
        seg->log_head += new_count;
        assert(seg->log_head  <= log_count); 
        return  (durable_adjlist_t<T>*)(seg->log_beg + index_log);
	}

	inline disk_vtable_t* new_dvt(write_seg_t* seg) {
        //index_t j = __sync_fetch_and_add(&seg->dvt_count, 1L);
        index_t j = seg->dvt_count;
        ++seg->dvt_count;
		return seg->dvt + j;
	}
	//------------------
    
    void prepare_dvt(write_seg_t* seg, vid_t& last_vid, bool clean = false);
	void adj_prep(write_seg_t* seg);
    void handle_write(bool clean = false);
    void read_vtable();
    void file_open(const string& filename, bool trunc);
};

template <class T>
class thd_mem_t {
    
    vunit_t<T>* vunit_beg;
    snapT_t<T>* dlog_beg;
    char*       adjlog_beg;
	
	public:
    index_t    	delta_size1;
	uint32_t    vunit_count;
	uint32_t    dsnap_count;
#ifdef BULK
    index_t     degree_count;
#endif
	index_t    	delta_size;
    
    char*       adjlog_beg1;
	
    inline vunit_t<T>* alloc_vunit() {
		if (vunit_count == 0) {
            vunit_bulk(1L << LOCAL_VUNIT_COUNT);
		}
		vunit_count--;
		return vunit_beg++;
	}
    
    inline status_t vunit_bulk(vid_t count) {
        vunit_count = count;
        vunit_beg = (vunit_t<T>*)alloc_huge(vunit_count*sizeof(vunit_t<T>));
        if (MAP_FAILED == vunit_beg) {
            vunit_beg = (vunit_t<T>*)calloc(sizeof(vunit_t<T>), vunit_count);
            assert(vunit_beg);
        }
        return eOK;
	}	

	inline snapT_t<T>* alloc_snapdegree() {
		if (dsnap_count == 0) {
            snapdegree_bulk(1L << LOCAL_VUNIT_COUNT);
		}
		dsnap_count--;
		return dlog_beg++;
	}
    
    inline status_t snapdegree_bulk(vid_t count) {
        dsnap_count = count;
        dlog_beg = (snapT_t<T>*)alloc_huge(sizeof(snapT_t<T>)*dsnap_count);
        if (MAP_FAILED == dlog_beg) {
            dlog_beg = (snapT_t<T>*)calloc(sizeof(snapT_t<T>), dsnap_count);
        }
        return eOK;
	}
    
	inline delta_adjlist_t<T>* alloc_delta_adjlist(degree_t count) {
        degree_t max_count;
        
        if (count >= HUB_COUNT || count >= 256) {
            max_count = TO_MAXCOUNT1(count);
        } else {
            max_count = TO_MAXCOUNT(count);
        }
        //max_count = TO_MAXCOUNT(count);
        index_t size = max_count*sizeof(T) + sizeof(delta_adjlist_t<T>);
        
		//delta_adjlist_t<T>* adj_list =  (delta_adjlist_t<T>*)malloc(size);
        
        index_t tmp = 0;
		if (size > delta_size) {
			tmp = max(1UL << LOCAL_DELTA_SIZE, size);
            delta_adjlist_bulk(tmp);
		}
		delta_adjlist_t<T>* adj_list = (delta_adjlist_t<T>*)adjlog_beg;
		assert(adj_list != 0);
		adjlog_beg += size;
		delta_size -= size;
        
        adj_list->set_nebrcount(0);
        adj_list->add_next(0);
        adj_list->set_maxcount(max_count);
        
		return adj_list;
	}
    
    inline status_t delta_adjlist_bulk(index_t size) {
        delta_size = size;
        adjlog_beg = (char*)alloc_huge(delta_size);
        if (MAP_FAILED == adjlog_beg) {
            adjlog_beg = (char*)malloc(delta_size);
            assert(adjlog_beg);
        }
        //cout << "alloc adj " << delta_size << endl; 
        return eOK;
    }
};


//one type's key-value store
template <class T>
class onekv_t {
 private:
    T* kv;
    tid_t  tid;
    int  vtf;

 public:
    inline onekv_t() {
        tid = 0;
        kv = 0;
        vtf = -1;
    }

    void setup(tid_t tid);

    inline T* get_kv() { return kv; }
    inline tid_t get_tid() { return tid;}
    
    inline void set_value(vid_t vert1_id, T dst) {
        kv[vert1_id] = dst;
    }
    
    void handle_write(bool clean = false);
    void read_vtable(); 
    void file_open(const string& filename, bool trunc);
};

typedef vert_table_t<sid_t> beg_pos_t;
typedef beg_pos_t  lgraph_t;
typedef vert_table_t<lite_edge_t> lite_vtable_t;

typedef onegraph_t<sid_t> sgraph_t;
typedef onegraph_t<lite_edge_t>lite_sgraph_t;

typedef onekv_t<sid_t> skv_t; 
typedef onekv_t<lite_edge_t> lite_skv_t;
