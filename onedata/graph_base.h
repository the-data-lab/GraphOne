#pragma once
#include <omp.h>
#include <iostream>
//#include <libaio.h>
#include <sys/mman.h>
#include <asm/mman.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>

#include "type.h"
#include "vunit.h"
#include "mem_pool.h"

//One vertex's neighbor information
template <class T>
class vert_table_t {
 public:
	vunit_t<T>*   v_unit;
    inline vert_table_t() { v_unit = 0;}
};

//one type's graph
template <class T>
class onegraph_t {

#ifdef BULK
 public:
    nebrcount_t*   nebr_count;//Only being used in BULK, remove it in future
#endif
 
 protected:
    //type id and vertices count together
    tid_t     tid;
    vid_t     max_vcount;
    snapid_t  snap_id;

    //array of adj list of vertices
    vert_table_t<T>* beg_pos;
	
	//Thread local memory data structures
	thd_mem_t<T>* thd_mem;

 private:
    //vertex table file related log
    write_seg_t  write_seg[3];
    vid_t        dvt_max_count;
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
    virtual void  setup(tid_t tid, vid_t a_max_vcount);
    virtual void  archive(edgeT_t<T>* edge, index_t count, snapid_t a_snapid);
    void  compress();
    void  handle_write(bool clean = false);
    void  read_vtable();
    void  file_open(const string& filename, bool trunc);
    
    
    inline void set_snapid(snapid_t a_snapid) { snap_id = a_snapid;}
    sdegree_t get_degree(vid_t v, snapid_t snap_id);
    inline sdegree_t get_degree(vid_t vid) {
        vunit_t<T>* v_unit = get_vunit(vid);
        if (v_unit) return v_unit->get_degree();
        return 0;
    }
    
    degree_t get_nebrs(vid_t vid, T* ptr, sdegree_t count /*= -1*/);
    degree_t get_wnebrs(vid_t vid, T* ptr, degree_t start, degree_t count);
    degree_t start(vid_t v, header_t<T>& header, degree_t offset = 0);
    status_t next(header_t<T>& header, T& dst);

	void increment_count_noatomic(vid_t vid, degree_t count = 1);
    void decrement_count_noatomic(vid_t vid, degree_t count = 1);
    
    void del_nebr_noatomic(vid_t vid, T sid);
    void add_nebr_noatomic(vid_t vid, T sid);
    void add_nebr_bulk(vid_t vid, T* adj_list2, degree_t count);
    degree_t find_nebr(vid_t vid, sid_t sid); 
	
    inline delta_adjlist_t<T>* get_delta_adjlist(vid_t vid) {
        vunit_t<T>* v_unit = get_vunit(vid);
        if (v_unit) {
            return v_unit->get_delta_adjlist();
        }
        return 0;
    }

protected:
    inline void set_delta_adjlist(vid_t vid, delta_adjlist_t<T>* adj_list) {
        vunit_t<T>* v_unit = get_vunit(vid);
        assert(v_unit);
        return v_unit->set_delta_adjlist(adj_list);
    } 
    status_t evict_old_adjlist(vid_t vid, degree_t degree);
    
    status_t compress_nebrs(vid_t vid);
	
    inline vunit_t<T>* get_vunit(vid_t v) {return beg_pos[v].v_unit;}
	inline void set_vunit(vid_t v, vunit_t<T>* v_unit) {
		beg_pos[v].v_unit = v_unit;
	}
    //-----------durability thing------------
    void prepare_dvt(write_seg_t* seg, vid_t& last_vid, bool clean = false);
	void adj_prep(write_seg_t* seg);
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
    
    //------------------------ local allocation-------
	inline vunit_t<T>* new_vunit() {
		return thd_mem->alloc_vunit();
	}
    
	inline snapT_t<T>* new_snapdegree() {
		return thd_mem->alloc_snapdegree();
	}

	inline delta_adjlist_t<T>* new_delta_adjlist(degree_t count, bool hub=false) {
        return thd_mem->alloc_adjlist(count, hub);
	}

    inline void delete_delta_adjlist(delta_adjlist_t<T>* adj_list, bool chain = false) {
        thd_mem->free_adjlist(adj_list, chain);
    }
	//------------------

public:
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
};

//one type's key-value store
template <class T>
class onekv_t {
 private:
    T*     kv;
    tid_t  tid;
    vid_t  max_vcount;
    int    vtf;

 public:
    inline onekv_t() {
        tid = 0;
        kv = 0;
        vtf = -1;
    }

    void setup(tid_t t, vid_t a_max_vcount);

    inline T* get_kv() { return kv; }
    inline tid_t get_tid() { return tid;}
    
    inline void set_value(vid_t vert1_id, T dst) {
        kv[vert1_id] = dst;
    }
    
    void handle_write(bool clean = false);
    void read_vtable(); 
    void file_open(const string& filename, bool trunc);
};

/*
typedef vert_table_t<sid_t> beg_pos_t;
typedef beg_pos_t  lgraph_t;
typedef vert_table_t<lite_edge_t> lite_vtable_t;
*/

typedef onegraph_t<sid_t> sgraph_t;
typedef onegraph_t<lite_edge_t>lite_sgraph_t;

typedef onekv_t<sid_t> skv_t; 
typedef onekv_t<lite_edge_t> lite_skv_t;

