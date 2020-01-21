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
	vunit_t<T>   *v_unit;
    public:
    inline vert_table_t() { v_unit = 0;}
    inline vunit_t<T>* get_vunit() { return v_unit;}
    inline void set_vunit(vunit_t<T>* v_unit1) { /*assert(0);*/v_unit = v_unit1;}
};

template <class T>
struct reader_t {
   void** hp;//THD_COUNT is the array size
   sdegree_t* degree;
   gview_t<T>* viewh;
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
    reader_t<T> reader[VIEW_COUNT];

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
    virtual void  archive(edgeT_t<T>* edge, index_t count, snapshot_t* snapshot);
    void  compress();
    void  handle_write(bool clean = false);
    void  read_vtable();
    void  file_open(const string& filename, bool trunc);
    /*
    inline int register_reader(gview_t<T>* viewh, sdegree_t* degree) {
        return thd_mem->register_reader(viewh, degree);
    }
    inline void unregister_reader(int reg_id) {return thd_mem->unregister_reader(reg_id);}
   */ 
    inline void set_snapid(snapid_t a_snapid) { snap_id = a_snapid;}
    sdegree_t get_degree(vid_t v, snapid_t snap_id);
    inline sdegree_t get_degree(vid_t vid) {
        vunit_t<T>* v_unit = get_vunit(vid);
        if (v_unit) return v_unit->get_degree();
        return 0;
    }
    
    degree_t get_nebrs(vid_t vid, T* ptr, sdegree_t count, int reg_id = -1);//XXX remove default
    degree_t get_diff_nebrs(vid_t vid, T* ptr, sdegree_t start, sdegree_t sdegree, int reg_id, degree_t& diff_degree);
    degree_t get_wnebrs(vid_t vid, T* ptr, sdegree_t start, sdegree_t count, int reg_id = -1);
    degree_t start(vid_t v, header_t<T>& header, degree_t offset = 0);
    status_t next(header_t<T>& header, T& dst);

	void increment_count_noatomic(vid_t vid, snapshot_t* snapshot, degree_t count = 1);
    void decrement_count_noatomic(vid_t vid, snapshot_t* snapshot, degree_t count = 1);
    
    void del_nebr_noatomic(vid_t vid, T sid);
    void add_nebr_noatomic(vid_t vid, T sid);
    void add_nebr_bulk(vid_t vid, T* adj_list2, degree_t count);
    degree_t find_nebr(vid_t vid, sid_t sid, bool add = false); 
    T* find_nebr_bypos(vid_t vid, degree_t pos); 
	
    inline delta_adjlist_t<T>* get_delta_adjlist(vid_t vid) {
        vunit_t<T>* v_unit = get_vunit(vid);
        if (v_unit) {
            return v_unit->get_delta_adjlist();
        }
        return 0;
    }

    inline int register_reader(gview_t<T>* viewh, sdegree_t* degree) {
        int reg_id = 0;
        for (; reg_id < VIEW_COUNT; ++reg_id) { 
            if (reader[reg_id].hp == 0) {
                reader[reg_id].hp = (void**)calloc(sizeof(void*),THD_COUNT);
                reader[reg_id].degree = degree;
                reader[reg_id].viewh = viewh;
                return reg_id;
            }
        }
        assert(0);
        return reg_id;
    }

    inline void unregister_reader(int reg_id) {
        reader[reg_id].viewh = 0;
        free(reader[reg_id].hp);
        reader[reg_id].hp = 0;
        reader[reg_id].degree = 0;
    }
    inline snapid_t get_eviction(vid_t vid, degree_t& evict_count) {
        vunit_t<T>* v_unit = get_vunit(vid);
        if (v_unit) {
            evict_count = v_unit->del_count;
            return v_unit->snap_id;
        }
        return 0;
    }

protected:
    degree_t get_nebrs_internal(vunit_t<T>* vunit, T* ptr, sdegree_t count, delta_adjlist_t<T>*& delta_adjlist, degree_t& i_count);

    inline void add_hp(vunit_t<T>* v_unit, int reg_id) {
        reader[reg_id].hp[omp_get_thread_num()]= v_unit;
    }

    inline void rem_hp(vunit_t<T>* v_unit, int reg_id) {
        reader[reg_id].hp[omp_get_thread_num()] = 0;
    }
    
    inline snapid_t get_degree_min(vid_t vid, sdegree_t& degree) {
        sdegree_t sdegree = 0;
        snapid_t snap_id1 = 0;
        index_t compaction_marker = 0;
        index_t prev_compaction_marker = 0;

        for (int j = 0; j < VIEW_COUNT; ++j) {
            if (reader[j].viewh == 0) { 
                continue;
            }
            compaction_marker = reader[j].viewh->get_compaction_marker();
            snap_id1 = reader[j].viewh->get_prev_snapid();
            sdegree = reader[j].degree[vid];
            //if (0 == compaction_marker || compaction_marker < prev_compaction_marker) {
            //    continue;
            //}
            if (snap_id1 == reader[j].viewh->get_prev_snapid() &&
                snap_id1 == reader[j].viewh->get_snapid()) {
                prev_compaction_marker = compaction_marker;
                continue;
            } else { //We came here beacuse view may be getting updated
                snap_id1 = reader[j].viewh->get_snapid();
                compaction_marker = reader[j].viewh->snapshot->marker;
                if (compaction_marker < prev_compaction_marker) continue;
                prev_compaction_marker = compaction_marker;
                #ifdef DEL
                //sdegree = get_degree(vid, snap_id1);
                #endif
                //read value is fine in window/delete case.
            }
        }
        #ifdef DEL
        if (snap_id1 == 0 && snap_id > 2) { // no readers
            //assert(0);
            snap_id1 = snap_id - 1;
            #ifdef COMPACTION
            sdegree = get_degree(vid, snap_id-1);//auto
            #else
            sdegree = get_degree(vid, snap_id);//manual
            #endif
        }
        #endif
        degree = sdegree;
        return snap_id1;
    }
    
    inline void free_vunit(vunit_t<T>* v_unit) {
        mem_t<T>* mem1 = thd_mem->mem + omp_get_thread_num();
        vunit_t<T>** hp;
        for (int j = 0; j < VIEW_COUNT; ++j) {
            hp = (vunit_t<T>**)reader[j].hp;
            if (0 == hp) continue;
            for (int k = 0; k < THD_COUNT; ++k) {
                if (v_unit == hp[k]) {
                    //insert it back
                    mem1->vunit_retired_last->next = v_unit;
                    mem1->vunit_retired_last = v_unit;
                    mem1->vunit_retired_count++;
                    v_unit->next = 0;
                    return;
                }
            }
        }
        //free it;
        thd_mem->free_adjlist(v_unit->delta_adjlist, v_unit->chain_count);
        if (mem1->vunit_free) {
            v_unit->next = mem1->vunit_free->next;
        } else {
            v_unit->next = 0;
        }
        mem1->vunit_free = v_unit;
        return;
    }
    
    inline void retire_vunit(vunit_t<T>* v_unit1) {
        mem_t<T>* mem1 = thd_mem->mem + omp_get_thread_num();  
        vunit_t<T>* v_unit = v_unit1;
        v_unit->next = 0;
        mem1->vunit_retired_count++;
        if ( 0 == mem1->vunit_retired) {
            mem1->vunit_retired = v_unit;
            mem1->vunit_retired_last = v_unit;
            return;
        }
        mem1->vunit_retired_last->next = v_unit;
        mem1->vunit_retired_last = v_unit;
        
        if (mem1->vunit_retired_count < 1024) {
            return;
        }
        for(int i = 1; i < mem1->vunit_retired_count; ++i) {
            v_unit = mem1->vunit_retired;
            mem1->vunit_retired = v_unit->next;
            mem1->vunit_retired_count--;
            free_vunit(v_unit);
        }
    }

protected:
    inline void set_delta_adjlist(vid_t vid, delta_adjlist_t<T>* adj_list) {
        vunit_t<T>* v_unit = get_vunit(vid);
        assert(v_unit);
        return v_unit->set_delta_adjlist(adj_list);
    } 
    //status_t evict_old_adjlist(vid_t vid, degree_t degree);
    
    status_t window_nebrs(vid_t vid);
    status_t compress_nebrs(vid_t vid);
	
    inline vunit_t<T>* get_vunit(vid_t v) {return beg_pos[v].get_vunit();}
	inline void set_vunit(vid_t v, vunit_t<T>* v_unit) {
		beg_pos[v].set_vunit(v_unit);
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

