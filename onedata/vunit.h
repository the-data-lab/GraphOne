#pragma once

#include "type.h"

template <class T>
class delta_adjlist_t {
	delta_adjlist_t<T>* next;
    degree_t   max_count;
	degree_t   count;
	//T  adj_list;

 public:
	inline delta_adjlist_t<T>() {next = 0; count = 0;}
	inline degree_t get_nebrcount() { return count;}
	inline void set_nebrcount(degree_t degree) {count = degree;}

	inline degree_t incr_nebrcount_noatomic() {
        return count++;
    }
	inline degree_t incr_nebrcount() {
        return  __sync_fetch_and_add(&count, 1);
    }
    inline degree_t incr_nebrcount_bulk(degree_t count1) {
        degree_t old_count = count;
        count += count1;
        return old_count;
    }
    inline degree_t get_maxcount() {return max_count;}
    inline void set_maxcount(degree_t value) {max_count = value;}
	inline T* get_adjlist() { return (T*)(&count + 1); }
	inline void add_next(delta_adjlist_t<T>* ptr) {next = ptr; }
	inline delta_adjlist_t<T>* get_next() { return next; }
    
    inline void add_nebr(T sid) { 
        T* adj_list1 = get_adjlist();
        degree_t index = incr_nebrcount();
        adj_list1[index] = sid;
    }
    
    inline void add_nebr_noatomic(T sid) { 
        T* adj_list1 = get_adjlist();
        degree_t index = incr_nebrcount_noatomic();
        adj_list1[index] = sid;
    }
    
    // XXX Should be used for testing purpose, be careful to use it,
    // as it avoids atomic instructions.
    inline void add_nebr_bulk(T* adj_list2, degree_t count) {
        if(count != 0) {
            T* adj_list1 = get_adjlist();
            degree_t old_count = incr_nebrcount_bulk(count);
            memcpy(adj_list1+old_count, adj_list2, count*sizeof(T));
        }
    }
    
};

template <class T>
class durable_adjlist_t {
    union {
        sid_t sid;
        durable_adjlist_t<T>* next;
    };
    degree_t max_count;
	degree_t count;
	//T  adj_list;

 public:
	inline durable_adjlist_t<T>() {sid = 0; count = 0;}
	inline degree_t get_nebrcount() { return count;}
	void set_nebrcount(degree_t degree) {count = degree;}
	inline T* get_adjlist() { return (T*)(&count + 1); }
};

class nebrcount_t {
 public:
    degree_t    add_count;
    degree_t    del_count;
};


template <class T>
class  snapT_t {
 public:
    snapT_t<T>*     prev;//prev snapshot of this vid 
    degree_t  degree;
    uint16_t  snap_id;
    uint16_t  del_count;
};

//This will be used as disk write structure
template <class T>
class  disk_snapT_t {
 public:
    vid_t     vid;
    snapid_t  snap_id;
    degree_t  degree;
    degree_t del_count;
};

//Special v-unit flags
//#define VUNIT_NORMAL 0
//#define VUNIT_SHORT  1
//#define VUNIT_LONG   2
//#define TO_VUNIT_FLAG(flag)  (flag & 0x3)
//#define TO_VUNIT_COUNT(flag) ((flag >> 2 ) & 0x7)

template <class T>
class vunit_t {
 public:
	//uint16_t      vflag;
	//uint16_t    max_size; //max count in delta adj list
    snapT_t<T>*   snap_blob;
	delta_adjlist_t<T>* delta_adjlist;
	delta_adjlist_t<T>* adj_list;//Last chain

	inline void reset() {
		//vflag = 0;
        snap_blob = 0;
		delta_adjlist = 0;
        adj_list = 0;
	}
    
    inline degree_t get_degree() {
        if (snap_blob) return snap_blob->degree - snap_blob->del_count;
        else  return 0; 
    }
    
    inline degree_t get_delcount() {
        if (snap_blob) return snap_blob->del_count;
        else  return 0; 
    }
    inline degree_t get_degree(snapid_t snap_id)
    {
        if (0 == snap_blob) { 
            return 0;
        }
        
        degree_t nebr_count = 0;
        if (snap_id >= snap_blob->snap_id) {
            nebr_count = snap_blob->degree - snap_blob->del_count; 
        } else {
            snap_blob = snap_blob->prev;
            while (snap_blob && snap_id < snap_blob->snap_id) {
                snap_blob = snap_blob->prev;
            }
            if (snap_blob) {
                nebr_count = snap_blob->degree - snap_blob->del_count; 
            }
        }
        return nebr_count;
    }
    
    /*
    inline index_t get_offset() { return v_unit->offset; }
	inline void set_offset(index_t adj_list1) { 
		v_unit->offset = adj_list1; 
	}*/
    
	inline delta_adjlist_t<T>* get_delta_adjlist() {
        return delta_adjlist;
    }
    
	inline void set_delta_adjlist(delta_adjlist_t<T>* adj_list1) {
        if (adj_list) {
			adj_list->add_next(adj_list1);
			adj_list = adj_list1;
		} else {
			delta_adjlist = adj_list1;
			adj_list = adj_list1;
		}
	}

	inline snapT_t<T>* get_snapblob() { return snap_blob; } 
    
    //The incoming is simple, called from read_stable
    inline void set_snapblob(snapT_t<T>* snap_blob1) { 
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
        set_snapblob(snap_blob2);
        return snap_blob2;
    } 
};

//used for offline processing
class ext_vunit_t {
    public:
    degree_t  count;
    degree_t  del_count;
    index_t   offset;
};

class disk_vtable_t {
    public:
    vid_t    vid;
	//Length of durable adj list
    degree_t count;
    degree_t del_count;
    uint64_t file_offset;
};

//Used for writing adj list to disk
class write_seg_t {
 public:
     disk_vtable_t* dvt;
     index_t        dvt_count;
     char*          log_beg;
     index_t        log_head;
	 //This is file offset at which to start writing
	 index_t        log_tail;

     inline write_seg_t() {
        dvt = 0;
        dvt_count = 0;
        log_beg = 0;
        log_head = 0;
		log_tail = 0;
     }
	 inline void reset() {
        dvt = 0;
        dvt_count = 0;
        log_beg = 0;
        log_head = 0;
		log_tail = 0;
	 }
};

