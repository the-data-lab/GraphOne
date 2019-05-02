#include "type.h"

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

