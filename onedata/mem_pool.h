#pragma once

#include <unistd.h>
#include <algorithm>
#include "view_interface.h"
using std::cout;
using std::endl;
using std::max;

/*
template <class T>
index_t TO_CACHELINE<T>(index_t count, index_t meta_size)
{
    index_t total_size = count*sizeof(T) + meta_size;
    index_t cachelined_size = (total_size | CL_UPPER);
}
*/

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
struct mem_t {
    vunit_t<T>* vunit_beg;
    snapT_t<T>* dlog_beg;
    char*       adjlog_beg;

    vunit_t<T>** vunit_retired;
    vunit_t<T>* vunit_free;
    	
	public:
    index_t    	delta_size1;
	uint32_t    vunit_count;
	uint32_t    dsnap_count;
    uint32_t    vunit_retired_count;
#ifdef BULK
    index_t     degree_count;
#endif
	index_t    	delta_size;
    
    char*       adjlog_beg1;
};

struct reader_t {
   void** hp;//THD_COUNT is the array size
};

template <class T>
class thd_mem_t {
    mem_t<T>* mem;  
    reader_t reader[VIEW_COUNT];
 
 public:	
    inline vunit_t<T>* alloc_vunit() {
        mem_t<T>* mem1 = mem + omp_get_thread_num();  
		vunit_t<T>* v_unit;
        if (mem1->vunit_free) {
            v_unit = mem1->vunit_free;
            mem1->vunit_free->next;
            return v_unit;
        }
        if (mem1->vunit_count == 0) {
            vunit_bulk(1L << LOCAL_VUNIT_COUNT);
		}
		mem1->vunit_count--;
		return mem1->vunit_beg++;
	}
    
    inline status_t vunit_bulk(vid_t count) {
        mem_t<T>* mem1 = mem + omp_get_thread_num();  
        mem1->vunit_count = count;
        mem1->vunit_beg = (vunit_t<T>*)alloc_huge(count*sizeof(vunit_t<T>));
        if (MAP_FAILED == mem1->vunit_beg) {
            mem1->vunit_beg = (vunit_t<T>*)calloc(sizeof(vunit_t<T>), count);
            assert(mem1->vunit_beg);
        }
        return eOK;
	}
    
    inline int register_reader() {
        int reg_id = 0;
        for (; reg_id < VIEW_COUNT; ++reg_id) { 
            if (reader[reg_id].hp == 0) {
                reader[reg_id].hp = (void**)calloc(sizeof(void*),THD_COUNT);
                return reg_id;
            }
        }
        assert(0);
        return reg_id;
    }

    inline void unregister_reader(int reg_id) {
        free(reader[reg_id].hp);
        reader[reg_id].hp = 0;
    }

    inline void retire_vunit(vunit_t<T>* v_unit1) {
        mem_t<T>* mem1 = mem + omp_get_thread_num();  
        mem1->vunit_retired[mem1->vunit_retired_count++] = v_unit1;
        if (1024 == mem1->vunit_retired_count) {
            vunit_t<T>* v_unit;
            vunit_t<T>** vunit_retired = mem1->vunit_retired;
            int    vunit_retired_count = mem1->vunit_retired_count;
            mem1->vunit_retired = (vunit_t<T>**) calloc(sizeof(vunit_t<T>*), 1024);
            mem1->vunit_retired_count = 0;
            for (int i = 0; i < vunit_retired_count; i++) {
                v_unit = vunit_retired[i];
                free_vunit(v_unit);
            }
            free(vunit_retired);
        }
    }

    inline void free_vunit(vunit_t<T>* v_unit) {
        mem_t<T>* mem1 = mem + omp_get_thread_num();
        vunit_t<T>** hp;
        for (int j = 0; j < VIEW_COUNT; ++j) {
            hp = (vunit_t<T>**)reader[j].hp;
            if (0 == hp) continue;
            for (int k = 0; k < THD_COUNT; ++k) {
                if (v_unit == hp[k]) {
                    //insert it back
                    mem1->vunit_retired[mem1->vunit_retired_count++] = v_unit;
                    return;
                }
            }
        }
        //free it;
        free_adjlist(v_unit->delta_adjlist, true);
        v_unit->next = mem1->vunit_free->next;
        mem1->vunit_free = v_unit;
        return;
    }

    inline void add_hp(vunit_t<T>* v_unit, int reg_id) {
        reader[reg_id].hp[omp_get_thread_num()]= v_unit;
    }

    inline void rem_hp(vunit_t<T>* v_unit, int reg_id) {
        reader[reg_id].hp[omp_get_thread_num()] = 0;
    }
	
    inline snapT_t<T>* alloc_snapdegree() {
        mem_t<T>* mem1 = mem + omp_get_thread_num();  
		if (mem1->dsnap_count == 0) {
            snapdegree_bulk(1L << LOCAL_VUNIT_COUNT);
		}
		mem1->dsnap_count--;
		return mem1->dlog_beg++;
	}
    
    inline status_t snapdegree_bulk(vid_t count) {
        mem_t<T>* mem1 = mem + omp_get_thread_num();  
        mem1->dsnap_count = count;
        mem1->dlog_beg = (snapT_t<T>*)alloc_huge(sizeof(snapT_t<T>)*count);
        if (MAP_FAILED == mem1->dlog_beg) {
            mem1->dlog_beg = (snapT_t<T>*)calloc(sizeof(snapT_t<T>), count);
        }
        return eOK;
	}
    
	inline delta_adjlist_t<T>* alloc_adjlist(degree_t count, bool hub) {
		delta_adjlist_t<T>* adj_list = 0;
        index_t size = count*sizeof(T) + sizeof(delta_adjlist_t<T>);
        if (hub || count >= 256) {
            size = TO_PAGESIZE(size);
        } else {
            size = TO_CACHELINE(size);
        }
        degree_t max_count = (size - sizeof(delta_adjlist_t<T>))/sizeof(T);

        #if defined(DEL) || defined(MALLOC) 
		adj_list =  (delta_adjlist_t<T>*)malloc(size);
        assert(adj_list!=0);
        #else 
        mem_t<T>* mem1 = mem + omp_get_thread_num();  
        index_t tmp = 0;
		if (size > mem1->delta_size) {
			tmp = max(1UL << LOCAL_DELTA_SIZE, size);
            delta_adjlist_bulk(tmp);
		}
		adj_list = (delta_adjlist_t<T>*)mem1->adjlog_beg;
		assert(adj_list != 0);
		mem1->adjlog_beg += size;
		mem1->delta_size -= size;
        #endif
        
        adj_list->set_nebrcount(0);
        adj_list->add_next(0);
        adj_list->set_maxcount(max_count);
        
		return adj_list;
	}

    void free_adjlist(delta_adjlist_t<T>* adj_list, bool chain) {
        #if defined(DEL) || defined(MALLOC)
        mem_t<T>* mem1 = mem + omp_get_thread_num();  
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
        #endif
    }
    
    inline status_t delta_adjlist_bulk(index_t size) {
        mem_t<T>* mem1 = mem + omp_get_thread_num();  
        mem1->delta_size = size;
        mem1->adjlog_beg = (char*)alloc_huge(size);
        if (MAP_FAILED == mem1->adjlog_beg) {
            mem1->adjlog_beg = (char*)malloc(size);
            assert(mem1->adjlog_beg);
        }
        //cout << "alloc adj " << delta_size << endl; 
        return eOK;
    }

    inline thd_mem_t() {
        if(posix_memalign((void**)&mem, 64, THD_COUNT*sizeof(mem_t<T>))) {
            cout << "posix_memalign failed()" << endl;
            mem = (mem_t<T>*)calloc(sizeof(mem_t<T>), THD_COUNT);
        } else {
            memset(mem, 0, THD_COUNT*sizeof(mem_t<T>));
        } 
    } 
};

