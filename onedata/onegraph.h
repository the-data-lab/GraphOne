#pragma once
#include <algorithm>
#include "mem_pool.h"
#include "out_going.h"
#include "onesnb.h"

using std::min;

template <class T>
void onegraph_t<T>::archive(edgeT_t<T>* edges, index_t count, snapid_t a_snapid)
{
    tid_t src_index;
    sid_t src;
    T dst;
    vid_t vert1_id;
    snap_id = a_snapid;
    for (index_t i = 0; i < count; ++i) {
        src = edges[i].src_id;
        //src_index = TO_TID(src);
        vert1_id = TO_VID(src);

        if (!IS_DEL(src)) { 
            increment_count_noatomic(vert1_id);
        } else { 
            decrement_count_noatomic(vert1_id);
        }
    }
    
    for (index_t i = 0; i < count; ++i) {
        src = edges[i].src_id;
        dst = edges[i].dst_id;
        //src_index = TO_TID(src);
        vert1_id = TO_VID(src);
        if (!IS_DEL(src)) { 
            add_nebr_noatomic(vert1_id, dst);
        } else {
            del_nebr_noatomic(vert1_id, dst);
        }
    }
}

template <class T>
void onegraph_t<T>::increment_count_noatomic(vid_t vid, degree_t count /*=1*/) 
{
    //allocate v-unit
    vunit_t<T>* v_unit = get_vunit(vid);
    if (v_unit == 0) {
        v_unit = new_vunit();
        set_vunit(vid, v_unit);
    }

	snapT_t<T>* curr = v_unit->get_snapblob();
	if (curr == 0 || curr->snap_id < snap_id) {
		//allocate new snap blob 
		snapT_t<T>* next = v_unit->recycle_snapblob(snap_id);
		if (next == 0) {
			next = new_snapdegree();
            next->snap_id       = snap_id;
            if (curr) {
                next->degree = curr->degree;
            }
            v_unit->set_snapblob(next);
        }
		curr = next;
	}
#ifdef DEL	
	curr->degree.add_count += count;
#else
	curr->degree += count;
#endif
}

template <class T>
void onegraph_t<T>::decrement_count_noatomic(vid_t vid, degree_t count /*=1*/) 
{
    //allocate v-unit
    vunit_t<T>* v_unit = get_vunit(vid);
    if (v_unit == 0) {
        v_unit = new_vunit();
        set_vunit(vid, v_unit);
    }
	snapT_t<T>* curr = v_unit->get_snapblob();
	if (curr == 0 || curr->snap_id < snap_id) {
		//allocate new snap blob 
		snapT_t<T>* next = v_unit->recycle_snapblob(snap_id);
		if (next == 0) {
			next = new_snapdegree();
            next->snap_id       = snap_id;
            if (curr) {
                next->degree = curr->degree;
            }
            v_unit->set_snapblob(next);
        }
		curr = next;
	
	}
#ifdef DEL
    assert((curr->degree.del_count < MAX_DEL_DEGREE) && (curr->degree.del_count + count) >= count);
	curr->degree.del_count += count;
#else
	assert(0);
#endif
    //assert(curr->del_count <= curr->degree);
}

    
template <class T>
void  onegraph_t<T>::add_nebr_noatomic(vid_t vid, T sid) 
{
    vunit_t<T>* v_unit = get_vunit(vid); 
    delta_adjlist_t<T>* adj_list1 = v_unit->adj_list;
    #ifndef BULK 
    if (adj_list1 == 0 || adj_list1->get_nebrcount() >= adj_list1->get_maxcount()) {
        
        delta_adjlist_t<T>* adj_list = 0;
        snapT_t<T>* curr = v_unit->get_snapblob();
        degree_t new_count = get_total(curr->degree);
        degree_t max_count = new_count;
        if (curr->prev) {
            max_count -= get_total(curr->prev->degree); 
        }
        
        adj_list = new_delta_adjlist(max_count, (new_count >= HUB_COUNT));
        v_unit->set_delta_adjlist(adj_list);
        adj_list1 = adj_list;
    }
    #endif
    adj_list1->add_nebr_noatomic(sid);
}

template <class T>
void onegraph_t<T>::add_nebr_bulk(vid_t vid, T* adj_list2, degree_t count) 
{
    vunit_t<T>* v_unit = get_vunit(vid); 
    delta_adjlist_t<T>* adj_list1 = v_unit->adj_list;
    #ifndef BULK 
    //First add whatever spaces are left
    if (adj_list1 != 0) {
        degree_t left_space = adj_list1->get_maxcount() - adj_list1->get_nebrcount();
        if (left_space < count) { 
            adj_list1->add_nebr_bulk(adj_list2, left_space);
            adj_list2 += left_space;
            count -= left_space; 
        } else {
            adj_list1->add_nebr_bulk(adj_list2, count);
            return;
        }
    }
    if (adj_list1 == 0 || adj_list1->get_nebrcount() >= adj_list1->get_maxcount()) {
        
        delta_adjlist_t<T>* adj_list = 0;
        snapT_t<T>* curr = v_unit->get_snapblob();
        degree_t new_count = get_total(curr->degree);
        degree_t max_count = new_count;
        if (curr->prev) {
            max_count -= get_total(curr->prev->degree);
        }
        
        adj_list = new_delta_adjlist(max_count, (new_count >= HUB_COUNT));
        v_unit->set_delta_adjlist(adj_list);
        adj_list1 = adj_list;
    }
    #endif
    adj_list1->add_nebr_bulk(adj_list2, count);
}

template <class T>
void onegraph_t<T>::del_nebr_noatomic(vid_t vid, T sid) 
{
    vunit_t<T>* v_unit = get_vunit(vid); 
    sid_t actual_sid = TO_SID(get_sid(sid)); 
    degree_t location = find_nebr(vid, actual_sid);
    if (INVALID_DEGREE != location) {
        set_sid(sid, DEL_SID(location));
        delta_adjlist_t<T>* adj_list1 = v_unit->adj_list;
        #ifndef BULK 
        if (adj_list1 == 0 || adj_list1->get_nebrcount() >= adj_list1->get_maxcount()) {
            
            delta_adjlist_t<T>* adj_list = 0;
            snapT_t<T>* curr = v_unit->get_snapblob();
            degree_t new_count = get_total(curr->degree);
            degree_t max_count = new_count;
            if (curr->prev) {
                max_count -= get_total(curr->prev->degree); 
            }
            
            adj_list = new_delta_adjlist(max_count, (new_count >= HUB_COUNT));
            v_unit->set_delta_adjlist(adj_list);
            adj_list1 = adj_list;
        }
        #endif
        adj_list1->add_nebr_noatomic(sid);

     } else {
        //Didn't find the old value. decrease del degree count
	    snapT_t<T>* curr = v_unit->get_snapblob();
#ifdef DEL
		curr->degree.del_count--;
#else
		assert(0);
#endif
     }
}

template <class T>
void onegraph_t<T>::compress()
{
    vid_t   v_count = get_vcount(tid);

    #pragma omp for schedule (dynamic, 256) nowait
    for (vid_t vid = 0; vid < v_count; ++vid) {
        compress_nebrs(vid);
    }
}

template <class T>
status_t onegraph_t<T>::compress_nebrs(vid_t vid)
{
    vunit_t<T>* v_unit = get_vunit(vid); 
    if (v_unit == 0) return eOK;
    
   

    //Get the new count
	sdegree_t sdegree = v_unit->get_degree();
	degree_t nebr_count = get_actual(sdegree);
    degree_t del_count = get_delcount(sdegree);
    
	//Only 1 chain, and no deletion data,then no compaction required
    if (del_count == 0 /*&& (v_unit->delta_adjlist == v_unit->adj_list)*/) {
        return eOK;
    }

    //Allocate new memory
    delta_adjlist_t<T>* adj_list = new_delta_adjlist(nebr_count);
    adj_list->set_nebrcount(nebr_count);
    
    //copy the data from older edge arrays to new edge array
    T* ptr = adj_list->get_adjlist();
    degree_t ret = get_nebrs(vid, ptr, sdegree);
    assert(ret == nebr_count);

    delta_adjlist_t<T>* old_adjlist = v_unit->delta_adjlist;
    //replace the edge array atomically
    /*
    if(true != __sync_bool_compare_and_swap(&v_unit->delta_adjlist, old_adjlist, adj_list)) {
        assert(0);
    }*/
    v_unit->delta_adjlist = adj_list;
    v_unit->adj_list = adj_list;
    v_unit->compress_degree();
    delete_delta_adjlist(old_adjlist, true);//chain free
    return eOK;
}

//Freeing happens upto
template <class T>
status_t onegraph_t<T>::evict_old_adjlist(vid_t vid, degree_t degree)
{
    vunit_t<T>* v_unit = get_vunit(vid); 
    if (v_unit == 0) return eOK;;

    delta_adjlist_t<T>* delta_adjlist = v_unit->delta_adjlist;
    degree_t delta_degree = degree;//nebr_count + del_count; 
    
    delta_adjlist_t<T>* old_adjlist = 0;
    degree_t local_degree = 0;
    degree_t total_count = 0;

    while (delta_adjlist != 0 && delta_degree > 0) {
        local_degree = delta_adjlist->get_nebrcount();
        total_count += local_degree;
        if (total_count >= degree) {
           break; 
        }
        old_adjlist = delta_adjlist;
        delta_adjlist = delta_adjlist->get_next();
        delta_degree -= local_degree;
        delete_delta_adjlist(old_adjlist);
    }

    v_unit->delta_adjlist = delta_adjlist;
    if (delta_adjlist == 0) {
        v_unit->adj_list = 0;
    }
    
    //reduce the degree count
    snapT_t<T>* snap_blob = v_unit->get_snapblob();
    snap_blob->degree -= degree;
    //snap_blob->del_count = 0; 
    
    //v_unit->offset = degree;//XXX
    return eOK;
}

template <class T>
degree_t onegraph_t<T>::get_nebrs(vid_t vid, T* ptr, sdegree_t count/*,edgeT_t<T>* edges, index_t marker*/)
{
    vunit_t<T>* v_unit = get_vunit(vid); 
    if (v_unit == 0) return 0;

    degree_t del_count = get_delcount(count);
    
    //traverse the delta adj list this far
    degree_t delta_degree = get_total(count); 
    delta_adjlist_t<T>* delta_adjlist = v_unit->delta_adjlist;
    
    T* local_adjlist = 0;
    degree_t local_degree = 0;
    degree_t i_count = 0;
    degree_t total_count = 0;
    vid_t src_vid;
    
    if (0 == del_count) {
        while (delta_adjlist != 0 && delta_degree > 0) {
            local_adjlist = delta_adjlist->get_adjlist();
            local_degree = delta_adjlist->get_nebrcount();
            i_count = min(local_degree, delta_degree);
            memcpy(ptr+total_count, local_adjlist, sizeof(T)*i_count);
            total_count+=i_count;
            
            delta_adjlist = delta_adjlist->get_next();
            delta_degree -= local_degree;
        }
        /*for (index_t j=1; j <= marker && total_count<delta_degree; ++j){
            src_vid = TO_VID(get_src(edges[marker-j]));
            if (src_vid == vid) {
                ptr[total_count++] = edges[marker-j].dst_id;
            }
        }*/
    } else {
        degree_t nebr_count = get_actual(count);
        degree_t other_count = delta_degree - nebr_count;
        degree_t* del_pos = (degree_t*)calloc(2*del_count, sizeof(degree_t));
        T* others = (T*)calloc(other_count, sizeof(T));
        
        degree_t pos = 0;
        degree_t idel = 0;
        degree_t other_pos = 0;
        bool is_del = false;

        while (delta_adjlist != 0 && delta_degree > 0) {
            local_adjlist = delta_adjlist->get_adjlist();
            local_degree = delta_adjlist->get_nebrcount();
            i_count = min(local_degree, delta_degree);
            
            for (degree_t i = 0; i < i_count; ++i) {
                is_del = IS_DEL(get_sid(local_adjlist[i])); 
                if (is_del) {
                    pos = UNDEL_SID(get_sid(local_adjlist[i]));
                    if (total_count < nebr_count){
                        del_pos[idel++] = pos;
                        del_pos[idel++] = total_count;
                    } else {
                        if (pos < nebr_count) {
                            del_pos[idel++] = pos;
                        }
                    }
                } 
                if (total_count < nebr_count) {//normal case
                    ptr[total_count++] = local_adjlist[i];
                } else {//space is full
                    others[other_pos++] = local_adjlist[i];
                    if (is_del && pos >= total_count) {
                        others[pos-total_count] = local_adjlist[i];
                    }
                }
            }
            
            delta_adjlist = delta_adjlist->get_next();
            delta_degree -= local_degree;
        }
        assert(other_pos <= other_count);
        assert(idel <= 2*del_count);
        while (other_pos != 0 && idel !=0) {
            if (IS_DEL(get_sid(others[--other_pos]))) continue;
            pos = del_pos[--idel];
            ptr[pos] = others[other_pos];    
        }
        /*
        //handle the edge log
        index_t rem_count = 0;
        index_t new_del = 0;
        for (index_t j = 1, k =0; j <= marker && k < rem_count; ++j) {
            src_vid = TO_VID(get_src(edges[marker-j]));
            if (src_vid != vid) continue;
            is_del = IS_DEL(get_src(edges[marker-j])); 
            if (!is_del && idel !=0) {
                pos = del_pos[--idel];
                ptr[pos] = others[other_pos];    
            } else {
                new_del += (true == is_del);
                if (total_count < nebr_count) {//normal case
                    ptr[total_count++] = edges[marker-j].dst_id;
                } else {//space is full
                    others[other_pos++] = edges[marker-j].dst_id;
                }
            }
            ++k;
        }
        //Need to handle if more deletion has been observed.
        if (new_del) { }*/
        free(del_pos);
        free(others);
    }
    return total_count;
}

template <class T>
degree_t onegraph_t<T>::get_wnebrs(vid_t vid, T* ptr, degree_t start, degree_t count)
{
    vunit_t<T>* v_unit = get_vunit(vid); 
    delta_adjlist_t<T>* delta_adjlist = v_unit->delta_adjlist;
    T* local_adjlist = 0;
    degree_t local_degree = 0;
    degree_t i_count = 0;
    degree_t total_count = 0;
            
    //traverse the delta adj list
    degree_t delta_degree = start; 
    
    while (delta_adjlist != 0 && delta_degree > 0) {
        local_adjlist = delta_adjlist->get_adjlist();
        local_degree = delta_adjlist->get_nebrcount();
        if (delta_degree >= local_degree) {
            delta_adjlist = delta_adjlist->get_next();
            delta_degree -= local_degree;
        } else {
            i_count = local_degree - delta_degree;
            memcpy(ptr, local_adjlist + delta_degree, sizeof(T)*i_count);
            total_count += i_count;
            delta_adjlist = delta_adjlist->get_next();
            delta_degree = 0;
            break;
        }
    }
    
    delta_degree = count;
    while (delta_adjlist !=0 && total_count < count) {
        local_adjlist = delta_adjlist->get_adjlist();
        local_degree = delta_adjlist->get_nebrcount();
        i_count = min(local_degree, delta_degree);
        memcpy(ptr+total_count, local_adjlist, sizeof(T)*i_count);
        total_count+=i_count;
        
        delta_adjlist = delta_adjlist->get_next();
        delta_degree -= local_degree;
        
    }
    return total_count;
}

template <class T>
sdegree_t onegraph_t<T>::get_degree(vid_t vid, snapid_t snap_id)
{
    vunit_t<T>* v_unit = get_vunit(vid);
    sdegree_t sdegree(0);
    if (v_unit) {
        sdegree = v_unit->get_degree(snap_id);
    } 
    return sdegree;
}

template <class T>
degree_t onegraph_t<T>::start(vid_t v, header_t<T>& header, degree_t offset/*=0*/)
{
    delta_adjlist_t<T>* delta_adjlist = get_delta_adjlist(v);
    
    //traverse the delta adj list
    degree_t delta_degree = offset; 
    degree_t local_degree = 0;
    
    while (delta_adjlist != 0 && delta_degree > 0) {
        local_degree = delta_adjlist->get_nebrcount();
        if (delta_degree >= local_degree) {
            delta_adjlist = delta_adjlist->get_next();
            delta_degree -= local_degree;
        } else {
            break;
        }
    }
    
    header.next = delta_adjlist->get_next();
    header.max_count = delta_adjlist->get_maxcount();
    header.count = delta_degree;
    header.adj_list = delta_adjlist->get_adjlist();
    
    return 0;
}

template <class T>
status_t onegraph_t<T>::next(header_t<T>& header, T& dst)
{
    if (header.max_count == header.count) {
        delta_adjlist_t<T>* delta_adjlist = header.next;
        header.next = delta_adjlist->get_next();
        header.max_count = delta_adjlist->get_maxcount();
        header.count = 0;
        header.adj_list = delta_adjlist->get_adjlist();
    }
    
    T* local_adjlist = header.adj_list;
    dst = local_adjlist[header.count++];
    return eOK;
}


#ifdef BULK
template <class T>
void onegraph_t<T>::setup_adjlist()
{
    vid_t  total_thds  = omp_get_num_threads();
    vid_t         tid  = omp_get_thread_num();  
        
    vid_t    v_count = get_vcount(tid);
    vid_t    portion = v_count/total_thds;
    vid_t  vid_start = portion*tid;
    vid_t  vid_end   = portion*(tid + 1);
    if (tid == total_thds - 1) {
        vid_end = v_count;
    }
   
    setup_adjlist_noatomic(vid_start, vid_end); 
}

template <class T>
void onegraph_t<T>::setup_adjlist_noatomic(vid_t vid_start, vid_t vid_end)
{
    degree_t count, del_count, total_count;
/*
	vunit_t<T>* v_unit = 0;
	snapT_t<T>* next;
    snapid_t snap_id = pgraph->snap_id + 1;
	int tid = omp_get_thread_num();
	thd_mem_t<T>* my_thd_mem = thd_mem + tid;
	memset(my_thd_mem, 0, sizeof(thd_mem_t<T>));
	//Memory estimation
    for (vid_t vid = vid_start; vid < vid_end; ++vid) {
        del_count = nebr_count[vid].del_count;
        count = nebr_count[vid].add_count;
        total_count = count + del_count;
        
        if (0 == total_count) { continue; }
        
        next = beg_pos[vid].recycle_snapblob(snap_id);
        if (next != 0) {
            next->del_count += del_count;
            next->degree    += count;
        }
        my_thd_mem->degree_count += (0 == next);
        ++my_thd_mem->dsnap_count;
    
        v_unit = beg_pos[vid].get_vunit();
        my_thd_mem->vunit_count += (v_unit == 0);
        my_thd_mem->delta_size += total_count;
    }

	//Bulk memory allocation
    my_thd_mem->vunit_bulk(my_thd_mem->vunit_count);
    my_thd_mem->snapdegree_bulk(my_thd_mem->degree_count);

	index_t new_count = my_thd_mem->delta_size*sizeof(T) 
						+ my_thd_mem->dsnap_count*sizeof(delta_adjlist_t<T>);
    my_thd_mem->delta_adjlist_bulk(new_count);
*/
	delta_adjlist_t<T>* delta_adjlist = 0;

	//individual allocation
    for (vid_t vid = vid_start; vid < vid_end; ++vid) {
        del_count = nebr_count[vid].del_count;
        count = nebr_count[vid].add_count;
        total_count = count + del_count;
        
        if (0 == total_count) { continue; }

        increment_count_noatomic(vid, count);
        decrement_count_noatomic(vid, del_count);

        //delta adj list allocation
        delta_adjlist = new_delta_adjlist(total_count);
        set_delta_adjlist(vid, delta_adjlist);
        reset_count(vid);
    }
}
#endif

template <class T>
void onegraph_t<T>::setup(tid_t t, vid_t a_max_vcount)
{
    snap_id = 0;
    tid = t;
    max_vcount = a_max_vcount;
    beg_pos = (vert_table_t<T>*)calloc(sizeof(vert_table_t<T>), max_vcount);
    thd_mem = new thd_mem_t<T>; 

    dvt_max_count = DVT_SIZE;
    log_count = DURABLE_SIZE;
    
    if (posix_memalign((void**) &write_seg[0].dvt, 2097152, 
                       dvt_max_count*sizeof(disk_vtable_t))) {
        perror("posix memalign vertex log");    
    }
    if (posix_memalign((void**) &write_seg[1].dvt, 2097152, 
                       dvt_max_count*sizeof(disk_vtable_t))) {
        perror("posix memalign vertex log");    
    }
    if (posix_memalign((void**) &write_seg[2].dvt, 2097152, 
                       dvt_max_count*sizeof(disk_vtable_t))) {
        perror("posix memalign vertex log");    
    }
    if (posix_memalign((void**)&write_seg[0].log_beg, 2097152, log_count)) {
        //log_beg = (index_t*)calloc(sizeof(index_t), log_count);
        perror("posix memalign edge log");
    }
    if (posix_memalign((void**)&write_seg[1].log_beg, 2097152, log_count)) {
        //log_beg = (index_t*)calloc(sizeof(index_t), log_count);
        perror("posix memalign edge log");
    }

#ifdef BULK
    nebr_count = (nebrcount_t*)calloc(sizeof(nebrcount_t), max_vcount);
#endif
}

//returns the location of the found value
template <class T>
degree_t onegraph_t<T>::find_nebr(vid_t vid, sid_t sid) 
{
    //Find the location of deleted one
    vunit_t<T>* v_unit = get_vunit(vid);
    if (0 == v_unit) return INVALID_DEGREE;

    //degree_t  durable_degree = v_unit->count;
    degree_t      ret_degree = INVALID_DEGREE;
    degree_t    local_degree = 0;
    degree_t          degree = 0;
    sid_t               nebr = 0;
    T*         local_adjlist = 0;
    delta_adjlist_t<T>* delta_adjlist = v_unit->delta_adjlist;
    delta_adjlist_t<T>* last = v_unit->adj_list;

    {
        local_adjlist = last->get_adjlist();
        local_degree  = last->get_nebrcount();
        for (degree_t i = local_degree; i != 0; --i) {
            nebr = get_sid(local_adjlist[i-1]);
            if (nebr == sid) {
		ret_degree = i-1;
		break;
            }
        }
	//XXX A better way to do this calculation is possible
	if (ret_degree != INVALID_DEGREE) {
    	    while (delta_adjlist != last) {
        	local_degree  = delta_adjlist->get_nebrcount();
        	degree += local_degree;
        	delta_adjlist = delta_adjlist->get_next();
	    }
	    return ret_degree + degree;
	}
    }
    
    delta_adjlist = v_unit->delta_adjlist;
    degree = 0;
    while (delta_adjlist != last) {
        local_adjlist = delta_adjlist->get_adjlist();
        local_degree  = delta_adjlist->get_nebrcount();
        for (degree_t i = 0; i < local_degree; ++i) {
            nebr = get_sid(local_adjlist[i]);
            if (nebr == sid) {
		ret_degree = i+degree;
            }
        }
        degree += local_degree;
        delta_adjlist = delta_adjlist->get_next();
    }
                
    return ret_degree;

    /*
    //Durable adj list 
    if (durable_degree == 0) return INVALID_DEGREE;

    index_t   offset = v_unit->offset;
    index_t sz_to_read = offset*sizeof(T) + sizeof(durable_adjlist_t<T>);
    durable_adjlist_t<T>* durable_adjlist = (durable_adjlist_t<T>*)malloc(sz_to_read);
    pread(etf, durable_adjlist, sz_to_read, offset);
    T* adj_list = delta_adjlist->get_adjlist();
    for (degree_t i = 0; i < durable_degree; ++i) {
        nebr = get_nebr(adj_list, i);
        if (nebr == sid) {
            return i;
        }
    }*/
}

template <class T>
void onegraph_t<T>::file_open(const string& filename, bool trunc)
{
    file = filename; 
    string  vtfile = filename + ".vtable";
    string  etfile = filename + ".etable";
    //string  stfile = filename + ".stable";

    if (trunc) {
		etf = open(etfile.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
		vtf = open(vtfile.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
        //vtf = fopen(vtfile.c_str(), "wb");
        log_tail = 0;
    } else {
		etf = open(etfile.c_str(), O_RDWR|O_CREAT, S_IRWXU);
		vtf = open(vtfile.c_str(), O_RDWR|O_CREAT, S_IRWXU);
        //vtf = fopen(vtfile.c_str(), "r+b");
    }
}

#include <stdio.h>
template <class T>
void onegraph_t<T>::handle_write(bool clean /* = false */)
{
    vid_t   v_count = get_vcount(tid);
    vid_t last_vid1 = 0;
    vid_t last_vid2 = 0;
    
    string etfile = file + ".etable";
    string vtfile = file + ".vtable";
    string etfile_new = file + ".etable_new";
    string vtfile_new = file + ".vtable_new";
    
    int etf_new = etf;
    int vtf_new = vtf;
    
    if (clean) {
		vtf_new = open(vtfile_new.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
		etf_new = open(etfile_new.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
        log_tail = 0;
    }
     
    write_seg_t* seg1 = new write_seg_t(write_seg[0]);
    write_seg_t* seg2 = new write_seg_t(write_seg[1]); 
    write_seg_t* seg3 = new write_seg_t(write_seg[2]); 
    
	prepare_dvt(seg1, last_vid1, clean);
    
    do {
        last_vid2 = last_vid1;
        swap(seg1, seg2);
        #pragma omp parallel num_threads(THD_COUNT)
        {
            #pragma omp master
            {
                prepare_dvt(seg1, last_vid1,clean);
            }
            if (1 == omp_get_thread_num())
            {
                //Write the dvt log
                if (seg2->dvt_count) {
                    off_t size = sizeof(disk_vtable_t)*seg2->dvt_count;
					if (size != write(vtf_new, seg2->dvt, size)) {
						perror("write issue in dvt");
						assert(0);
					}
                }
			}
            if (2 == omp_get_thread_num())
            {
				//Write new adj list
				if (seg3->log_head != 0) {
					off_t size = seg3->log_head;
					if(size != write(etf_new, seg3->log_beg, size)) {
						perror("pwrite issue in adj list");
						assert(0);
					}
				}
            }

			//adj_update(seg3);
            adj_prep(seg2);
        }

		swap(seg2, seg3);
		seg2->log_head = 0;
		seg2->dvt_count = 0;
		seg2->log_tail = seg3->log_tail;
		seg2->log_beg = seg3->log_beg;
		
    } while(last_vid2 < v_count);

	//The last write and adj update	
	#pragma omp parallel num_threads(THD_COUNT)
	{
		//Write new adj list
		#pragma omp master
		{
            if (seg3->log_head != 0) {
                off_t size = seg3->log_head;
                if (size != write(etf_new, seg3->log_beg, seg3->log_head)) {
                    perror("pwrite issue");
                    assert(0);
                }
            }
		}
	    //adj_update(seg3);
	}

    //Rename the files
    if (clean) {
        swap(vtf, vtf_new);
        close(vtf_new);
        remove(vtfile.c_str());
        rename(vtfile_new.c_str(), vtfile.c_str());
        
        
        swap(etf, etf_new);
        //XXX Should be closed only when no one is using it. 
        close(etf_new);
        remove(etfile.c_str());
        rename(etfile_new.c_str(), etfile.c_str());
    }

    //adjlog_tail = adjlog_head;
	//adjlog_head = 0;
}

template <class T>
void onegraph_t<T>::prepare_dvt (write_seg_t* seg, vid_t& last_vid, bool clean /* = false */)
{
    vid_t    v_count = get_vcount(tid);
    durable_adjlist_t<T>* adj_list2 = 0;
    snapT_t<T>* curr = 0;
	disk_vtable_t* dvt1 = 0;
	//Note the initial offset
	seg->log_tail = log_tail;
	seg->log_head = 0;
	seg->dvt_count = 0;
    vunit_t<T>* v_unit = 0;
	
    for (vid_t vid = last_vid; vid < v_count ; ++vid) {
        v_unit = get_vunit(vid);
        if (0 == v_unit || (0 == v_unit->adj_list  && !clean)) continue;
		
        curr		= v_unit->get_snapblob();
		if ((seg->log_head + get_total(curr->degree)*sizeof(T) + sizeof(durable_adjlist_t<T>)  > log_count) ||
            (seg->dvt_count >= dvt_max_count)) {
            last_vid = vid;
            log_tail += seg->log_head;
            return;
		}
        
        //durable adj list allocation
		adj_list2   = new_adjlist(seg, get_total(curr->degree));
        
		//v_unit log for disk write
        dvt1              = new_dvt(seg);
		dvt1->vid         = vid;
		dvt1->count	     = curr->degree;
        dvt1->file_offset = ((char*)adj_list2) - seg->log_beg;
        dvt1->file_offset += seg->log_tail;
    }

    last_vid = v_count;
    log_tail += seg->log_head;
    return;
}

template <class T>
void onegraph_t<T>::adj_prep(write_seg_t* seg)
{
	vid_t vid;
	disk_vtable_t* dvt1 = 0;
	//vunit_t<T>* v_unit = 0;
	vunit_t<T>* prev_v_unit = 0;
	//index_t  prev_offset;
    degree_t total_count = 0;
    //degree_t prev_total_count;

	delta_adjlist_t<T>* delta_adjlist = 0;
	durable_adjlist_t<T>* durable_adjlist = 0;
    T* adj_list1 = 0;

    #pragma omp for nowait 
	for (vid_t v = 0; v < seg->dvt_count; ++v) {
		dvt1 = seg->dvt + v;
		vid = dvt1->vid;

		prev_v_unit       = get_vunit(vid);
		//prev_total_count  = prev_v_unit->count;
		//prev_offset       = prev_v_unit->offset;
        total_count       = get_total(dvt1->count);
		
		//Find the allocated durable adj list
		durable_adjlist = (durable_adjlist_t<T>*)(seg->log_beg + dvt1->file_offset 
												  - seg->log_tail);
        adj_list1 = durable_adjlist->get_adjlist();
	    /*
        //Copy the Old durable adj list
		if (prev_total_count) {
			//Read the old adj list from disk
            index_t sz_to_read = sizeof(durable_adjlist_t<T>) + prev_total_count*sizeof(T);
			pread(etf, durable_adjlist , sz_to_read, prev_offset);
			adj_list1 += prev_total_count;
        }*/
        
        durable_adjlist->set_nebrcount(total_count);

        //Copy the new in-memory adj-list
		delta_adjlist = prev_v_unit->delta_adjlist;
        while(delta_adjlist) {
			memcpy(adj_list1, delta_adjlist->get_adjlist(),
				   delta_adjlist->get_nebrcount()*sizeof(T));
			adj_list1 += delta_adjlist->get_nebrcount();
			delta_adjlist = delta_adjlist->get_next();
		}
    }
}

template <class T>
void onegraph_t<T>::read_vtable()
{
    off_t size = fsize(vtf);
    if (size == -1L) {
        assert(0);
    }
    vid_t count = (size/sizeof(disk_vtable_t));
    
    index_t adj_size = 0;
    index_t offset = 0;
    char* adj_list;
    //durable_adjlist_t<T>* durable_adjlist = 0;
    delta_adjlist_t<T>*   delta_adjlist = 0;

    index_t sz_read = 0;
	vid_t vid = 0;
	vunit_t<T>* v_unit = 0;
    disk_vtable_t* dvt = write_seg[0].dvt;
	snapT_t<T>* next ; 
    snapid_t snap_id = 1;
     
    //read in batches
    while (count != 0 ) {
        //vid_t read_count = read(dvt, sizeof(disk_vtable_t), dvt_max_count, vtf);
        vid_t read_count = read(vtf, dvt, sizeof(disk_vtable_t)*dvt_max_count);
        read_count /= sizeof(disk_vtable_t); 
        
        //read edge file
        offset = dvt[0].file_offset;
        adj_size = dvt[read_count -1].file_offset + sizeof(durable_adjlist_t<T>)
                +(get_total(dvt[read_count-1].count))*sizeof(T)
                - offset;
        adj_list = (char*)malloc(adj_size);

        while (sz_read != adj_size) {
           sz_read += pread(etf, adj_list+offset, adj_size - sz_read, offset);
           offset += sz_read;
        }
        assert(sz_read == adj_size);

        for (vid_t v = 0; v < read_count; ++v) {
			vid = dvt[v].vid;
            
            v_unit = new_vunit();
            set_vunit(vid, v_unit);
			
            //allocate new snapshot for degree, and initialize
			next = new_snapdegree();
            next->snap_id       = snap_id;
            //next->next          = 0;
            next->degree        = dvt[v].count;
            v_unit->set_snapblob(next);

            //assign delta adjlist
            delta_adjlist = (delta_adjlist_t<T>*)(adj_list + dvt[v].file_offset);
            delta_adjlist->add_next(0);
            v_unit->delta_adjlist = delta_adjlist;
            v_unit->adj_list = delta_adjlist;
            
        }
        count -= read_count;
    }
}
/*****************************/
/*
template <class T>
void onegraph_t<T>::read_etable()
{
    if (etf == -1) {
		etf = open(etfile.c_str(), O_RDWR);
        //etf = fopen(etfile.c_str(), "r+b");//append/write + binary
    }
    assert(etf != 0);

    
    
    index_t size = fsize(etf);
    if (size == -1L) {
        assert(0);
    }
    sid_t edge_count = size/sizeof(T);
    //fread(log_beg, sizeof(T), edge_count, etf);
    //while ( 0!= (sz_read = read(etf, log_beg, size, 0))) {//offset as 0 }
    
    log_head = edge_count;
    log_wtail = log_head;
    log_whead = log_head;

    for(sid_t vid = 0; vid < v_count; vid++) {
        //set the in-memory adj-list 
    }
}
    */

/*
template <class T>
void onegraph_t<T>::adj_write(write_seg_t* seg)
{
	vid_t vid;
	disk_vtable_t* dvt1 = 0;
	vunit_t<T>* v_unit = 0;
	vunit_t<T>* prev_v_unit = 0;
	index_t  prev_offset;
    degree_t total_count = 0;
    degree_t prev_total_count;

	delta_adjlist_t<T>* delta_adjlist = 0;
	durable_adjlist_t<T>* durable_adjlist = 0;
    T* adj_list1 = 0;

    #pragma omp for schedule(dynamic, 256) nowait
	for (vid_t v = 0; v < seg->dvt_count; ++v) {
		dvt1 = seg->dvt + v;
		vid = dvt1->vid;
		if (0 == nebr_count[vid].adj_list) continue;
		prev_v_unit       = beg_pos[vid].get_vunit();
		prev_total_count  = prev_v_unit->count;
		prev_offset       = prev_v_unit->offset;
        total_count       = dvt1->count + dvt1->del_count;
		
		//Find the allocated durable adj list
		durable_adjlist = (durable_adjlist_t<T>*)(seg->log_beg + dvt1->file_offset - log_tail);
        adj_list1 = durable_adjlist->get_adjlist();
	   
        //Copy the Old durable adj list
		if (prev_total_count) {
			//Read the old adj list from disk
            index_t sz_to_read = sizeof(durable_adjlist_t<T>) + prev_total_count*sizeof(T);
			pread(etf, durable_adjlist , sz_to_read, prev_offset);
			adj_list1 += prev_total_count;
        }
        
        durable_adjlist->set_nebrcount(total_count);

        //Copy the new in-memory adj-list
		delta_adjlist = prev_v_unit->delta_adjlist;
        while(delta_adjlist) {
			memcpy(adj_list1, delta_adjlist->get_adjlist(),
				   delta_adjlist->get_nebrcount()*sizeof(T));
			adj_list1 += delta_adjlist->get_nebrcount();
			delta_adjlist = delta_adjlist->get_next();
		}

		v_unit = new_vunit();
		v_unit->count = total_count;
		v_unit->offset = dvt1->file_offset;// + log_tail;
		v_unit->delta_adjlist = 0;
		//beg_pos[vid].set_vunit(v_unit);
		nebr_count[vid].v_unit = v_unit;
            
		nebr_count[vid].add_count = 0;
        nebr_count[vid].del_count = 0;
        //nebr_count[vid].adj_list = 0;
    }
}

template <class T>
void onegraph_t<T>::adj_update(write_seg_t* seg)
{
	vid_t vid;
	disk_vtable_t* dvt1 = 0;
	vunit_t<T>* v_unit = 0;
	vunit_t<T>* v_unit1 = 0;

    #pragma omp for nowait 
	for (vid_t v = 0; v < seg->dvt_count; ++v) {
		dvt1 = seg->dvt + v;
		vid = dvt1->vid;
		v_unit =   new_vunit(seg, v);
		v_unit1 = beg_pos[vid].set_vunit(v_unit);
        vunit_ind[(seg->my_vunit_head + v) % vunit_count] = v_unit1 - vunit_beg;
	}
}

*/


    /*
template <class T>
void onegraph_t<T>::update_count() 
{
    vid_t    v_count = TO_VID(super_id);
    disk_snapT_t<T>* slog = (disk_snapT_t<T>*)snap_log;
    T* adj_list1 = 0;
    T* adj_list2 = 0;
    T* prev_adjlist = 0;
	vunit_t<T>* v_unit = 0;
	vunit_t<T>* prev_v_unit = 0;
	delta_adjlist_t<T>* delta_adjlist = 0;
    index_t j = 0;
    snapT_t<T>* curr = 0;
    index_t count;
    
    #pragma omp for
    for (vid_t vid = 0; vid < v_count ; ++vid) {
        if (0 == nebr_count[vid].adj_list) continue;
		
		prev_v_unit = beg_pos[vid].get_vunit();
        prev_adjlist = prev_v_unit->adj_list;
        
        //durable adj list allocation
        curr		= beg_pos[vid].get_snapblob();
        adj_list1   = new_adjlist(curr->degree + 1);
        adj_list2   = adj_list1;
        
        //Copy the Old durable adj list
        set_nebrcount1(adj_list1, curr->degree);
        adj_list1 += 1;
        if (0 != prev_adjlist) {
            count = get_nebrcount1(prev_adjlist);
            memcpy(adj_list1, prev_adjlist, count*sizeof(T));
            adj_list1 += count;
        }

        //Copy the new in-memory adj-list
		delta_adjlist = prev_v_unit->delta_adjlist;
        while(delta_adjlist) {
			memcpy(adj_list1, delta_adjlist->get_adjlist(),
				   delta_adjlist->get_nebrcount()*sizeof(T));
			adj_list1 += delta_adjlist->get_nebrcount();
			delta_adjlist = delta_adjlist->get_next();
		}


		v_unit = new_vunit();
		v_unit->count = curr->degree;
		v_unit->adj_list = adj_list2;
		v_unit->delta_adjlist = 0;
		beg_pos[vid].set_vunit(v_unit);
            
		//snap log for disk write
		j = __sync_fetch_and_add(&snap_whead, 1L); 
		slog[j].vid       = vid;
		slog[j].snap_id   = snap_id;
		//slog[j].del_count = del_count;
		slog[j].degree    = curr->degree;
		if (curr) { slog[j].degree += curr->degree; }

		//v_unit log for disk write
        j = __sync_fetch_and_add(&dvt_count, 1L); 
        dvt[j].vid         = vid;
		dvt[j].count	   = curr->degree;
        dvt[j].file_offset = adj_list2 - log_beg;
        dvt[j].old_offset  =  prev_adjlist - log_beg;
        
		nebr_count[vid].add_count = 0;
        nebr_count[vid].del_count = 0;
        nebr_count[vid].adj_list = 0;
    }
    log_whead = log_head;
    adjlog_head = 0;
}
*/

template <class T>
onegraph_t<T>::onegraph_t() 
{
    beg_pos = 0;
    vtf = -1;
    etf = -1;
    stf = 0;
    log_count = 0;
    dvt_max_count = 0;
    write_seg[0].reset();
    write_seg[1].reset();
    write_seg[2].reset();

#ifdef BULK
    nebr_count = 0;
#endif
}
