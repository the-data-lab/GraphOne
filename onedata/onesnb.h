#pragma once

#include "graph_base.h"

typedef uint32_t bpart_t;
typedef uint32_t spart_t;

//For tile count in the graph
#define bit_shift2 16
#define part_mask2 (-1UL << bit_shift2)
#define part_mask2_2 ~(part_mask2)

//For tiles in a physical group
#define p_p 256 
#define bit_shift3 8 
#define part_mask3 (-1UL << bit_shift3)
#define part_mask3_2 ~(part_mask3)

//For physical group count in the graph
//bit_shift3+bit_shift3
#define bit_shift1 24 
#define part_mask1 (-1UL << bit_shift1)
#define part_mask1_2 ~(part_mask1) 

inline index_t
beg_edge_offset2(vid_t i, vid_t j, vid_t part_count)
{
    return ((i*part_count + j) << bit_shift2);
}

template <class T>
class onesnb_t : public onegraph_t<T> {
 private:
     vid_t p;

 public:
    void  setup(tid_t tid, vid_t a_max_vcount);
    void  archive(edgeT_t<T>* edge, index_t count, snapid_t a_snapid);
    void  add_nebr_noatomic(vid_t vid, T sid);
};

template <class T>
void onesnb_t<T>::setup(tid_t t, vid_t a_max_vcount)
{
    //onegraph_t<T>::setup(t, a_max_vcount);
    this->snap_id = 0;
    this->tid = t;
    p = (a_max_vcount >> bit_shift1) + (0 != (a_max_vcount & part_mask1_2));
    this->max_vcount = (p*p << bit_shift2);
    this->beg_pos = (vert_table_t<T>*)calloc(sizeof(vert_table_t<T>), a_max_vcount);
    this->thd_mem = new thd_mem_t<T>; 
}

template <class T>
void onesnb_t<T>::archive(edgeT_t<T>* edges, index_t count, snapid_t a_snapid)
{
    T dst;
    vid_t v0, v1;
    bpart_t j, i;
    spart_t s_i, s_j;
	
    vid_t offset, index;
    snb_t snb;
    
    this->snap_id = a_snapid;

    for (index_t e = 0; e < count; ++e) {
        v0 = edges[e].src_id;
        v1 = get_dst(edges[e]);
        
        i = (v0 >> bit_shift1);
        j = (v1 >> bit_shift1);
        s_i = ((v0 >> bit_shift2) & part_mask3_2);
        s_j = ((v1 >> bit_shift2) & part_mask3_2);
        
        offset = beg_edge_offset2(i,j, p);
        index = offset + ((s_i<< bit_shift3) + s_j);

        this->increment_count_noatomic(index);
    }
    
    for (index_t e = 0; e < count; ++e) {
        v0 = edges[e].src_id;
        v1 = get_sid(edges[e].dst_id);
        
        i = (v0 >> bit_shift1);
        j = (v1 >> bit_shift1);
        s_i = ((v0 >> bit_shift2) & part_mask3_2);
        s_j = ((v1 >> bit_shift2) & part_mask3_2);
        
        offset = beg_edge_offset2(i,j, p);
        index = offset + ((s_i<< bit_shift3) + s_j);
        
        snb.src = v0;
        snb.dst = v1;
        dst = edges[e].dst_id;
        set_snb(dst, snb);
        this->add_nebr_noatomic(index, dst);
    }
}

template <class T>
void  onesnb_t<T>::add_nebr_noatomic(vid_t vid, T sid) 
{
    vunit_t<T>* v_unit = this->get_vunit(vid); 
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
        
        adj_list = this->new_delta_adjlist(max_count, (new_count >= HUB_COUNT));
        v_unit->set_delta_adjlist(adj_list);
        adj_list1 = adj_list;
    }
    #endif
    //if (IS_DEL(get_sid(sid))) { 
    //    return del_nebr_noatomic(vid, sid);
    //}
    adj_list1->add_nebr_noatomic(sid);
}
