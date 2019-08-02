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


//2*bit_shift3
#define bit_shift4 16 
#define part_mask4 (-1UL << bit_shift4)
#define part_mask4_2 ~(part_mask4)

inline index_t
beg_edge_offset2(vid_t i, vid_t j, vid_t part_count)
{
    return ((i*part_count + j) << bit_shift4);
}

template <class T>
class onesnb_t : onegraph_t<T> {
 private:
     vid_t p;

 public:
    void  setup(tid_t tid, vid_t a_max_vcount);
    void  archive(edgeT_t<T>* edge, index_t count, snapid_t a_snapid);
};

template <class T>
void onesnb_t<T>::setup(tid_t t, vid_t a_max_vcount)
{
    onegraph_t<T>::setup(t, a_max_vcount);
    p = (a_max_vcount >> bit_shift1) + (0 != (a_max_vcount & part_mask1_2));
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

    for (index_t i = 0; i < count; ++i) {
        v0 = edges[i].src_id;
        v1 = get_sid(edges[i].dst_id);
        
        i = (v0 >> bit_shift1);
        j = (v1 >> bit_shift1);
        s_i = ((v0 >> bit_shift2) & part_mask3_2);
        s_j = ((v1 >> bit_shift2) & part_mask3_2);
        
        offset = beg_edge_offset2(i,j, p);
        index = offset + ((i<< bit_shift3) + j);

        this->increment_count_noatomic(index);
    }
    
    for (index_t i = 0; i < count; ++i) {
        v0 = edges[i].src_id;
        v1 = get_sid(edges[i].dst_id);
        
        i = (v0 >> bit_shift1);
        j = (v1 >> bit_shift1);
        s_i = ((v0 >> bit_shift2) & part_mask3_2);
        s_j = ((v1 >> bit_shift2) & part_mask3_2);
        
        offset = beg_edge_offset2(i,j, p);
        index = offset + ((i<< bit_shift3) + j);
        
        snb.src = 0;
        snb.dst = 0;
        dst = edges[i].dst_id;
        set_sid(dst, snb);
        this->add_nebr_noatomic(index, dst);
    }
}
