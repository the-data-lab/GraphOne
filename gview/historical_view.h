#pragma once

#include "wsstream_view.h"

template <class T>
class prior_snap_t : public snap_t<T> {
 protected:    
    using snap_t<T>::pgraph;    
    using snap_t<T>::graph_out;
    using snap_t<T>::graph_in;
    using snap_t<T>::degree_in;
    using snap_t<T>::degree_out;
    using snap_t<T>::v_count;
    
    sdegree_t*        wdegree_out;
    sdegree_t*        wdegree_in;
 public:
    void create_view(pgraph_t<T>* pgraph1, index_t start_offset, index_t end_offset);
};

template <class T>
void prior_snap_t<T>::create_view(pgraph_t<T>* pgraph1, index_t start_offset, index_t end_offset)
{
    pgraph   = pgraph1;
    
    v_count  = g->get_type_scount();
    graph_out = pgraph->sgraph_out[0];
    degree_out = (sdegree_t*) calloc(v_count, sizeof(sdegree_t));
    wdegree_out= (sdegree_t*) calloc(v_count, sizeof(sdegree_t));
    
    if (pgraph->sgraph_in == pgraph->sgraph_out) {
        graph_in   = graph_out;
        degree_in  = degree_out;
        wdegree_in = wdegree_out;
    } else if (pgraph->sgraph_in != 0) {
        graph_in   = pgraph->sgraph_in[0];
        degree_in  = (sdegree_t*) calloc(v_count, sizeof(sdegree_t));
        wdegree_in = (sdegree_t*) calloc(v_count, sizeof(sdegree_t));
    }

    if (0 != start_offset) {
        pgraph->create_degree(wdegree_out, wdegree_in, 0, start_offset);
        memcpy(degree_out, wdegree_out, sizeof(sdegree_t)*v_count);
        
        if (pgraph->sgraph_out != pgraph->sgraph_in && pgraph->sgraph_in != 0) {
            memcpy(degree_in, wdegree_in, sizeof(sdegree_t)*v_count);
        }
    }
        
    pgraph->create_degree(degree_out, degree_in, start_offset, end_offset);
}
