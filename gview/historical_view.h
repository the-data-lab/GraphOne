#pragma once

#include "wsstream_view.h"

template <class T>
class prior_snap_t : public wsnap_t<T> {
 protected:    
    using wsnap_t<T>::pgraph;    
    using wsnap_t<T>::graph_out;
    using wsnap_t<T>::graph_in;
    using wsnap_t<T>::degree_in;
    using wsnap_t<T>::degree_in1;
    using wsnap_t<T>::degree_out;
    using wsnap_t<T>::degree_out1;
    using wsnap_t<T>::v_count;
 public:
    void create_view(pgraph_t<T>* pgraph1, index_t start_offset, index_t end_offset);
};

template <class T>
void prior_snap_t<T>::create_view(pgraph_t<T>* pgraph1, index_t start_offset, index_t end_offset)
{
    pgraph   = pgraph1;
    
    v_count  = g->get_type_scount();
    graph_out = pgraph->sgraph_out[0];
    degree_out = (degree_t*) calloc(v_count, sizeof(degree_t));
    degree_out1= (degree_t*) calloc(v_count, sizeof(degree_t));
    
    if (pgraph->sgraph_in == pgraph->sgraph_out) {
        graph_in   = graph_out;
        degree_in  = degree_out;
        degree_in1 = degree_out1;
    } else if (pgraph->sgraph_in != 0) {
        graph_in   = pgraph->sgraph_in[0];
        degree_in  = (degree_t*) calloc(v_count, sizeof(degree_t));
        degree_in1 = (degree_t*) calloc(v_count, sizeof(degree_t));
    }

    if (0 != start_offset) {
        pgraph->create_degree(degree_out1, degree_in1, 0, start_offset);
        memcpy(degree_out, degree_out1, sizeof(degree_t)*v_count);
        
        if (pgraph->sgraph_out != pgraph->sgraph_in && pgraph->sgraph_in != 0) {
            memcpy(degree_in, degree_in1, sizeof(degree_t)*v_count);
        }
    }
        
    pgraph->create_degree(degree_out, degree_in, start_offset, end_offset);
}
