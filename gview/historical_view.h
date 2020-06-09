#pragma once

#include "static_view.h"

template <class T>
void slide_forward(sdegree_t* degree_out, sdegree_t* degree_in, edgeT_t<T>* edges, index_t edge_count)
{
    vid_t src_vid, dst_vid;
    edgeT_t<T> edge;
    bool is_del = false;
    for (index_t i = 0; i < edge_count; ++i) {
        edge = edges[i];
        src_vid = TO_VID(get_src(edge));
        dst_vid = TO_VID(get_dst(edge));
        is_del = IS_DEL(get_src(edge));
#ifdef DEL
        if (is_del) {
        __sync_fetch_and_add(&degree_out[src_vid].del_count, 1);
        __sync_fetch_and_add(&degree_in[dst_vid].del_count, 1);
        } else {
        __sync_fetch_and_add(&degree_out[src_vid].add_count, 1);
        __sync_fetch_and_add(&degree_in[dst_vid].add_count, 1);
        }
#else
        if (is_del) {assert(0);
        __sync_fetch_and_add(&degree_out+src_vid, 1);
        __sync_fetch_and_add(&degree_in+dst_vid, 1);
        }
#endif
    }
}

template <class T>
void slide_backward(sdegree_t* degree_out, sdegree_t* degree_in, edgeT_t<T>* edges, index_t edge_count)
{
    vid_t src_vid, dst_vid;
    edgeT_t<T> edge;
    bool is_del = false;
    for (index_t i = 0; i < edge_count; ++i) {
        edge = edges[i];
        src_vid = TO_VID(get_src(edge));
        dst_vid = TO_VID(get_dst(edge));
        is_del = IS_DEL(get_src(edge));
#ifdef DEL
        if (is_del) {
        __sync_fetch_and_sub(&degree_out[src_vid].del_count, 1);
        __sync_fetch_and_sub(&degree_in[dst_vid].del_count, 1);
        } else {
        __sync_fetch_and_sub(&degree_out[src_vid].add_count, 1);
        __sync_fetch_and_sub(&degree_in[dst_vid].add_count, 1);
        }
#else
        if (is_del) {assert(0);
        __sync_fetch_and_sub(&degree_out+src_vid, 1);
        __sync_fetch_and_sub(&degree_in+dst_vid, 1);
        }
#endif
    }
}

template <class T>
void slide_forwarduni(sdegree_t* degree_out, edgeT_t<T>* edges, index_t edge_count)
{
    vid_t src_vid, dst_vid;
    edgeT_t<T> edge;
    bool is_del = false;
    #pragma omp for
    for (index_t i = 0; i < edge_count; ++i) {
        edge = edges[i];
        src_vid = TO_VID(get_src(edge));
        dst_vid = TO_VID(get_dst(edge));
        is_del = IS_DEL(get_src(edge));
#ifdef DEL
        if (is_del) {
        __sync_fetch_and_add(&degree_out[src_vid].del_count, 1);
        } else {
        __sync_fetch_and_add(&degree_out[src_vid].add_count, 1);
        }
#else
        if (is_del) {assert(0);
        __sync_fetch_and_add(&degree_out+src_vid, 1);
        }
#endif
    }
}

template <class T>
void slide_backwarduni(sdegree_t* degree_out, edgeT_t<T>* edges, index_t edge_count)
{
    vid_t src_vid, dst_vid;
    edgeT_t<T> edge;
    bool is_del = false;
    #pragma omp for
    for (index_t i = 0; i < edge_count; ++i) {
        edge = edges[i];
        src_vid = TO_VID(get_src(edge));
        dst_vid = TO_VID(get_dst(edge));
        is_del = IS_DEL(get_src(edge));
#ifdef DEL
        if (is_del) {
        __sync_fetch_and_sub(&degree_out[src_vid].del_count, 1);
        } else {
        __sync_fetch_and_sub(&degree_out[src_vid].add_count, 1);
        }
#else
        if (is_del) {assert(0);
        __sync_fetch_and_sub(&degree_out+src_vid, 1);
        }
#endif
    }
}

template <class T>
class hsnap_t : public snap_t<T> {
 protected:    
    using snap_t<T>::pgraph;    
    using snap_t<T>::graph_out;
    using snap_t<T>::graph_in;
    using snap_t<T>::degree_in;
    using snap_t<T>::degree_out;
    using snap_t<T>::v_count;
    using snap_t<T>::edges;
    using snap_t<T>::edge_count;

 public:
    void init_view(pgraph_t<T>* pgraph1, index_t a_flag) {
        snap_t<T>::init_view(pgraph1, a_flag);
        snap_t<T>::update_view();
    }
    
    void update_view(index_t end_offset) {
        edge_count = 0;
        if(end_offset < this->update_marker) {//slide backward
            edge_count = this->update_marker - end_offset;
            edges = (edgeT_t<T>*)realloc(edges, edge_count*sizeof(edgeT_t<T>));
            pgraph->get_prior_edges(end_offset, this->update_marker, edges);
            if (this->is_ddir() || this->is_udir()) {
                slide_backward(degree_out, degree_in, edges, edge_count);
            } else if (this->is_unidir()) {
                slide_backwarduni(degree_out, edges, edge_count);
            }
        } else if (end_offset > this->update_marker){ //slide forward
            edge_count = end_offset - this->update_marker;
            edges = (edgeT_t<T>*)realloc(edges, edge_count*sizeof(edgeT_t<T>));
            pgraph->get_prior_edges(this->update_marker, end_offset, edges);
            if (this->is_ddir() || this->is_udir()) {
                slide_forward(degree_out, degree_in, edges, edge_count);
            } else if (this->is_unidir()) {
                slide_forwarduni(degree_out, edges, edge_count);
            }
        } else {
            //all good
        }
        this->update_marker = end_offset;
    }
};

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
