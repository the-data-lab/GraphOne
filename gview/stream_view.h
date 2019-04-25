#pragma once

template <class T>
class stream_t {
 public:
    index_t          edge_offset;//Compute starting offset
    edgeT_t<T>*      edges; //mew edges
    index_t          edge_count;//their count
    pgraph_t<T>*     pgraph;
    
    void*            algo_meta;//algorithm specific data
    typename callback<T>::func   stream_func;

 public:
    stream_t(){
        edges = 0;
        edge_count = 0;
        algo_meta = 0;
    }
 public:
    inline void    set_algometa(void* a_meta) {algo_meta = a_meta;}
    inline void*   get_algometa() {return algo_meta;}

    inline edgeT_t<T>* get_edges() { return edges;}
    inline void        set_edges(edgeT_t<T>*a_edges) {edges = a_edges;}
    
    inline index_t     get_edgecount() { return edge_count;}
    inline void        set_edgecount(index_t a_edgecount){edge_count = a_edgecount;}

 public:   
    void update_stream_view();
};


template <class T>
void stream_t<T>::update_stream_view()
{
    blog_t<T>* blog = pgraph->blog;
    index_t  marker = blog->blog_head;
    
    index_t a_edge_count = edge_count;
    
    //XXX need to copy it
    edges = blog->blog_beg + edge_offset + a_edge_count;
    edge_count = marker - edge_offset;
    edge_offset += a_edge_count;
}
