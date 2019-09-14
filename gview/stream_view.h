#pragma once

template <class T>
class stream_t : public gview_t<T> {
 public:
    index_t          snap_marker;//Compute starting offset
    edgeT_t<T>*      edges; //new edges
    index_t          edge_count;//their count
    
    using gview_t<T>::pgraph;
    using gview_t<T>::v_count;
    
 public:
    stream_t(){
        edges = 0;
        edge_count = 0;
        snap_marker = 0;
    }
 public:
    inline edgeT_t<T>* get_edges() { return edges;}
    inline index_t     get_edgecount() { return edge_count;}
    
    inline void        set_edges(edgeT_t<T>*a_edges) {edges = a_edges;}
    inline void        set_edgecount(index_t a_edgecount){edge_count = a_edgecount;}
    inline index_t     get_snapmarker() {return snap_marker;}
    

 public:   
    status_t update_view();
    void init_view(pgraph_t<T>* pgraph, index_t a_flag);
};

template <class T>
void stream_t<T>::init_view(pgraph_t<T>* ugraph, index_t a_flag)
{
    pgraph = ugraph;
    this->flag = a_flag;
    v_count = g->get_type_scount();
    
    blog_t<T>* blog = ugraph->blog;
    snap_marker = 0;
    edges      = 0;
    edge_count = 0;

}

template <class T>
status_t stream_t<T>::update_view()
{
    blog_t<T>* blog = pgraph->blog;
    index_t  marker = blog->blog_head;
    edgeT_t<T>* edge = blog->blog_beg + marker  - 1;
    if (marker > 0 && edge->src_id == get_dst(edge)) {
        --marker;
    }
   
    if (marker > snap_marker) {
        //XXX need to copy it
        edges = blog->blog_beg + snap_marker;
        edge_count = marker - snap_marker;
        snap_marker = marker;
        return eOK;
    } 
    return eNoWork;
}
