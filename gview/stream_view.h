#pragma once

template <class T>
class stream_t : public gview_t<T> {
 public:
    using gview_t<T>::pgraph;
    using gview_t<T>::v_count;
    using gview_t<T>::reg_id;
    blog_reader_t<T> reader;
    
 public:
    stream_t(){}
    ~stream_t() {
        if (reg_id != -1) reader.blog->unregister_reader(reg_id);
    }
    inline edgeT_t<T>* get_edges() { return reader.blog->blog_beg;}
    inline index_t     get_snapmarker() {return reader.marker;}
    inline void        update_view_done() { reader.tail = reader.marker;}
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
    reader.blog = pgraph->blog;
    reg_id = reader.blog->register_reader(&reader);
}

template <class T>
status_t stream_t<T>::update_view()
{
    index_t  marker = reader.blog->blog_head;

    if (marker - reader.marker < 65536) { usleep(100000); }
   
    marker = reader.blog->blog_head;
    if (marker > reader.marker) {
        //XXX need to copy it
        reader.tail = reader.marker;
        reader.marker = marker;

        return eOK;
    }
    return eNoWork;
}
