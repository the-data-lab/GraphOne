#pragma once

#include "static_view.h"

template <class T>
class sstream_t : public snap_t<T> {
 public:
    using snap_t<T>::pgraph;

 protected:
    using snap_t<T>::degree_in;
    using snap_t<T>::degree_out;
    using snap_t<T>::graph_out;
    using snap_t<T>::graph_in;
    using snap_t<T>::snapshot;
    using snap_t<T>::prev_snapshot;
    using snap_t<T>::edges;
    using snap_t<T>::edge_count;
    using snap_t<T>::v_count;
    using snap_t<T>::flag;
    using snap_t<T>::reader;
    using snap_t<T>::reg_id;
    //For edge-centric
    edgeT_t<T>*      new_edges;//New edges
    index_t          new_edge_count;//Their count

 public:   
    Bitmap*  bitmap_in;
    Bitmap*  bitmap_out;

 public:
    inline sstream_t(): snap_t<T>() {
        bitmap_in = 0;
        bitmap_out = 0;
        new_edges = 0;
        new_edge_count = 0;
    }
    inline ~sstream_t() {
        if (bitmap_in != bitmap_out) {
            delete bitmap_in;
            bitmap_in = 0;
        }
        if(bitmap_out != 0) {
            delete bitmap_out;
            bitmap_out = 0;
        }
    }
    
    inline void init_view(pgraph_t<T>* pgraph, index_t a_flag) 
    {
        snap_t<T>::init_view(pgraph, a_flag);
        
        bitmap_out = new Bitmap(v_count);
        if (graph_out == graph_in) {
            bitmap_in = bitmap_out;
        } else if (graph_in !=0){
            bitmap_in = new Bitmap(v_count);
        }
    }

    status_t    update_view();
 protected:
    void update_degreesnap();
    void update_degreesnapd();
    status_t update_view_help(snapshot_t* new_snapshot);
    void handle_flagu(sdegree_t* degree, Bitmap* bitmap);
    void handle_flagd(sdegree_t* out_degree, sdegree_t* in_degree);
    void handle_flaguni(sdegree_t* degree, Bitmap* bitmap);
    void handle_e_centric(index_t snap_marker, index_t old_marker);
 public:
    //These two functions are for vertex centric programming
    inline bool has_vertex_changed_out(vid_t v) {return bitmap_out->get_bit(v);}
    inline bool has_vertex_changed_in(vid_t v) {return bitmap_in->get_bit(v);}
    
    //This function is for edge centric programming. Memory management is our headache
    inline index_t get_new_edges(edgeT_t<T>*& changed_edges) {changed_edges = new_edges; return new_edge_count;}
    //inline index_t get_new_edges_length() {return 0;}

    
    inline void set_vertex_changed_out(vid_t v) {bitmap_out->set_bit_atomic(v);;}
    inline void reset_vertex_changed_out(vid_t v) {bitmap_out->reset_bit(v);;}
    inline void set_vertex_changed_in(vid_t v) {bitmap_in->set_bit_atomic(v);;}
    inline void reset_vertex_changed_in(vid_t v) {bitmap_in->reset_bit(v);;}
};

template <class T>
void sstream_t<T>::update_degreesnap()
{
    sdegree_t nebr_count = 0;
    snapid_t snap_id = 0;
    if (snapshot) {
        snap_id = snapshot->snap_id;

        #pragma omp for 
        for (vid_t v = 0; v < v_count; ++v) {
            nebr_count = graph_out->get_degree(v, snap_id);
            if (degree_out[v] != nebr_count) {
                degree_out[v] = nebr_count;
                bitmap_out->set_bit(v);
            } else {
                bitmap_out->reset_bit(v);
            }
            //cout << v << " " << degree_out[v] << endl;
        }
    }
}

template <class T>
void sstream_t<T>::update_degreesnapd()
{
    sdegree_t      nebr_count = 0;
    snapid_t snap_id = 0;
    if (snapshot) {
        snap_id = snapshot->snap_id;
        #pragma omp for nowait 
        for (vid_t v = 0; v < v_count; ++v) {
            nebr_count = graph_out->get_degree(v, snap_id);
            if (degree_out[v] != nebr_count) {
                degree_out[v] = nebr_count;
                bitmap_out->set_bit(v);
            } else {
                bitmap_out->reset_bit(v);
            }
        
            nebr_count = graph_in->get_degree(v, snap_id);;
            if (degree_in[v] != nebr_count) {
                degree_in[v] = nebr_count;
                bitmap_in->set_bit(v);
            } else {
                bitmap_in->reset_bit(v);
            }
        }
    }

    return;
}

template <class T>
void sstream_t<T>::handle_e_centric(index_t snap_marker, index_t old_marker)
{
    //Get the edge copies for edge centric computation XXX
    if (IS_E_CENTRIC(flag)) { 
        assert(0);//FIX ME
        /*
        new_edge_count = snap_marker - old_marker;
        new_edges = (edgeT_t<T>*)realloc (new_edges, new_edge_count*sizeof(edgeT_t<T>));
        memcpy(new_edges, blog->blog_beg + (old_marker & blog->blog_mask), new_edge_count*sizeof(edgeT_t<T>));
        */
    }
}

template <class T>
status_t sstream_t<T>::update_view()
{
    snapshot_t* new_snapshot = pgraph->get_snapshot();
    if (new_snapshot == 0|| (new_snapshot == prev_snapshot)) return eNoWork;
    
    return update_view_help(new_snapshot);

}

template <class T>
status_t sstream_t<T>::update_view_help(snapshot_t* new_snapshot)
{
    blog_t<T>* blog = pgraph->blog;
    index_t  marker = blog->blog_head;

    snapshot = new_snapshot;

    if (snapshot == 0|| (snapshot == prev_snapshot)) return eNoWork;
    index_t old_marker = prev_snapshot? prev_snapshot->marker: 0;
    index_t snap_marker = snapshot->marker;
    
    
    //for stale
    this->handle_visibility(marker, snap_marker);
    
    #pragma omp parallel num_threads(THD_COUNT)
    {
    if (graph_in == graph_out || graph_in == 0) {
        update_degreesnap();
    } else {
        update_degreesnapd();
    }
    if (graph_in != graph_out && graph_in != 0) {
        handle_flagd(degree_out, degree_in);
    } else if (graph_in == graph_out) {
        handle_flagu(degree_out, bitmap_out);
    } else {
        handle_flaguni(degree_out, bitmap_out);
    }
    }
    if (prev_snapshot) {
        prev_snapshot->drop_ref();
    }
    prev_snapshot = snapshot;
    
    //wait for archiving to complete
    if (IS_SIMPLE(flag)) {
        pgraph->waitfor_archive(reader.marker);
        reader.tail = -1;
        reader.marker = -1;
    }
    
    return eOK;
}

template <class T>
void sstream_t<T>::handle_flagu(sdegree_t* degree, Bitmap* bitmap)
{
    sid_t src, dst;
    edgeT_t<T> edge;
    vid_t src_vid, dst_vid;
    bool is_del = false;
    bool is_simple = IS_SIMPLE(flag);
    if (true != is_simple) {
        return;
    }
    for (index_t i = reader.tail; i < reader.marker; ++i) {
        read_edge(reader.blog, i, edge);
        src_vid = TO_VID(get_src(edge));
        dst_vid = TO_VID(get_dst(edge));
        //set the bitmap
        bitmap->set_bit_atomic(src_vid);
        bitmap->set_bit_atomic(dst_vid);

        is_del = IS_DEL(get_src(edge));
#ifdef DEL
        if (is_del) {
        __sync_fetch_and_add(&degree[src_vid].del_count, 1);
        __sync_fetch_and_add(&degree[dst_vid].del_count, 1);
        } else {
        __sync_fetch_and_add(&degree[src_vid].add_count, 1);
        __sync_fetch_and_add(&degree[dst_vid].add_count, 1);
        }
#else
        if (is_del) {assert(0);
        __sync_fetch_and_add(&degree+src_vid, 1);
        __sync_fetch_and_add(&degree+dst_vid, 1);
        }
#endif
    }
}

template <class T>
void sstream_t<T>::handle_flagd(sdegree_t* out_degree, sdegree_t* in_degree)
{
    vid_t src_vid, dst_vid;
    edgeT_t<T> edge;
    bool is_del = false;
    bool is_private = IS_PRIVATE(flag);
    bool is_simple = IS_SIMPLE(flag);
    if (true != is_simple) {
        return;
    }
    for (index_t i = reader.tail; i < reader.marker; ++i) {
        read_edge(reader.blog, i, edge);
        src_vid = TO_VID(get_src(edge));
        dst_vid = TO_VID(get_dst(edge));
        bitmap_out->set_bit_atomic(src_vid);
        bitmap_in->set_bit_atomic(dst_vid);
        is_del = IS_DEL(get_src(edge));
#ifdef DEL
        if (is_del) {
        __sync_fetch_and_add(&out_degree[src_vid].del_count, 1);
        __sync_fetch_and_add(&in_degree[dst_vid].del_count, 1);
        } else {
        __sync_fetch_and_add(&out_degree[src_vid].add_count, 1);
        __sync_fetch_and_add(&in_degree[dst_vid].add_count, 1);
        }
#else
        if (is_del) {assert(0);
        __sync_fetch_and_add(&out_degree+src_vid, 1);
        __sync_fetch_and_add(&in_degree+dst_vid, 1);
        }
#endif
    }
}

template <class T>
void sstream_t<T>::handle_flaguni(sdegree_t* degree, Bitmap* bitmap)
{
    vid_t src_vid, dst_vid;
    edgeT_t<T> edge;
    bool is_del = false;
    bool is_simple = IS_SIMPLE(flag);
    if (false != is_simple) {
        return;
    }
    #pragma omp for
    for (index_t i = reader.tail; i < reader.marker; ++i) {
        read_edge(reader.blog, i, edge);
        src_vid = TO_VID(get_src(edge));
        dst_vid = TO_VID(get_dst(edge));
        bitmap->set_bit_atomic(src_vid);
        is_del = IS_DEL(get_src(edge));
#ifdef DEL
        if (is_del) {
        __sync_fetch_and_add(&degree[src_vid].del_count, 1);
        } else {
        __sync_fetch_and_add(&degree[src_vid].add_count, 1);
        }
#else
        if (is_del) {assert(0);
        __sync_fetch_and_add(&degree+src_vid, 1);
        }
#endif
    }
}
