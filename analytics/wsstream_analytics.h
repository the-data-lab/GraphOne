#pragma once
#include <omp.h>
#include "graph_view.h"

template <class T>
void wsstream_example(gview_t<T>* view) 
{
    wsstream_t<T>* viewh = dynamic_cast<wsstream_t<T>*>(view);
    vid_t v_count = viewh->get_vcount();
    index_t window_sz = viewh->window_sz;
    int update_count = 0;
    index_t edge_count = 0;
    degree_t degree;
    index_t check_sz = window_sz;
    if (viewh->is_udir()) check_sz += window_sz;
    
    degree_t sz = 1024;
    T* adj_list = (T*)malloc(sz*sizeof(T));
    assert(adj_list);

    //sleep(1);
    while (viewh->get_snapmarker() < _edge_count) {
        if (eOK != viewh->update_view()) {
            usleep(10);
            continue;
        }
        ++update_count;
        edge_count = 0;
        
        for (vid_t v = 0; v < v_count; ++v) {
            degree = viewh->get_degree_out(v);
            if (degree == 0) continue;
            assert (degree < check_sz);
            edge_count += degree;
            if (degree > sz) {
                //assert(0);
                sz = degree;
                adj_list = (T*)realloc(adj_list, sz*sizeof(T));
                assert(adj_list);
            }
            //viewh->get_nebrs_out(v, adj_list);
        }
        assert(edge_count == check_sz);
        //cout << check_sz << ":" << edge_count << endl;
    }

    cout << "wsstream: update_count " << update_count << ":" << viewh->get_snapid() << endl;//edge_count << endl;
}

template<class T>
void log_func(blog_t<T>* blog, int wtf)
{
    index_t snap_marker;
    index_t count;
    edgeT_t<T> edge;
    
    //
    snap_marker = blog->blog_head;
    blog->blog_marker = snap_marker;
    count = snap_marker - blog->blog_tail;
    /*
    for (index_t i = blog->blog_tail; i < snap_marker; ++i) {
        read_edge(blog, i, edge); 
    }*/

    //write to a file
    index_t actual_tail = blog->blog_tail & blog->blog_mask;
    index_t actual_marker = blog->blog_marker & blog->blog_mask;
    if (actual_tail < actual_marker) {
        //write and update tail
        //fwrite(blog->blog_beg + w_tail, sizeof(edgeT_t<T>), w_count, wtf);
        write(wtf, blog->blog_beg + actual_tail, sizeof(edgeT_t<T>)*count);
    }
    else {
        write(wtf, blog->blog_beg + actual_tail, sizeof(edgeT_t<T>)*(blog->blog_count - actual_tail));
        write(wtf, blog->blog_beg, sizeof(edgeT_t<T>)*actual_marker);
    }

    blog->update_marker();
}

template <class T>
void wsstream_dup(gview_t<T>* view) 
{
    wsstream_t<T>* viewh = dynamic_cast<wsstream_t<T>*>(view);
    vid_t v_count = viewh->get_vcount();
    
    blog_t<T> blog;
    blog.alloc_edgelog(BLOG_SHIFT);
    
    int update_count = 0;
    int wtf =  open ("/home/pkumar/src/GraphOne/build/del.bin", O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
    
    //sleep(1);
    while (viewh->get_snapmarker() < _edge_count) {
        if (eOK != viewh->update_view_tumble()) {
            usleep(10);
            continue;
        }
        #pragma omp parallel num_threads(THD_COUNT) 
        {
            degree_t sz = 1024;
            T* adj_list = (T*)malloc(sz*sizeof(T));
            assert(adj_list);
            
            vid_t v, sid; 
            degree_t degree;
            edgeT_t<T> edge;
           
            #pragma omp for
            for (vid_t v = 0; v< v_count; ++v) {
                degree = viewh->get_degree_out(v);
                if (degree == 0) continue;
                if (degree > sz) {
                    sz = degree;
                    adj_list = (T*)realloc(adj_list, sz*sizeof(T));
                    assert(adj_list);
                }
                viewh->get_nebrs_out(v, adj_list);
                for (degree_t j = 0; j < degree; ++j) {
                    set_src(edge, v);
                    edge.dst_id = adj_list[j];
                    blog.batch_edge(edge);
                }
            }
        }

        //Lets write the log
        log_func(&blog, wtf);
        
        ++update_count;
    }
    close(wtf);

    cout << "wsstream: update_count " << update_count << ":" << viewh->get_snapid() << endl;//edge_count << endl;
}
