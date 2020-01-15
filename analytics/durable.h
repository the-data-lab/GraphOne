#pragma once
#include <omp.h>
#include <algorithm>

#include "type.h"
#include "graph.h"
#include "sgraph.h"
#include "wtime.h"

template <class T>
void write_edge(stream_t<T>* streamh) {
    index_t marker = streamh->reader.marker;
    index_t tail = streamh->reader.tail;
    index_t count = marker - tail;
    if (w_count == 0) return eNoWork;
    int wtf = streamh->pgraph->wtf;
        
    for (index_t i = tail; i < marker; ++i) {
        read_edge(streamh->reader.blog, i, edge);
    }

    index_t actual_tail = tail & reader.blog->blog_mask;
    index_t actual_marker = marker & reader.blog->blog_mask;
    if (actual_tail < actual_marker) {
        //write and update tail
        //fwrite(blog->blog_beg + w_tail, sizeof(edgeT_t<T>), w_count, wtf);
        write(wtf, reader.blog->blog_beg + actual_tail, sizeof(edgeT_t<T>)*count);
    }
    else {
        write(wtf, reader.blog->blog_beg + actual_tail, sizeof(edgeT_t<T>)*(blog->blog_count - actual_tail));
        write(wtf, reader.blog->blog_beg, sizeof(edgeT_t<T>)*actual_marker);
    }

    //Write the string weights if any
    streamh->pgraph->mem.handle_write();
    streamh->update_view_done();
}

template <class T>
void durable_edge(gview_t<T>* viewh)
{
    stream_t<T>* streamh = (stream_t<T>*)viewh;
    edgeT_t<T> edge;
    
    while (streamh->get_snapmarker() < _edge_count) {
        while(eOK != streamh->update_view()) usleep(100);
        write_edge(streamh);
    }
    write_edge(streamh);

}
