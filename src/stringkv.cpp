#include "graph.h"
#include "stringkv.h"


cfinfo_t* stringkv_t::create_instance()
{
    return new stringkv_t;
}

status_t stringkv_t::batch_edge(edgeT_t<char*>& edge)
{
    vid_t vid = TO_VID(edge.src_id);
    tid_t tid = TO_TID(edge.src_id);
    strkv_out[tid]->set_value(vid, edge.dst_id);
    return eOK;
}

status_t stringkv_t::batch_update(const string& src, const string& dst, propid_t pid /* = 0*/)
{
    edgeT_t<char*> edge; 
    edge.src_id = g->get_sid(src.c_str());
    edge.dst_id = const_cast<char*>(dst.c_str());
    batch_edge(edge);
    return eOK;
}

void stringkv_t::make_graph_baseline()
{
}

void stringkv_t::prep_graph_baseline()
{
    sflag_t    flag = flag1;
    tid_t   t_count = g->get_total_types();
    vid_t   max_vcount;    
    if (0 == strkv_out) {
        strkv_out = (strkv_t**) calloc (sizeof(strkv_t*), t_count);
    }

    if (flag == 0) {
        flag1_count = g->get_total_types();
        for(tid_t i = 0; i < flag1_count; i++) {
            if (0 == strkv_out[i]) {
                max_vcount = g->get_type_scount(i);
                strkv_out[i] = new strkv_t;
                strkv_out[i]->setup(i, max_vcount);
            }
        }
        return;
    } 
    
    tid_t      pos  = 0;
    flag1_count = __builtin_popcountll(flag);
    for(tid_t i = 0; i < flag1_count; i++) {
        pos = __builtin_ctzll(flag);
        flag ^= (1L << pos);//reset that position
        if (0 == strkv_out[pos]) {
            max_vcount = g->get_type_scount(pos);
            strkv_out[pos] = new strkv_t;
            strkv_out[pos]->setup(pos, max_vcount);
        }
    }
}

void stringkv_t::file_open(const string& dir, bool trunc)
{
    if (strkv_out == 0) return;
    
    tid_t       t_count = g->get_total_types();
    
    //base name using relationship type
    string basefile;
    if (col_count) {
        basefile = dir + col_info[0]->p_name;
    } else {
        basefile = dir;
    }

    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        if (strkv_out[i] == 0) continue;
        strkv_out[i]->file_open(basefile, trunc);
    }
}

void stringkv_t::store_graph_baseline(bool clean /*=false*/)
{
    if (strkv_out == 0) return;
    
    tid_t       t_count = g->get_total_types();
    
    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        if (strkv_out[i] == 0) continue;

        strkv_out[i]->handle_write();
    }
}

void stringkv_t::read_graph_baseline()
{
    tid_t   t_count = g->get_total_types();
    if (0 == strkv_out) {
        strkv_out  = (strkv_t**) calloc (sizeof(strkv_t*), t_count);
    }
    
    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        if (strkv_out[i] == 0) continue;
        strkv_out[i]->read_vtable();
    }
}
