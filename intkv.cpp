#include "graph.h"

typedef struct __label_int64_t {
    vid_t src_id;
    int64_t dst_id;
} label_int64_t;

status_t int64kv_t::batch_update(const string& src, const string& dst, propid_t pid /* = 0*/)
{
    vid_t src_id;
    uint64_t dst_id;
    index_t index = 0;
    label_int64_t* edges;
    
    if (batch_info1[batch_count1].count == MAX_ECOUNT) {
        void* mem = alloc_buf();
        if (mem == 0) return eEndBatch;
        ++batch_count1;
        batch_info1[batch_count1].count = 0;
        batch_info1[batch_count1].buf = mem; 
    }
    edges = (edge_t*) batch_info1[batch_count1].buf;
   
   // map<string, vid_t>::iterator str2vid_iter = str2vid.find(src);
   // if (str2vid.end() == str2vid_iter) {
   //     src_id = vert_count++;
   //     str2vid[src] = src_id;
   // } else {
   //     src_id = str2vid_iter->second;
   // }
   
	if ((src_id = str2vid.find(src)) == UINT64_MAX){
		src_id = vert_count++;
		str2vid.insert(src, src_id);
	}

    sscanf(dst.c_str(), "%ld", &dst_id);
    index = count++;
    edges[index].src_id = src_id; 
    edges[index].dst_id = dst_id;
    return eOk;
}
    
void int64kv_t::make_graph_baseline()
{
    if (batch_info[0].count == 0) return;
    flag1_count = __builtin_popcountll(flag1);
    
    //super bins memory allocation
    prep_lkv();
    
    fill_kv_out();
    
    //clean up
    cleanup();

}

    
void int64kv_t::store_graph_baseline(string dir)
{
    if (count == 0) return;

    string file = dir + p_name + ".kv_out";
    FILE* f = fopen(file.c_str(), "wb");
    assert(f != 0);
    fwrite(kv_out, sizeof(int64_t), vert_count, f);
    fclose(f);
}

typedef struct __label_int8_t {
    vid_t src_id;
    uint8_t dst_id;
} label_int8_t;

void int8kv_t::batch_update(const string& src, const string& dst, propid_t pid /* = 0*/)
{
    vid_t src_id;
    uint8_t dst_id;
    index_t index = 0;
    label_int8_t* edges = (label_int8_t*) batch_info[batch_count].buf;
    //map<string, vid_t>::iterator str2vid_iter = str2vid.find(src);
    //if (str2vid.end() == str2vid_iter) {
    //    src_id = vert_count++;
    //    str2vid[src] = src_id;
    //} else {
    //    src_id = str2vid_iter->second;
    //}
	if ((src_id = str2vid.find(src)) == UINT64_MAX){
		src_id = vert_count++;
		str2vid.insert(src, src_id);
	}
    
    sscanf(dst.c_str(), "%d", &dst_id);
    index = count++;
    edges[index].src_id = src_id; 
    edges[index].dst_id = dst_id;
}
    
void int8kv_t::make_graph_baseline()
{
    vid_t src;
    uint8_t dst;
    label_int8_t* edges = (label_int8_t*) batch_info[batch_count].buf;
    kv_out = (uint8_t*)calloc(sizeof(uint8_t), vert_count);

    //populate
    for (index_t i = 0; i < batch_info[batch_count].count; ++i) {
        src = edges[i].src_id;
        dst = edges[i].dst_id;
        kv_out[src] = dst;
    }
}

void int8kv_t::store_graph_baseline(string dir)
{
    if (count == 0) return;

    string file = dir + p_name + ".kv_out";
    FILE* f = fopen(file.c_str(), "wb");
    assert(f != 0);
    fwrite(kv_out, sizeof(uint8_t), vert_count, f);
    fclose(f);
}
