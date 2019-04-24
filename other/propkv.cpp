#include "graph.h"
#include "propkv.h"

void labelkv_t::add_edge_property(const char* longname, prop_encoder_t* prop_encoder)
{
    encoder = prop_encoder;
}

cfinfo_t* labelkv_t::create_instance()
{
    return new labelkv_t;
}

status_t labelkv_t::batch_update(const string& src, const string& dst, propid_t pid /* = 0*/)
{
    vid_t src_id;
    index_t index = 0;
    lite_edge_t* edges;
    
    if (batch_info1[batch_count1].count == MAX_ECOUNT) {
        void* mem = alloc_buf();
        if (mem == 0) return eEndBatch;
        ++batch_count1;
        batch_info1[batch_count1].count = 0;
        batch_info1[batch_count1].buf = mem; 
    }

    src_id = g->get_sid(src.c_str());
    tid_t type_id = TO_TID(src_id);
    flag1 |= (1L << type_id);
    
    index = batch_info1[batch_count1].count++;
    edges = (lite_edge_t*) batch_info1[batch_count1].buf;
    edges[index].first = src_id; 
    encoder->encode(dst.c_str(), edges[index].second);
    return eOK;
}

void labelkv_t::make_graph_baseline()
{
    if (batch_info[batch_count].count == 0) return;

    flag1_count = __builtin_popcountll(flag1);
    
    //super bins memory allocation
    prep_propkv();

    //populate and get the original count back
    //handle kv_out as well.
    fill_kv_out();

    cleanup();
}

propkv_t** labelkv_t::prep_propkv()
{
    sflag_t    flag = flag1;
    tid_t      pos  = 0;
    tid_t   t_count = g->get_total_types();
    
    if (0 == kv_out) {
        kv_out = (propkv_t**) calloc (sizeof(propkv_t*), t_count);
    }

    for(tid_t i = 0; i < flag1_count; i++) {
        pos = __builtin_ctz(flag);
        flag ^= (1L << pos);//reset that position
        if (0 == kv_out[pos]) {
            kv_out[pos] = new propkv_t;
            kv_out[pos]->setup(pos);
        }
    }
    return kv_out;
}

void labelkv_t::fill_kv_out()
{
    sid_t src;
    univ_t dst;
    vid_t     vert1_id;
    tid_t     src_index;
    lite_edge_t*   edges;
    index_t   count;

    for (int j = 0; j <= batch_count; ++j) { 
        edges = (lite_edge_t*)batch_info[j].buf;
        count = batch_info[j].count;
    
        for (index_t i = 0; i < count; ++i) {
            src = edges[i].first;
            dst = edges[i].second;
            
            vert1_id = TO_VID(src);
            src_index = TO_TID(src);
            kv_out[src_index]->set_value(vert1_id, dst);
        }
    }
}

void labelkv_t::store_graph_baseline(string dir)
{
    if (kv_out == 0) return;
    
    string postfix = "out";

    char name[8];
    tid_t       t_count = g->get_total_types();
    
    //base name using relationship type
    string basefile;
    if (col_count) {
        basefile = dir + col_info[0]->p_name;
    } else {
        basefile = dir;
    }
    string vtfile, etfile;

    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        if (kv_out[i] == 0) continue;
        sprintf(name, "%d.", i);
        vtfile = basefile + name + "vtable" + postfix;

        kv_out[i]->persist_kvlog(vtfile);
    }
}

void labelkv_t::read_graph_baseline(const string& dir)
{
    tid_t   t_count = g->get_total_types();
    if (0 == kv_out) {
        kv_out  = (propkv_t**) calloc (sizeof(propkv_t*), t_count);
    }
    
    string postfix = "out";

    char name[8];
    
    //base name using relationship type
    string basefile;
    if (col_count) {
        basefile = dir + col_info[0]->p_name;
    } else {
        basefile = dir;
    }
    string vtfile;

    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        sprintf(name, "%d.", i);
        vtfile = basefile + name + "vtable" + postfix;

        FILE* vtf = fopen(vtfile.c_str(), "r+b");
        if (vtf == 0) continue;
        fclose(vtf);

        kv_out[i] = new propkv_t;
        kv_out[i]->setup(i);
        kv_out[i]->read_vtable(vtfile);
    }
}

////////////////////////////////////////////////////////////////

propkv_t::propkv_t()
{
    kv = 0;
    super_id = 0;
    max_vcount = 0;
    
    dvt_count = 0;
    dvt_max_count = (1L << 9);
    if (posix_memalign((void**) &dvt, 2097152, 
                       dvt_max_count*sizeof(disk_propkv_t*))) {
        perror("posix memalign vertex log");    
    }
    vtf = 0;
}

void propkv_t::set_value(vid_t vid, univ_t value)
{
    kv[vid] = value;
    dvt[dvt_count].vid = vid;
    dvt[dvt_count].univ = value; 
    ++dvt_count;
}

void propkv_t::setup(tid_t tid) 
{
    if ( 0 == super_id ) {
        super_id = g->get_type_scount(tid);
        vid_t v_count = TO_VID(super_id);
        max_vcount = (v_count << 1);
        kv = (univ_t*)calloc(sizeof(univ_t), max_vcount);
    } else {
        super_id = g->get_type_scount(tid);
        vid_t v_count = TO_VID(super_id);
        if (max_vcount < v_count) {
            assert(0);
        }
    }
}

void propkv_t::persist_kvlog(const string& vtfile)
{
    //Make a copy
    sid_t count =  dvt_count;

    //update the mark
    dvt_count = 0;

    //Write the file
    if(vtf == 0) {
        vtf = fopen(vtfile.c_str(), "a+b");
        assert(vtf != 0);
    }
    fwrite(dvt, sizeof(disk_propkv_t), count, vtf);
}

void propkv_t::read_vtable(const string& vtfile)
{
    //Write the file
    if(vtf == 0) {
        vtf = fopen(vtfile.c_str(), "r+b");
        assert(vtf != 0);
    }

    off_t size = fsize(vtfile.c_str());
    if (size == -1L) {
        assert(0);
    }
    vid_t count = (size/sizeof(disk_propkv_t));

    //read in batches
    while (count !=0) {
        vid_t read_count = fread(dvt, sizeof(disk_propkv_t), dvt_max_count, vtf);
        for (vid_t v = 0; v < read_count; ++v) {
            kv[dvt[v].vid] = dvt[v].univ;
        }
        count -= read_count;
    }
    dvt_count = 0;
}
