#include "graph.h"
#include "mlabel.h"

mkv_t::mkv_t()
{
    super_id = 0;
    kv_array = 0;
    nebr_count = 0;
    max_vcount = 0;
    
    //XXX everything is in memory
    log_count = (1L << 9);//32*8 MB
    if (posix_memalign((void**)&log_beg, 2097152, log_count*sizeof(char))) {
        //log_beg = (sid_t*)calloc(sizeof(sid_t), log_count);
        perror("posix memalign edge log");
    }
    log_head = 0;
    log_tail = 0;
    log_wpos = 0;
    
    dvt_count = 0;
    dvt_max_count = (1L << 9);
    if (posix_memalign((void**) &dvt, 2097152, 
                       dvt_max_count*sizeof(disk_manykv_t*))) {
        perror("posix memalign vertex log");    
    }
    vtf = 0;
    etf = 0;
}

void mkv_t::setup(tid_t tid)
{
    if ( 0 == super_id ) {
        super_id = g->get_type_scount(tid);
        vid_t v_count = TO_VID(super_id);
        max_vcount = (v_count << 1);
        kv_array = (kvarray_t*)calloc(sizeof(kvarray_t), max_vcount);
        nebr_count = (kv_t*)calloc(sizeof(kv_t), max_vcount);
    } else {
        super_id = g->get_type_scount(tid);
        vid_t v_count = TO_VID(super_id);
        if (max_vcount < v_count) {
            assert(0);
        }
    }
}

void mkv_t::setup_adjlist()
{
    vid_t v_count = TO_VID(super_id);
    propid_t size = 0;
    propid_t additional_size = sizeof(propid_t) + sizeof(propid_t);//degree and size
    propid_t degree = 0;
    propid_t* adj_list = 0;
    vid_t v = 0;


    for (vid_t vid = 0; vid < v_count; ++vid) {
        adj_list = kv_array[vid].adj_list;
        //XXX align it
        size = (nebr_count[vid].offset + 1) & 0xFFFE;
        degree = nebr_count[vid].pid;

        if (adj_list && adj_list[1] != degree ) {
            adj_list = (propid_t*)(log_beg + log_head);

            //key, offset copying
            propid_t copy_sz = kv_array[vid].adj_list[1]*sizeof(kv_t) + additional_size;
            memcpy(adj_list, kv_array[vid].adj_list, copy_sz);
            
            //value copying
            propid_t rem_sz = kv_array[vid].adj_list[0] - copy_sz;
            propid_t new_offset = degree*sizeof(kv_t) + additional_size;
            memcpy(adj_list + new_offset, 
                   kv_array[vid].adj_list + copy_sz, 
                   rem_sz);

            kv_array[v].adj_list = adj_list;
        
            //values will be allocated from this place.
            nebr_count[vid].offset = new_offset + rem_sz;
            //reset the count
            nebr_count[vid].pid = adj_list[1];
            
            dvt[v].vid = vid;
            dvt[v].degree = degree;
            dvt[v].size = size;
            dvt[v].file_offset = log_head;
            
            log_head += size + dvt[v].size;
            ++v;

        } else if (!adj_list) {
            adj_list = (propid_t*)(log_beg + log_head);
            
            //Doesn't matter
            adj_list[0] = additional_size;
            adj_list[1] = 0;//degree, key count

            kv_array[vid].adj_list = adj_list;

            //values will be allocated from this place.
            nebr_count[vid].offset = additional_size + degree*sizeof(kv_t);
            //reset the count
            nebr_count[vid].pid = 0;

            dvt[v].vid = vid;
            dvt[v].degree = degree;
            dvt[v].size = size + additional_size;//additional size should be added only once
            dvt[v].file_offset = log_head;
            
            log_head += size + additional_size;
            ++v;
        }
    }
    dvt_count = v;
}
    
void mkv_t::add_nebr(vid_t vid, propid_t pid, char* dst) 
{
    propid_t* adj_list = kv_array[vid].adj_list;
    //First two members are size and degree;
    kv_t* kv = (kv_t*)(kv_array[vid].adj_list + 2);
    propid_t value_size = strlen(dst) + 1;

    kv[nebr_count[vid].pid].pid = pid;
    kv[nebr_count[vid].pid].offset = nebr_count[vid].offset;
    memcpy((char*)adj_list + nebr_count[vid].offset, dst, value_size);
    
    nebr_count[vid].pid += 1;
    nebr_count[vid].offset += value_size;
}


void mkv_t::persist_elog(const string& etfile)
{
    //Make a copy
    sid_t wpos = log_wpos;
    
    //Update the mark
    log_wpos = log_head;
        
    //Write the file.
    if (etf == 0) {
        etf = fopen(etfile.c_str(), "a+b");//append/write + binary
        assert(etf != 0);
    }
    fwrite(log_beg+wpos, sizeof(char), log_head-wpos, etf);
}

void mkv_t::persist_vlog(const string& vtfile)
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
    fwrite(dvt, sizeof(disk_manykv_t), count, vtf);
}

void mkv_t::read_etable(const string& etfile)
{
    if (etf == 0) {
        etf = fopen(etfile.c_str(), "r+b");//append/write + binary
        assert(etf != 0);
    }

    off_t size = fsize(etfile.c_str());
    if (size == -1L) {
        assert(0);
    }
    sid_t edge_count = size;
    fread(log_beg, sizeof(char), edge_count, etf);

    log_head = edge_count;
    log_wpos = log_head;
}

void mkv_t::read_vtable(const string& vtfile)
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
    vid_t count = (size/sizeof(disk_manykv_t));

    //read in batches
    while (count !=0 ) {
        vid_t read_count = fread(dvt, sizeof(disk_manykv_t), dvt_max_count, vtf);
        for (vid_t v = 0; v < read_count; ++v) {
            nebr_count[dvt[v].vid].pid = dvt[v].degree;
            nebr_count[dvt[v].vid].offset = dvt[v].file_offset;


            kv_array[dvt[v].vid].adj_list = (propid_t*) (log_beg + dvt[v].file_offset);
        }
        count -= read_count;
    }
    dvt_count = 0;
}

void mkv_t::print_raw_dst(vid_t vid, propid_t pid) 
{
    kv_t*  kv = (kv_t*)(kv_array[vid].adj_list + 2);
    propid_t count = kv_array[vid].get_nebrcount();
    
    char* adj_list = (char*)(kv_array[vid].adj_list);
    for (propid_t i = 0; i <= count; ++i) {
        if (pid == kv[i].pid) {
            cout << adj_list + kv[i].offset;
            break;
        }
    }
}

/*****************/
status_t manykv_t::batch_update(const string& src, const string& dst, propid_t pid /*=0*/)
{
    sid_t src_id;
    index_t index = 0;
    pedge_t* edges;
    char* dst_id;
    
    if (batch_info1[batch_count1].count == MAX_PECOUNT) {
        void* mem = alloc_buf();
        if (mem == 0) return eEndBatch;
        ++batch_count1;
        batch_info1[batch_count1].count = 0;
        batch_info1[batch_count1].buf = mem; 
    }
    src_id = g->get_sid(src.c_str());
    tid_t type_id = TO_TID(src_id);
    flag1 |= (1L << type_id);
    
    dst_id = gstrdup(dst.c_str());
    index = batch_info1[batch_count1].count++;
    edges = (pedge_t*) batch_info1[batch_count1].buf;
    edges[index].pid = pid;
    edges[index].src_id = src_id; 
    edges[index].dst_id.value_charp = dst_id;//gstrdup(dst.c_str);
    return eOK;
}

void manykv_t::make_graph_baseline()
{
    if (batch_info[0].count == 0) return;
    flag1_count = __builtin_popcountll(flag1);

    //super bins memory allocation
    prep_mkv();
    
    //estimate edge count
    calc_edge_count();
    
    //prefix sum then reset the count
    prep_mkv_internal();

    //populate and get the original count back
    fill_mkv_out();
    update_count();
    
    //clean up
    cleanup();
}

void manykv_t::prep_mkv()
{
    sflag_t    flag = flag1;
    tid_t      pos  = 0;
    tid_t   t_count = g->get_total_types();
    
    if (0 == mkv_out) {
        mkv_out = (mkv_t**) calloc (sizeof(mkv_t*), t_count);
    }

    for(tid_t i = 0; i < flag1_count; i++) {
        pos = __builtin_ctz(flag);
        flag ^= (1L << pos);//reset that position
        if (0 == mkv_out[pos]) {
            mkv_out[pos] = new mkv_t;
        }
        mkv_out[pos]->setup(pos);
    }
}

void manykv_t::calc_edge_count()
{
    sid_t     src;
    vid_t     vert1_id;
    tid_t     src_index;
    pedge_t*  edges;
    index_t   count;

    for (int j = 0; j <= batch_count; ++j) { 
        edges = (pedge_t*)batch_info[j].buf;
        count = batch_info[j].count;
        for (index_t i = 0; i < count; ++i) {
            src = edges[i].src_id;
            src_index = TO_TID(src);
            vert1_id = TO_VID(src);
            mkv_out[src_index]->increment_count(vert1_id, strlen(edges[i].dst_id.value_charp) + 1);
        }
    }
}

void manykv_t::prep_mkv_internal()
{
    tid_t       t_count = g->get_total_types();
    
    for(tid_t i = 0; i < t_count; i++) {
        if (0 == mkv_out[i]) continue;
        mkv_out[i]->setup_adjlist();
    }
}

void manykv_t::fill_mkv_out()
{
    sid_t src;
    char* dst;
    propid_t pid;
    vid_t     vert1_id;
    tid_t     src_index;
    
    pedge_t*  edges;
    index_t   count;

    for (int j = 0; j <= batch_count; ++j) { 
        edges = (pedge_t*)batch_info[j].buf;
        count = batch_info[j].count;
        for (index_t i = 0; i < count; ++i) {
            pid = edges[i].pid;
            src = edges[i].src_id;
            dst = edges[i].dst_id.value_charp;
            src_index = TO_TID(src);
            vert1_id = TO_VID(src);
            
            mkv_out[src_index]->add_nebr(vert1_id, pid, dst);
        }
    }
}

void manykv_t::update_count()
{
    vid_t       v_count = 0;
    tid_t       t_count = g->get_total_types();
    
    for(tid_t i = 0; i < t_count; i++) {
        if (0 == mkv_out[i]) continue;
        v_count = mkv_out[i]->get_vcount();
        for (vid_t j = 0; j < v_count; ++j) {
            mkv_out[i]->update_count(j);
        }
    }
}

void manykv_t::print_raw_dst(tid_t tid, vid_t vid, propid_t pid)
{
    mkv_out[tid]->print_raw_dst(vid, pid);
}

void manykv_t::store_graph_baseline(string dir)
{
    if (mkv_out == 0) return;
    
    string postfix = "out";

    //const char* name = 0;
    //typekv_t*   typekv = g->get_typekv();
    char name[8];
    tid_t       t_count = g->get_total_types();
    
    //base name using relationship type
    string basefile = dir + col_info[0]->p_name + "family";
    string vtfile, etfile;

    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        if (0 == mkv_out[i]) continue;
        //name = typekv->get_type_name(i);
        sprintf(name, "%d.", i);
        vtfile = basefile + name + "vtable" + postfix;
        etfile = basefile + name + "etable" + postfix;

        mkv_out[i]->persist_vlog(vtfile);
        mkv_out[i]->persist_elog(etfile);
    }
}
    
void manykv_t::read_graph_baseline( const string& dir)
{
    string  postfix = "out";
    tid_t   t_count = g->get_total_types();

    if (0 == mkv_out) {
        mkv_out = (mkv_t**) calloc (sizeof(mkv_t*), t_count);
    }

    //const char* name = 0;
    //typekv_t*   typekv = g->get_typekv();
    char name[8];
    
    //base name using relationship type
    string basefile = dir + col_info[0]->p_name + "family";
    string vtfile, etfile;

    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        sprintf(name, "%d.", i);
        vtfile = basefile + name + "vtable" + postfix;
        etfile = basefile + name + "etable" + postfix;
        
        FILE* vtf = fopen(vtfile.c_str(), "r+b");
        if (vtf == 0) continue;
        fclose(vtf);

        mkv_out[i] = new mkv_t;
        mkv_out[i]->setup(i);
        mkv_out[i]->read_vtable(vtfile);
        mkv_out[i]->read_etable(etfile);
    }
}

/*****************/
status_t edge_prop_t::batch_update(eid_t eid, const string& dst, propid_t pid /*=0*/)
{
    index_t  index = 0;
    pedge_t* edges;
    char*    dst_id;
    
    if (batch_info1[batch_count1].count == MAX_PECOUNT) {
        void* mem = alloc_buf();
        if (mem == 0) return eEndBatch;
        ++batch_count1;
        batch_info1[batch_count1].count = 0;
        batch_info1[batch_count1].buf = mem; 
    }

    dst_id = gstrdup(dst.c_str());
    index = batch_info1[batch_count1].count++;
    edges = (pedge_t*) batch_info1[batch_count1].buf;
    edges[index].pid = pid;
    edges[index].src_id = eid; 
    edges[index].dst_id.value_charp = dst_id;//gstrdup(dst.c_str);
    return eOK;
}

void edge_prop_t::make_graph_baseline()
{
    if (batch_info[0].count == 0) return;
    flag1_count = __builtin_popcountll(flag1);

    //super bins memory allocation
    prep_mkv();
    
    //estimate edge count
    calc_edge_count();
    
    //prefix sum then reset the count
    prep_mkv_internal();

    //populate and get the original count back
    fill_mkv_out();
    update_count();
    
    //clean up
    cleanup();
}

void edge_prop_t::prep_mkv()
{
    mkv_out.setup(0);
}

void edge_prop_t::calc_edge_count()
{
    eid_t     src;
    pedge_t*  edges;
    index_t   count;

    for (int j = 0; j <= batch_count; ++j) { 
        edges = (pedge_t*)batch_info[j].buf;
        count = batch_info[j].count;
        for (index_t i = 0; i < count; ++i) {
            src = edges[i].src_id;
            mkv_out.increment_count(src, strlen(edges[i].dst_id.value_charp) + 1);
        }
    }
}

void edge_prop_t::prep_mkv_internal()
{
    mkv_out.setup_adjlist();
}

void edge_prop_t::fill_mkv_out()
{
    sid_t src;
    char* dst;
    propid_t pid;
    
    pedge_t*  edges;
    index_t   count;

    for (int j = 0; j <= batch_count; ++j) { 
        edges = (pedge_t*)batch_info[j].buf;
        count = batch_info[j].count;
        for (index_t i = 0; i < count; ++i) {
            pid = edges[i].pid;
            src = edges[i].src_id;
            dst = edges[i].dst_id.value_charp;
            mkv_out.add_nebr(src, pid, dst);
        }
    }
}

void edge_prop_t::update_count()
{
    vid_t v_count = mkv_out.get_vcount();
    
    for (vid_t j = 0; j < v_count; ++j) {
        mkv_out.update_count(j);
    }
}

void edge_prop_t::print_raw_dst(tid_t tid, eid_t eid, propid_t pid)
{
    mkv_out.print_raw_dst(eid, pid);
}

void edge_prop_t::store_graph_baseline(string dir)
{
    string postfix = "out";

    //base name using relationship type
    string basefile = dir + col_info[0]->p_name + "family";
    string vtfile, etfile;

    vtfile = basefile +  ".vtable" + postfix;
    etfile = basefile +  ".etable" + postfix;

    mkv_out.persist_vlog(vtfile);
    mkv_out.persist_elog(etfile);
}
    
void edge_prop_t::read_graph_baseline( const string& dir)
{
    string  postfix = "out";

    //base name using relationship type
    string basefile = dir + col_info[0]->p_name + "family";
    string vtfile, etfile;

    vtfile = basefile + ".vtable" + postfix;
    etfile = basefile + ".etable" + postfix;
        
    FILE* vtf = fopen(vtfile.c_str(), "r+b");
    if (vtf == 0) assert(0);
    fclose(vtf);

    mkv_out.setup(0);
    mkv_out.read_vtable(vtfile);
    mkv_out.read_etable(etfile);
}
