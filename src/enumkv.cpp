#include "graph.h"
#include "enumkv.h"


status_t enumkv_t::batch_update(const string& src, const string& dst, propid_t pid /*= 0*/)
{
    edgeT_t<char*> edge; 
    edge.src_id = g->get_sid(src.c_str());
    edge.dst_id = const_cast<char*>(src.c_str());
    batch_edge(edge);
    return eOK;
}

status_t enumkv_t::batch_edge(edgeT_t<char*>& edge)
{
    sid_t       src_id = edge.src_id;
    string      dst = edge.dst_id;
    tid_t       tid = TO_TID(src_id);
    vid_t       vid = TO_VID(src_id);
    uint8_t     eid = 0;
    
    map<string, uint8_t>::iterator str2enum_iter = str2enum.find(dst);
    
    //Have not see this enum, create one
    if (str2enum.end() == str2enum_iter) {
        assert(eid <= max_ecount);
        eid = ecount++;
        str2enum[dst] = eid;
        enum2str[eid] = gstrdup(dst.c_str());
    } else { //existing type, get the last vertex id allocated
        eid = str2enum_iter->second;
    }
    numkv_out[tid]->set_value(vid, eid);
    //numkv_out[tid][vid] = eid;
    return eOK;
}

void enumkv_t::make_graph_baseline()
{
}


void enumkv_t::init_enum(int enumcount)
{
    max_ecount = enumcount;
    ecount = 0;
    enum2str = new char*[enumcount];
}

enumkv_t::enumkv_t()
{
    init_enum(256);
}

void enumkv_t::file_open(const string& odir, bool trunc)
{
    string base = odir + col_info[0]->p_name; 
    string vtfile = base + ".vtable";
    tid_t t_count = g->get_total_types();
    
    if(trunc) {
		//vtf = open(vtfile.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
		vtf = fopen(vtfile.c_str(), "w");
		assert(vtf != 0);
        for (tid_t i = 0; i < t_count; ++i) {
            if (numkv_out[i] != 0) {
                numkv_out[i]->file_open(base, trunc);
            }
        } 
    } else {
		//vtf = open(vtfile.c_str(), O_RDWR|O_CREAT, S_IRWXU);
		vtf = fopen(vtfile.c_str(), "r+");
		assert(vtf != 0); 
        for (tid_t i = 0; i < t_count; ++i) {
            if (numkv_out[i] != 0) {
                numkv_out[i]->file_open(base, trunc);
            }
        } 
    }
}

void enumkv_t::prep_graph_baseline()
{
    sflag_t    flag = flag1;
    vid_t   max_vcount;
    tid_t   t_count = g->get_total_types();
    
    if (0 == numkv_out) {
        numkv_out = (kvT_t<uint8_t>**) calloc (sizeof(kvT_t<uint8_t>*), t_count);
    }
    if (flag == 0) {
        flag1_count = g->get_total_types();
        for(tid_t i = 0; i < flag1_count; i++) {
            if (0 == numkv_out[i]) {
                max_vcount = g->get_type_scount(i);
                numkv_out[i] = new kvT_t<uint8_t>;
                numkv_out[i]->setup(i, max_vcount);
            }
        } 
        return;
    } 

    tid_t      pos  = 0;
    flag1_count = __builtin_popcountll(flag);
    for(tid_t i = 0; i < flag1_count; i++) {
        pos = __builtin_ctzll(flag);
        flag ^= (1L << pos);//reset that position
        if (0 == numkv_out[pos]) {
            max_vcount = g->get_type_scount(pos);
            numkv_out[pos]->setup(pos, max_vcount);
        }
    }
}

void enumkv_t::store_graph_baseline(bool clean /*=false*/)
{
    if (numkv_out == 0) return;
    
    tid_t       t_count = g->get_total_types();
    char        text[512];

    for (int i = 0; i < ecount; ++i) {
        sprintf(text, "%s\n", enum2str[i]);
        fwrite(text, sizeof(char), strlen(text), vtf);
    }
    
    // For each file.
    for (tid_t i = 0; i < t_count; ++i) {
        if (numkv_out[i] == 0) continue;
        numkv_out[i]->persist_vlog();
    }
}

void enumkv_t::read_graph_baseline()
{
    char  line[1024] = {0};
    tid_t    t_count = g->get_total_types();
    char*    saveptr = line;
    tid_t          i = 0; 

    while (fgets(saveptr, sizeof(line), vtf)) {
        enum2str[i] = gstrdup(saveptr);
        str2enum[saveptr] = i;
        ++i;
    }

    for (i = 0; i < t_count; ++i) {
        if (numkv_out[i] == 0) continue;
        numkv_out[i]->read_vtable();
    }
}

