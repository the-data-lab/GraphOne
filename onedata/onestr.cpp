#include <string.h>
#include "graph.h"
#include "onestr.h"

strkv_t::strkv_t()
{
    kv = 0;
    tid = 0;
    vtf = -1;
}

void strkv_t::set_value(vid_t vid, const char* value)
{
    //Check if values are same;
    if (0 != strncmp(mem.get_ptr(kv[vid]), value, strlen(value))) {
        index_t offset = 0;
        char* ptr = mem.alloc_str(strlen(value)+1, offset);
        memcpy(ptr, value, strlen(value) + 1);
        kv[vid] = offset;
    }

    /*
    kv[vid] = ptr;
    dvt[dvt_count].vid = vid;
    dvt[dvt_count].offset = ptr - log_beg; 
    ++dvt_count;
    */
}

const char* strkv_t::get_value(vid_t vid)
{
    return mem.get_ptr(kv[vid]);
}

void strkv_t::setup(tid_t t, vid_t v_count)
{
    tid = t;
    max_vcount = v_count;
    assert(v_count); 
    kv = (sid_t*)calloc(sizeof(sid_t), v_count);
    
    mem.setup(1L<<28);
    /*
    if (posix_memalign((void**) &dvt, 2097152, dvt_max_count*sizeof(disk_strkv_t*))) {
        perror("posix memalign vertex log");
    }*/
}

void strkv_t::handle_write()
{
    /*
    //Make a copy
    sid_t count =  dvt_count;

    //update the mark
    dvt_count = 0;

    fwrite(dvt, sizeof(disk_strkv_t), count, vtf);*/
    
    if (eOK == mem.handle_write()) {
        vid_t v_count = g->get_type_vcount(tid);
        if (v_count != 0) {
            pwrite(vtf, kv, v_count*sizeof(sid_t), 0);
        }
    }
}

void strkv_t::file_open(const string& filename, bool trunc)
{
    char  file_ext[16];
    sprintf(file_ext,"%u",tid);
    
    string vtfile = filename + file_ext + ".vtable";
    string etfile = filename + file_ext + ".etable";
    if (trunc) {
        //vtf = fopen(vtfile.c_str(), "wb");
		vtf = open(vtfile.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);

    } else {
	    vtf = open(vtfile.c_str(), O_RDWR|O_CREAT, S_IRWXU);
        //vtf = fopen(vtfile.c_str(), "r+b");
    }
        
    assert(vtf != -1);
    mem.file_open(etfile.c_str(), trunc);
}

void strkv_t::read_etable()
{
}

void strkv_t::read_vtable()
{
    //read etf
    mem.handle_read();

    //read vtf 
    off_t size = fsize(vtf);
    if (size == -1L) { assert(0); }
    
    if (size != 0) {
        vid_t vcount = size/sizeof(sid_t);
        assert(vcount == g->get_type_vcount(tid));
        vid_t max_vcount = g->get_type_scount(tid);
        kv = (sid_t*)calloc(sizeof(sid_t), max_vcount);
        read(vtf, kv, sizeof(sid_t)*vcount);
    }

    /*
    off_t size = 0; //XXX fsize(vtfile.c_str());
    if (size == -1L) {
        assert(0);
    }
    vid_t count = (size/sizeof(disk_strkv_t));

    //read in batches
    while (count !=0) {
        vid_t read_count = fread(dvt, sizeof(disk_strkv_t), dvt_max_count, vtf);
        for (vid_t v = 0; v < read_count; ++v) {
            kv[dvt[v].vid] = log_beg + dvt[v].offset;
        }
        count -= read_count;
    }
    dvt_count = 0;
    */
}

//void strkv_t::prep_str2sid(map<string, sid_t>& str2sid)
void strkv_t::prep_str2sid(str2intmap& str2sid)
{
    
    char* type_name = 0;
    sid_t super_id = TO_SUPER(tid);
    vid_t v_count = g->get_type_vcount(tid);
    char* log_beg = mem.get_ptr(0); 
    
    //create the str2vid now
    for (vid_t vid = 0; vid < v_count; ++vid) {
        type_name = log_beg + kv[vid];
        //str2sid[type_name] = super_id + vid;
		str2sid.insert(type_name, super_id + vid);
    }

    /*
    if (log_head == 0) return;
    string dst;
    sid_t sid = TO_THIGH(super_id);
    for(sid_t v = sid; v < super_id; ++v) {
        dst = kv[v - sid];
        str2sid[dst] = v;
    } */
}
