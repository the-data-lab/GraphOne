#include "graph.h"
#include "typekv.h"

sid_t typekv_t::type_update(const string& src, const string& dst)
{
    sid_t       src_id = 0;
    sid_t       super_id = 0;
    vid_t       vid = 0;
    tid_t       type_id;
    
    //allocate class specific ids.
    //map<string, vid_t>::iterator str2vid_iter = str2vid.find(src);
    //if (str2vid.end() != str2vid_iter) {
    //    src_id = str2vid_iter->second;
        
    sid_t str2vid_iter = str2vid.find(src);
    if (UINT64_MAX != str2vid_iter) {
        src_id = str2vid_iter;
        /*
        //dublicate entry 
        //If type mismatch, delete original //XXX
        tid_t old_tid = TO_TID(src_id);

        if (old_tid != type_id) {
            / *
            //Different types, delete
            str2vid.erase(str2vid_iter);
            cout << "Duplicate unique Id: " << src << " Deleting both. " ;
            cout << "Existing Type: " << (char*)(log_beg + t_info[old_tid].type_name) << "\t";
            cout << "New Type: " << (char*)(log_beg + t_info[type_id].type_name) << endl;
            * /
            assert(0);
            return INVALID_SID;
        }*/
    } else {
        //create new type
        map<string, tid_t>::iterator str2enum_iter = str2enum.find(dst);

        //Have not see this type, create one
        if (str2enum.end() == str2enum_iter) {
            type_id = t_count++;
            super_id = TO_SUPER(type_id);
            str2enum[dst] = type_id;
            t_info[type_id].vert_id = super_id; 
            t_info[type_id].type_name = strdup(dst.c_str());
            t_info[type_id].max_vcount = (1<<21);//guess
            string filename = g->odirname + col_info[0]->p_name;
            t_info[type_id].strkv.setup(type_id, t_info[type_id].max_vcount);
            t_info[type_id].strkv.file_open(filename, true);

        } else { //existing type, get the last vertex id allocated
            type_id = str2enum_iter->second;
            super_id = t_info[type_id].vert_id;
        }

        src_id = super_id++;
        t_info[type_id].vert_id = super_id;
		str2vid.insert(src, src_id);
        // str2vid[src] = src_id;

        vid     = TO_VID(super_id); 
        assert(vid < t_info[type_id].max_vcount);
        t_info[type_id].strkv.set_value(vid, src.c_str());
    }


    return src_id;
}

sid_t typekv_t::type_update(const string& src, tid_t type_id)
{
    sid_t       src_id = 0;
    sid_t       super_id = 0;
    vid_t       vid = 0;

    assert(type_id < t_count);

    super_id = t_info[type_id].vert_id;

    ////allocate class specific ids.
    //map<string, vid_t>::iterator str2vid_iter = str2vid.find(src);
    //if (str2vid.end() == str2vid_iter) {
    //    src_id = super_id++;
    //    t_info[type_id].vert_id = super_id;
    //    ++g->vert_count;
    //    str2vid[src] = src_id;

    //    vid     = TO_VID(src_id); 
    //    assert(vid < t_info[type_id].max_vcount);
    //    
    //    t_info[type_id].strkv.set_value(vid, src.c_str());
    //} else {
    //    //dublicate entry 
    //    //If type mismatch, delete original //XXX
    //    src_id = str2vid_iter->second;
    //    tid_t old_tid = TO_TID(src_id);

    //    if (old_tid != type_id) {
    //        //Different types, delete
    //        assert(0);
    //        return INVALID_SID;
    //    }
    //}
    
	//allocate class specific ids.
    sid_t str2vid_iter = str2vid.find(src);
    if (INVALID_SID == str2vid_iter) {
        src_id = super_id++;
        t_info[type_id].vert_id = super_id;
        str2vid.insert(src, src_id);

        vid     = TO_VID(src_id); 
        assert(vid < t_info[type_id].max_vcount);
        
        t_info[type_id].strkv.set_value(vid, src.c_str());
    } else {
        //dublicate entry 
        //If type mismatch, delete original //XXX
        src_id = str2vid_iter;
        tid_t old_tid = TO_TID(src_id);

        if (old_tid != type_id) {
            //Different types, delete
            assert(0);
            return INVALID_SID;
        }
    }
    return src_id;
}

status_t typekv_t::filter(sid_t src, univ_t a_value, filter_fn_t fn)
{
    //value is already encoded, so typecast it
    tid_t  dst = (tid_t) a_value.value_tid;
  
    assert(fn == fn_out); 
    
    tid_t type1_id = TO_TID(src);
    if (type1_id == dst) return eOK;
    return eQueryFail;
}


status_t typekv_t::get_encoded_value(const char* value, univ_t* univ) 
{
    map<string, tid_t>::iterator str2enum_iter = str2enum.find(value);
    if (str2enum.end() == str2enum_iter) {
        return eQueryFail;
    }

    univ->value_tid = str2enum_iter->second;
    return eOK;
}
    
status_t typekv_t::get_encoded_values(const char* value, tid_t** tids, qid_t* counts)
{
    tid_t tid;
    map<string, tid_t>::iterator str2enum_iter = str2enum.find(value);
    if (str2enum.end() == str2enum_iter) {
        return eQueryFail;
    }

    tid = str2enum_iter->second;
    assert(tid < t_count + it_count);
    
    if (tid < t_count) {
        *counts = 1;
        tids[0] = new tid_t;
        tids[0][0] = tid;
    } else {
        *counts = it_info[tid - t_count].count;
        tids[0] = it_info[tid - t_count].tlist; 
    }

    return eOK;
}

void typekv_t::make_graph_baseline() 
{
    
}

void typekv_t::file_open(const string& dir, bool trunc) 
{
    string filename = dir + col_info[0]->p_name;
    string vtfile;
    
    vtfile = filename + ".vtable";

    if(trunc) {
		//vtf = open(vtfile.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
		vtf = fopen(vtfile.c_str(), "w");
		assert(vtf != 0); 
    } else {
		//vtf = open(vtfile.c_str(), O_RDWR|O_CREAT, S_IRWXU);
		vtf = fopen(vtfile.c_str(), "r+");
		assert(vtf != 0); 
    }
    for (tid_t t = 0; t < t_count; ++t) {
        t_info[t].strkv.file_open(filename, trunc);
    }
}

void typekv_t::store_graph_baseline(bool clean)
{
    fseek(vtf, 0, SEEK_SET);

    //write down the type info, t_info
    char type_text[512];
    for (tid_t t = 0; t < t_count; ++t) {
#ifdef B32
        sprintf(type_text, "%u %u %s\n", t_info[t].max_vcount, TO_VID(t_info[t].vert_id), 
                t_info[t].type_name);
#elif B64
        sprintf(type_text, "%lu %lu %s\n", t_info[t].max_vcount, TO_VID(t_info[t].vert_id), 
                t_info[t].type_name);
#endif    
        fwrite(type_text, sizeof(char), strlen(type_text), vtf);
        t_info[t].strkv.handle_write();
    }
    //str2enum: No need to write. We make it from disk during initial read.
    //XXX: write down the deleted id list
}

void typekv_t::read_graph_baseline()
{
    char  line[1024] = {0};
    char* token = 0;
    tid_t t = 0;
    char* saveptr = line;
    
    while (fgets(saveptr, sizeof(line), vtf)) {
        token = strtok_r(saveptr, " \n", &saveptr);
        t_info[t].max_vcount = strtol(token, NULL, 0);
        token = strtok_r(saveptr, " \n", &saveptr);
        t_info[t].vert_id = strtol(token, NULL, 0) + TO_SUPER(t);
        token = strtok_r(saveptr, " \n", &saveptr);
        t_info[t].type_name = strdup(token);
        t_info[t].strkv.setup(t, t_info[t].max_vcount);
    
        string filename = g->odirname + col_info[0]->p_name;
        t_info[t].strkv.file_open(filename, false);
        t_info[t].strkv.read_vtable();
        t_info[t].strkv.prep_str2sid(str2vid);
        ++t;
    }

    //Populate str2enum now.
    for (tid_t t = 0; t < t_count; ++t) {
        str2enum[t_info[t].type_name] = t;
    }
    t_count = t;
}

typekv_t::typekv_t():cfinfo_t(etype)
{
    init_enum(256);
    vtf = 0;
}

//Required to be called as we need to have a guess for max v_count
tid_t typekv_t::manual_setup(vid_t vert_count, bool create_vert, const string& type_name/*="gtype"*/)
{
    str2enum[type_name.c_str()] = t_count;
    t_info[t_count].type_name = strdup(type_name.c_str());

    if (create_vert) {
        t_info[t_count].vert_id = TO_SUPER(t_count) + vert_count;
    } else {
        t_info[t_count].vert_id = TO_SUPER(t_count);
    }
    t_info[t_count].max_vcount = vert_count;
    t_info[t_count].strkv.setup(t_count, vert_count);
    return t_count++;//return the tid of this type
}

cfinfo_t*
typekv_t::create_instance()
{
    return new typekv_t;
}
    
void typekv_t::init_enum(int enumcount) 
{
    max_count = enumcount;
    t_count = 0;
    it_count = 0;
    t_info = new tinfo_t [enumcount];
    memset(t_info, 0, sizeof(tinfo_t)*enumcount);
    it_info = new inference_tinfo_t[enumcount];
    memset(it_info, 0, sizeof(inference_tinfo_t)*enumcount);
};
