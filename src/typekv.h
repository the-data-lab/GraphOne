#pragma once
#include "onestr.h"
#include "str2sid.h"

class tinfo_t {
 public:
    vid_t   max_vcount;
    vid_t   vert_id;
    tid_t   tid;
    char*   type_name;
    strkv_t strkv;
};

class inference_tinfo_t {
 public:
    char* type_name;
    tid_t count;
    tid_t* tlist;
};

//type class
class typekv_t : public cfinfo_t {
  private:
    
    //map <string, sid_t> str2vid;
	str2intmap str2vid;// = new str2intmap();	
    
    //mapping between type-name(string) and type-id
    map<string, tid_t> str2enum;
    
    //for each type/class, the count of vertices  
    tinfo_t*    t_info;
    tid_t       t_count;

    inference_tinfo_t* it_info;
    tid_t       it_count;

    tid_t       max_count;

    FILE*   vtf;   //vertex table file

  public:

    typekv_t();

    //void alloc_edgelog(tid_t t);
    tid_t manual_setup(vid_t vert_count, bool create_vert, const string& type_name="gtype");
    void init_enum(int enumcount);
    
    inline void populate_inference_type(const char* e, tid_t count, tid_t* tlist) {
        vid_t id = it_count++;
        str2enum[e] = id + t_count;
        it_info[id].type_name = gstrdup(e);
        it_info[id].tlist = tlist;
        it_info[id].count = count;
    };

    inline  vid_t get_type_scount(tid_t type) {
        return TO_VID(t_info[type].max_vcount);
    }
    inline vid_t get_type_vcount(tid_t type) {
        return TO_VID(t_info[type].vert_id);
    }
    inline const char* get_type_name(tid_t type) {
        return t_info[type].type_name;
    }
    inline tid_t get_total_types() {
        return t_count;
    }

    inline sid_t get_sid(const char* src) {
        sid_t str2vid_iter = str2vid.find(src);
        if (str2vid_iter == UINT64_MAX) {
            return INVALID_SID;
        }
        return str2vid_iter;
    }

    /*
	inline sid_t get_sid(const char* src) {
        map<string, sid_t>::iterator str2vid_iter = str2vid.find(src);
        if (str2vid_iter == str2vid.end()) {
            return INVALID_SID;
        }
        return str2vid_iter->second;
    }
	*/

    inline string get_vertex_name(sid_t sid) {
        tid_t tid = TO_TID(sid);
        vid_t vid = TO_VID(sid);
        return t_info[tid].strkv.get_value(vid);
        //return t_info[tid].log_beg + t_info[tid].vid2name[vid];
    }
    
    sid_t type_update(const string& src, const string& dst);
    sid_t type_update(const string& src, tid_t tid);
    
    void make_graph_baseline();
    virtual void store_graph_baseline(bool clean = false); 
    void read_graph_baseline();
    void file_open(const string& odir, bool trunc); 

  public:
    virtual status_t filter(sid_t sid, univ_t value, filter_fn_t fn);
    virtual status_t get_encoded_value(const char* value, univ_t* univ);
    virtual status_t get_encoded_values(const char* value, tid_t** tids, qid_t* counts);

    static cfinfo_t* create_instance();
};
