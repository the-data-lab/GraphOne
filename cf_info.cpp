
#include <algorithm>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "cf_info.h"
#include "graph.h"

double bu_factor = 0.07;
int32_t MAX_BCOUNT = 256;
uint64_t MAX_ECOUNT = (1<<9);
uint64_t MAX_PECOUNT = (MAX_ECOUNT << 1)/3;
index_t  BATCH_SIZE = (1L << 16);//edge batching in edge log
index_t  BATCH_MASK =  0xFFFF;

//In-memory data structure size
index_t  BLOG_SHIFT = 27;
//index_t  BLOG_SIZE = (1L << BLOG_SHIFT); //size of edge log
//index_t  BLOG_MASK = (BLOG_SIZE - 1);

vid_t RANGE_COUNT = 1024;
index_t  SNAP_COUNT  = (2);
index_t  LOCAL_VUNIT_COUNT = 20;
index_t  LOCAL_DELTA_SIZE = 28;
index_t  DURABLE_SIZE = (1L << 28);//Durable adj-list

#ifdef BULK
index_t  DELTA_SIZE = (1L << 37) ;//(32 + sizeof(T));  //sizeo of delta adj-list
#endif

//durable data structure buffer size
index_t  W_SIZE = (1L << 12); //Edges to write
index_t  DVT_SIZE = (1L <<24);//durable v-unit 

using std::swap;

void* alloc_buf()
{
    return calloc(sizeof(edge_t), MAX_ECOUNT);
}

void free_buf(void* buf)
{
    free(buf);
    buf = 0;
}
void cfinfo_t::create_columns(propid_t prop_count)
{
    col_info = new pinfo_t* [prop_count];
    col_count = 0;
}

void cfinfo_t::add_column(pinfo_t* prop_info, const char* longname, const char* shortname)
{
    g->add_property(longname);
    prop_info->p_name = gstrdup(shortname);
    prop_info->p_longname = gstrdup(longname);
    prop_info->cf_id = cf_id;
    prop_info->local_id = col_count;
    
    col_info[col_count] = prop_info;
    ++col_count;
}

void cfinfo_t::add_edge_property(const char* longname, prop_encoder_t* a_prop_encoder)
{
    prop_encoder = a_prop_encoder;
}

cfinfo_t::cfinfo_t(gtype_t type/* = evlabel*/)
{
    snap_id = 0;
    gtype = type;
    flag1 = 0;
    flag2 = 0;
    flag1_count = 0;
    flag2_count = 0;

    col_info = 0;
    col_count = 0;
    
    q_count = 512;
    q_beg = (index_t*)calloc(q_count, sizeof(index_t));
    if (0 == q_beg) {
        perror("posix memalign batch edge log");
    }
    q_head = 0;
    q_tail = 0;
    
    snapshot = 0;
    snap_f = 0;
    wtf = 0;
}

void cfinfo_t::create_wthread()
{
    if (egraph != gtype) {
        return;
    }

    if (0 != pthread_create(&w_thread, 0, cfinfo_t::w_func, (void*)this)) {
        assert(0);
    }
}

//void* cfinfo_t::w_func(void* arg)
//{
//    cout << "enterting w_func" << endl; 
//    cfinfo_t* ptr = (cfinfo_t*)(arg);
//    pthread_mutex_init(&ptr->w_mutex, 0);
//    pthread_cond_init(&ptr->w_condition, 0);
//    
//    do {
//        pthread_mutex_lock(&ptr->w_mutex);
//        pthread_cond_wait(&ptr->w_condition, &ptr->w_mutex);
//        pthread_mutex_unlock(&ptr->w_mutex);
//        ptr->write_edgelog();
//        cout << "Writing w_thd" << endl;
//    } while(1);
//
//    return 0;
//}

void* cfinfo_t::w_func(void* arg)
{
    cout << "enterting w_func" << endl; 
    
    cfinfo_t* ptr = (cfinfo_t*)(arg);
    pthread_mutex_init(&ptr->w_mutex, 0);
    pthread_cond_init(&ptr->w_condition, 0);
    
    do {
        ptr->write_edgelog();
        //cout << "Writing w_thd" << endl;
        usleep(10);
    } while(1);

    return 0;
}

void cfinfo_t::create_snapthread()
{
    if (egraph != gtype) {
        return;
    }

    pthread_mutex_init(&snap_mutex, 0);
    pthread_cond_init(&snap_condition, 0);
    if (0 != pthread_create(&snap_thread, 0, cfinfo_t::snap_func, (void*)this)) {
        assert(0);
    }
}

void* cfinfo_t::snap_func(void* arg)
{
    cfinfo_t* ptr = (cfinfo_t*)(arg);
    
    do {
        //struct timeval tp;
        struct timespec ts;
        int rc = clock_gettime(CLOCK_REALTIME, &ts);

        //ts.tv_sec = tp.tv_sec;
        //ts.tv_nsec = tp.tv_usec * 1000;
        ts.tv_nsec += 100 * 1000000;  //30 is my milliseconds
        pthread_mutex_lock(&ptr->snap_mutex);
        pthread_cond_timedwait(&ptr->snap_condition, &ptr->snap_mutex, &ts);
        pthread_mutex_unlock(&ptr->snap_mutex);
        while (eOK == ptr->create_snapshot());
    } while(1);

    return 0;
}

status_t cfinfo_t::create_snapshot()
{
    index_t snap_marker = 0;
    int work_done = 0;
    //index_t last_durable_marker = 0;
        //create_marker(0);
        if (eOK == move_marker(snap_marker)) {
            make_graph_baseline();
            update_marker();
            new_snapshot(snap_marker);
            ++work_done;
		}
        if (work_done != 0) {
            return eOK;
        }
        return eNoWork;
}

void cfinfo_t::new_snapshot(index_t snap_marker, index_t durable_marker /* = 0 */)
{
    snapshot_t* next = new snapshot_t;
    next->snap_id = snap_id + 1;
    
    if (snapshot) {
        if (durable_marker == 0) {
            next->durable_marker = snapshot->durable_marker;
        } else {
            next->durable_marker = durable_marker;
        }
    } else {
        next->durable_marker = 0;
    }

    next->marker = snap_marker;
    next->next = snapshot;
    snapshot = next;
    ++snap_id;
}

void cfinfo_t::read_snapshot()
{
    assert(snap_f != 0);

    off_t size = fsize(snapfile.c_str());
    if (size == -1L) {
        assert(0);
    }
    
    snapid_t count = (size/sizeof(disk_snapshot_t));
    disk_snapshot_t* disk_snapshot = (disk_snapshot_t*)calloc(count, sizeof(disk_snapshot_t));
    fread(disk_snapshot, sizeof(disk_snapshot_t), count, snap_f);
    
    snapshot_t* next = 0;
    for (snapid_t i = 0; i < count; ++i) {
        next = new snapshot_t;
        next->snap_id = disk_snapshot[i].snap_id;
        next->marker = disk_snapshot[i].marker;
        next->durable_marker = disk_snapshot[i].durable_marker;
        next->next = snapshot;
        snapshot = next;
    }
}

void cfinfo_t::write_snapshot()
{
    disk_snapshot_t* disk_snapshot = (disk_snapshot_t*)malloc(sizeof(disk_snapshot_t));
    disk_snapshot->snap_id= snapshot->snap_id;
    disk_snapshot->marker = snapshot->marker;
    disk_snapshot->durable_marker = snapshot->durable_marker;
    fwrite(disk_snapshot, sizeof(disk_snapshot_t), 1, snap_f);
}

status_t cfinfo_t::batch_update(const string& src, const string& dst, propid_t pid /* = 0*/)
{
    assert(0);
    return  eOK;
}

void cfinfo_t::compress_graph_baseline()
{
    assert(0);
    return;
}

status_t cfinfo_t::batch_update(const string& src, const string& dst, const char* property_str)
{
    assert(0);
    return  eOK;
}
    
status_t cfinfo_t::batch_update(const string& src, const string& dst, propid_t pid, 
                          propid_t count, prop_pair_t* prop_pair, int del /* = 0 */)
{
    //cout << "ignoring edge properties" << endl;
    batch_update(src, dst, pid);
    return eOK;
}

void cfinfo_t::waitfor_archive()
{   
    return;
}

void cfinfo_t::prep_graph_baseline()
{   
    return;
}
    
void cfinfo_t::make_graph_baseline()
{
    assert(0);
}
    
status_t cfinfo_t::move_marker(index_t& snap_marker)
{
   return eNoWork;
}

status_t cfinfo_t::write_edgelog()
{
   return eNoWork;
}

void cfinfo_t::store_graph_baseline(bool clean)
{
    assert(0);
}

void cfinfo_t::read_graph_baseline()
{
    assert(0);
}

void cfinfo_t::file_open(const string& filename, bool trunc)
{
    assert(0);
}

/*******label specific **********/
status_t cfinfo_t::filter(sid_t sid, univ_t value, filter_fn_t fn)
{
    assert(0);
    return eOK;
}
    
void cfinfo_t::print_raw_dst(tid_t tid, vid_t vid, propid_t pid /* = 0 */)
{
    assert(0);
}

status_t cfinfo_t::get_encoded_value(const char* value, univ_t* univ)
{
    assert(0);
    return eQueryFail;
}

///****************///
/*status_t cfinfo_t::transform(srset_t* iset, srset_t* oset, direction_t direction)
{
    assert(0);
    return eOK;
}

status_t cfinfo_t::extend(srset_t* iset, srset_t* oset, direction_t direction)
{
    assert(0);
    return eOK;
}
*/

off_t fsize(const string& fname)
{
    struct stat st;
    if (0 == stat(fname.c_str(), &st)) {
        return st.st_size;
    }
    perror("stat issue");
    return -1L;
}

off_t fsize(int fd)
{
    struct stat st;
    if (0 == fstat(fd, &st)) {
        return st.st_size;
    }
    perror("stat issue");
    return -1L;
}
