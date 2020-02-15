#pragma once

#include <string>
#include <dirent.h>
#include <assert.h>
#include <string>
#include <unistd.h>

#include "graph_view.h"
#include "typekv.h"
#include "sgraph.h"
#include "util.h"

using namespace std;

template <class T>
class plaingraph_manager_t {
  public:
    //Class member, pgraph_t<T> only
    pgraph_t<T>* pgraph; 
    
  public:
    plaingraph_manager_t() {
        pgraph = 0;
    }

    inline pgraph_t<T>* get_plaingraph() {
        return pgraph;
    }
    
    inline void
    set_pgraph(cfinfo_t* a_pgraph) {
        pgraph = static_cast<pgraph_t<T>*>(a_pgraph);
    }

    void schema(int dir);
  private:
    void schema_plaingraph();
    void schema_plaingraphd();
    void schema_plaingraphuni();

  public:
    void split_files(const string& idirname, const string& odirname);
    
    void setup_graph(vid_t v_count, egraph_t egraph_type=eADJ);
    void setup_graph_vert_nocreate(vid_t v_count, egraph_t egraph_type=eADJ);
    
    void recover_graph_adj(const string& idirname, const string& odirname);

   /* 
    void prep_graph_fromtext2(const string& idirname, const string& odirname, 
                              typename callback<T>::parse_fn2_t parsebuf_fn);
    */
    void prep_graph_edgelog2(const string& idirname, const string& odirname);
    void prep_graph_edgelog(const string& idirname, const string& odirname);
    void prep_graph2(const string& idirname, const string& odirname);
    void prep_graph(const string& idirname, const string& odirname);
    
    void prep_graph_adj(const string& idirname, const string& odirname);
    void prep_graph_mix(const string& idirname, const string& odirname);
    
    void prep_graph_and_compute(const string& idirname, const string& odirname, stream_t<T>* streamh);
    void waitfor_archive_durable(double start);
    
    void run_pr_simple();
    void run_pr();
    void run_prd();
    void run_bfs(sid_t root = 1);
    void run_bfs_snb(sid_t root = 1);
    void run_1hop();
    void run_2hop();
};

template <class T>
void plaingraph_manager_t<T>::schema(int dir)
{
    switch(dir) {
        case 0:
            schema_plaingraph();
            break;
        case 1:
            schema_plaingraphd();
            break;
        case 2:
            schema_plaingraphuni();
            break;
        default:
            assert(0);
    }
}

template <class T>
void plaingraph_manager_t<T>::schema_plaingraph()
{
    g->cf_info = new cfinfo_t*[2];
    g->p_info = new pinfo_t[2];
    
    pinfo_t*    p_info    = g->p_info;
    cfinfo_t*   info      = 0;
    const char* longname  = 0;
    const char* shortname = 0;
    
    longname = "gtype";
    shortname = "gtype";
    info = new typekv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    ++p_info;
    
    longname = "friend";
    shortname = "friend";
    info = new ugraph<T>;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = 1;
    info->flag2 = 1;
    ++p_info;
    set_pgraph(info);

}

template <class T>
void plaingraph_manager_t<T>::schema_plaingraphd()
{
    g->cf_info = new cfinfo_t*[2];
    g->p_info = new pinfo_t[2];
    
    pinfo_t*    p_info    = g->p_info;
    cfinfo_t*   info      = 0;
    const char* longname  = 0;
    const char* shortname = 0;
    
    longname = "gtype";
    shortname = "gtype";
    info = new typekv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    ++p_info;
    
    longname = "friend";
    shortname = "friend";
    info = new dgraph<T>;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = 1;
    info->flag2 = 1;
    ++p_info;
    set_pgraph(info);
}

template <class T>
void plaingraph_manager_t<T>::schema_plaingraphuni()
{
    g->cf_info = new cfinfo_t*[2];
    g->p_info = new pinfo_t[2];
    
    pinfo_t*    p_info    = g->p_info;
    cfinfo_t*   info      = 0;
    const char* longname  = 0;
    const char* shortname = 0;
    
    longname = "gtype";
    shortname = "gtype";
    info = new typekv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    ++p_info;
    
    longname = "friend";
    shortname = "friend";
    info = new unigraph<T>;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = 1;
    info->flag2 = 1;
    ++p_info;
    set_pgraph(info);
}

template <class T>
void plaingraph_manager_t<T>::setup_graph(vid_t v_count, egraph_t egraph_type)
{
    //do some setup for plain graphs
    typekv_t* typekv = g->get_typekv();
    typekv->manual_setup(v_count, true);
    pgraph->prep_graph_baseline(egraph_type);
}

template <class T>
void plaingraph_manager_t<T>::setup_graph_vert_nocreate(vid_t v_count, egraph_t egraph_type)
{
    //do some setup for plain graphs
    typekv_t* typekv = g->get_typekv();
    typekv->manual_setup(v_count, false);
    pgraph->prep_graph_baseline(egraph_type);
}

struct arg_t {
    string file;
    void* manager;
};

//void* recovery_func(void* arg); 

template <class T>
void* recovery_func(void* a_arg) 
{
    arg_t* arg = (arg_t*) a_arg; 
    string filename = arg->file;
    plaingraph_manager_t<T>* manager = (plaingraph_manager_t<T>*)arg->manager;

    pgraph_t<T>* ugraph = (pgraph_t<T>*)manager->get_plaingraph();
    blog_t<T>*     blog = ugraph->blog;
    
    index_t size = fsize(filename);
    index_t edge_count = size/sizeof(edgeT_t<T>);
    
    //Lets set the edge log higher
    index_t new_count = upper_power_of_two(edge_count);
    ugraph->alloc_edgelog(new_count);
    cout << "edge_count = " << edge_count << endl;
    
    index_t edge_count2 = file_and_insert(filename, "", ugraph);
    assert(edge_count == edge_count2); 
    
    return 0;
}

template <class T>
void plaingraph_manager_t<T>::recover_graph_adj(const string& idirname, const string& odirname)
{
    string idir = idirname;
    index_t batch_size = (1L << residue);
    cout << "batch_size = " << batch_size << endl;

    arg_t* arg = new arg_t;
    arg->file = idir;
    arg->manager = this;
    pthread_t recovery_thread;
    if (0 != pthread_create(&recovery_thread, 0, &recovery_func<T> , arg)) {
        assert(0);
    }
    
    pgraph_t<T>* ugraph = (pgraph_t<T>*)get_plaingraph();
    blog_t<T>*     blog = ugraph->blog;
    index_t marker = 0;
    //index_t snap_marker = 0;
    index_t size = fsize(idirname);
    index_t edge_count = size/sizeof(edgeT_t<T>);
    
    double start = mywtime();
    
    //Make Graph
    while (0 == blog->blog_head) {
        usleep(20);
    }
    while (marker < edge_count) {
        usleep(20);
        while (marker < blog->blog_head) {
            marker = min(blog->blog_head, marker+batch_size);
            ugraph->create_marker(marker);
            ugraph->create_snapshot();
        }
    }
    delete arg; 
    double end = mywtime ();
    cout << "Make graph time = " << end - start << endl;
}

template <class T>
void plaingraph_manager_t<T>::split_files(const string& idirname, const string& odirname)
{
    pgraph_t<T>* ugraph = (pgraph_t<T>*)get_plaingraph();
    blog_t<T>*     blog = ugraph->blog;
    
    free(blog->blog_beg);
    blog->blog_beg = 0;
    blog->blog_head  += read_idir(idirname, &blog->blog_beg, false);
    
    //Upper align this, and create a mask for it
    index_t new_count = upper_power_of_two(blog->blog_head);
    blog->blog_mask = new_count -1;
    
    double start = mywtime();
    
    //Make Graph
    index_t marker = 0;
    index_t batch_size = blog->blog_head/residue;
    cout << "file_counts = " << residue << endl;
    char tmp[32];
    string ifile;
    FILE*  fd = 0;

    for (int i = 1; i < residue; ++i) {
        sprintf(tmp, "%d.dat",i);
        ifile = odirname + "part" + tmp; 
        fd = fopen(ifile.c_str(), "wb");
        fwrite(blog->blog_beg+marker, sizeof(edgeT_t<T>), batch_size, fd); 
        marker += batch_size;
    } 
        sprintf(tmp, "%d.dat", residue);
        ifile = odirname + "part" + tmp; 
        fd = fopen(ifile.c_str(), "wb");
        fwrite(blog->blog_beg+marker, sizeof(edgeT_t<T>), blog->blog_head - marker, fd); 

    double end = mywtime ();
    cout << "Split time = " << end - start << endl;
}

template <class T>
void plaingraph_manager_t<T>::prep_graph_adj(const string& idirname, const string& odirname)
{
    pgraph_t<T>* ugraph = (pgraph_t<T>*)get_plaingraph();
    blog_t<T>*     blog = ugraph->blog;
    
    if (1 != _source) {//only binary files
        cout << "this testcase expect binary input file(s)" << endl << std::flush;
        assert(0);
    }

    free(blog->blog_beg);
    blog->blog_beg = 0;
    index_t total_size = alloc_mem_dir(idirname, (char**)&blog->blog_beg, true);
    index_t count = total_size/sizeof(edgeT_t<T>);
    index_t new_count = upper_power_of_two(count);
    blog->blog_mask = new_count -1;
    blog->blog_count = count;
    
    read_idir_text(idirname, odirname, pgraph, file_and_insert);

    double start = mywtime();
    
    //Make Graph
    index_t marker = 0;
    index_t batch_size = (1L << residue);
    cout << "batch_size = " << batch_size << endl;

    while (marker < blog->blog_head) {
        marker = min(blog->blog_head, marker+batch_size);
        ugraph->create_marker(marker);
        ugraph->create_snapshot();
    }
    double end = mywtime ();
    cout << "Make graph time = " << end - start << endl;
}

template <class T>
void plaingraph_manager_t<T>::prep_graph_mix(const string& idirname, const string& odirname)
{
    pgraph_t<T>* ugraph = (pgraph_t<T>*)get_plaingraph();
    blog_t<T>*     blog = ugraph->blog;
    
    if (1 != _source) {//only binary files
        cout << "this testcase expect binary input file(s)" << endl << std::flush;
        assert(0);
    }
    
    free(blog->blog_beg);
    blog->blog_beg = 0;
    index_t total_size = alloc_mem_dir(idirname, (char**)&blog->blog_beg, true);
    index_t count = total_size/sizeof(edgeT_t<T>);
    index_t new_count = upper_power_of_two(count);
    blog->blog_mask = new_count -1;
    blog->blog_count = count;
    
    read_idir_text(idirname, odirname, pgraph, file_and_insert);
    
    double start = mywtime();
    
    //Make Graph
    index_t marker = 0;
    index_t batch_size = (1L << residue);
    //marker = blog->blog_head - batch_size;
    cout << "edge counts in basefile = " << batch_size << endl;
    
    index_t unit_size = (1L<<23);
    while (marker < batch_size ) {
        marker = min(batch_size, marker+ unit_size);
        ugraph->create_marker(marker);
        ugraph->create_snapshot();
    }
    
    double end = mywtime ();
    cout << "Make graph time = " << end - start << endl;
}

template <class T>
void plaingraph_manager_t<T>::prep_graph_edgelog2(const string& idirname, 
        const string& odirname) 
{
    pgraph_t<T>* pgraph = (pgraph_t<T>*)get_plaingraph();
    if (_persist) {
        g->file_open(true);
        g->create_threads(false, true);
    } else {
        g->create_threads(false, false);
    }
    
    //Batch and Make Graph
    double start = mywtime();
    if (0 == _source) {//text
        read_idir_text2(idirname, odirname, pgraph, parsebuf_and_insert);
    } else {//binary
        read_idir_text2(idirname, odirname, pgraph, buf_and_insert);
    }
    double end = mywtime();
    cout << "Batch Update Time = " << end - start << endl;
    if (_persist) g->type_store(odirname);
}

template <class T>
void plaingraph_manager_t<T>::prep_graph_edgelog(const string& idirname, 
        const string& odirname)
{
    pgraph_t<T>* pgraph = (pgraph_t<T>*)get_plaingraph();
    
    if (_persist) {
        g->file_open(true);
        g->create_threads(false, true);
    } else {
        g->create_threads(false, false);
    }
    
    //Batch and Make Graph
    double start = mywtime();
    if (0 == _source) {//text
        read_idir_text(idirname, odirname, pgraph, parsefile_and_insert);
    } else {//binary
        read_idir_text(idirname, odirname, pgraph, file_and_insert);
    }
    double end = mywtime();
    cout << "Batch Update Time = " << end - start << endl;
    if (_persist) g->type_store(odirname);
}

template <class T>
void plaingraph_manager_t<T>::prep_graph(const string& idirname, const string& odirname)
{
    pgraph_t<T>* ugraph = (pgraph_t<T>*)get_plaingraph();
    
    if (_persist) {
        g->file_open(true);
        g->create_threads(true, true);
    } else {
        g->create_threads(true, false);
    }
    
    //Batch and Make Graph
    double start = mywtime();
    if(0==_source) {//text
        read_idir_text(idirname, odirname, ugraph, parsefile_and_insert);
    } else {//binary
        read_idir_text(idirname, odirname, ugraph, file_and_insert);
    } 
    double end = mywtime();
    cout << "Batch Update Time = " << end - start << endl;
    
    if (_persist) {
        //Wait for make and durable graph
        waitfor_archive_durable(start);
    } else {
        g->waitfor_archive();
        end = mywtime();
        cout << "Make graph time = " << end - start << endl;
    }
    if (_persist) g->type_store(odirname);
}

template <class T>
void plaingraph_manager_t<T>::prep_graph2(const string& idirname, const string& odirname)
{
    pgraph_t<T>* ugraph = (pgraph_t<T>*)get_plaingraph();
    
    if (_persist) {
        g->file_open(true);
        g->create_threads(true, true);
    } else {
        g->create_threads(true, false);
    }
    
    //Batch and Make Graph
    double start = mywtime();
    if (0 == _source) {//text
        read_idir_text2(idirname, odirname, ugraph, parsebuf_and_insert);
    } else {//binary
        read_idir_text2(idirname, odirname, ugraph, buf_and_insert);
    }
    double end = mywtime();
    cout << "Batch Update Time = " << end - start << endl;
    
    if (_persist) {
        //Wait for make and durable graph
        waitfor_archive_durable(start);
    } else {
        g->waitfor_archive();
        end = mywtime();
        cout << "Make graph time = " << end - start << endl;
    }
    
    if (_persist) g->type_store(odirname);
}

template <class T>
void plaingraph_manager_t<T>::waitfor_archive_durable(double start) 
{
    pgraph_t<T>* pgraph = (pgraph_t<T>*)get_plaingraph();
    blog_t<T>* blog = pgraph->blog;
    index_t marker = blog->blog_head;
    if (marker != blog->blog_marker) {
        pgraph->create_marker(marker);
    }
    double end = 0;
    bool done_making = false;
    bool done_persisting = false;
    while (!done_making || !done_persisting) {
        if (blog->blog_tail == blog->blog_head && !done_making) {
            end = mywtime();
            cout << "Make Graph Time = " << end - start << endl;
            done_making = true;
        }
        if (blog->blog_wtail == blog->blog_head && !done_persisting) {
            end = mywtime();
            cout << "Durable Graph Time = " << end - start << endl;
            done_persisting = true;
        }
        usleep(1);
    }
}

#include "mem_iterative_analytics.h"

template <class T>
void plaingraph_manager_t<T>::run_pr() 
{
    pgraph_t<T>* pgraph = (pgraph_t<T>*)get_plaingraph();
    snap_t<T>* snaph = create_static_view(pgraph, STALE_MASK|V_CENTRIC);
    
    mem_pagerank<T>(snaph, 5);
    delete_static_view(snaph);
}

template <class T>
void plaingraph_manager_t<T>::run_pr_simple()
{
    pgraph_t<T>* pgraph = (pgraph_t<T>*)get_plaingraph();
    double start = mywtime();
    snap_t<T>* snaph = create_static_view(pgraph, STALE_MASK|V_CENTRIC);
    double end = mywtime();
    cout << "static View creation = " << end - start << endl;    
    
    mem_pagerank_simple<T>(snaph, 5);
    
    delete_static_view(snaph);
}
template <class T>
void plaingraph_manager_t<T>::run_bfs(sid_t root/*=1*/)
{
    double start, end;

    pgraph_t<T>* pgraph1 = (pgraph_t<T>*)get_plaingraph();
    
    start = mywtime();
    snap_t<T>* snaph = create_static_view(pgraph1, V_CENTRIC);
    end = mywtime();
    cout << "static View creation = " << end - start << endl;    
    
    uint8_t* level_array = 0;
    level_array = (uint8_t*) calloc(snaph->get_vcount(), sizeof(uint8_t));
    start = mywtime();
    mem_bfs<T>(snaph, level_array, root);
    end = mywtime();
    cout << "BFS complex = " << end - start << endl;    
    
    free(level_array);
    /*
    level_array = (uint8_t*) calloc(snaph->get_vcount(), sizeof(uint8_t));
    start = mywtime();
    mem_bfs_simple<T>(snaph, level_array, root);
    end = mywtime();
    cout << "BFS simple = " << end - start << endl;    
    */
    delete_static_view(snaph);
}

template <class T>
void plaingraph_manager_t<T>::run_bfs_snb(sid_t root/*=1*/)
{
    double start, end;

    pgraph_t<T>* pgraph1 = (pgraph_t<T>*)get_plaingraph();
    
    start = mywtime();
    snap_t<T>* snaph = create_static_view(pgraph1, STALE_MASK|V_CENTRIC);
    end = mywtime();
    cout << "static View creation = " << end - start << endl;    
    
    uint8_t* level_array = 0;
    level_array = (uint8_t*) calloc(_global_vcount, sizeof(uint8_t));
    start = mywtime();
    mem_bfs_snb<T>(snaph, level_array, root);
    end = mywtime();
    free(level_array);
    delete_static_view(snaph);
}

template <class T>
void plaingraph_manager_t<T>::run_1hop()
{
    pgraph_t<T>* pgraph1 = (pgraph_t<T>*)get_plaingraph();
    snap_t<T>* snaph = create_static_view(pgraph1, STALE_MASK|V_CENTRIC);
    mem_hop1<T>(snaph);
    delete_static_view(snaph);
}

template <class T>
void plaingraph_manager_t<T>::run_2hop()
{
    pgraph_t<T>* pgraph1 = (pgraph_t<T>*)get_plaingraph();
    snap_t<T>* snaph = create_static_view(pgraph1, STALE_MASK|V_CENTRIC);
    mem_hop2<T>(snaph);
    delete_static_view(snaph);
}

#include "stream_analytics.h"

template <class T>
void plaingraph_manager_t<T>::prep_graph_and_compute(const string& idirname, const string& odirname, stream_t<T>* streamh)
{
    pgraph_t<T>* ugraph = (pgraph_t<T>*)get_plaingraph();
    blog_t<T>*     blog = ugraph->blog;
    
    blog->blog_head  += read_idir(idirname, &blog->blog_beg, false);
    
    double start = mywtime();
    
    index_t marker = 0, prior_marker = 0;
    index_t batch_size = (1L << residue);
    cout << "batch_size = " << batch_size << endl;

    while (marker < blog->blog_head) {
        marker = min(blog->blog_head, marker+batch_size);
        
        //This is like a controlled update_stream_view() call
        streamh->set_edgecount(marker - prior_marker);
        streamh->set_edges(blog->blog_beg + prior_marker);
        streamh->snap_marker = prior_marker;
        streamh->sstream_func(streamh);
        prior_marker = marker;
    }
    double end = mywtime ();
    cout << "Make graph time = " << end - start << endl;
}

void dump() 
{
    p_ugraph_t* pgraph = (p_ugraph_t*) g->get_sgraph(1);
    //pgraph->compress_graph_baseline();
    auto view = create_static_view(pgraph, 0); 

    cout << "Vertex count: " << view->get_vcount() << "\n";

    weight_sid_t* neighbours = nullptr;
    uint64_t neighbours_sz = 0;

    for(sid_t vertex_id = 0; vertex_id < view->get_vcount(); vertex_id++) {
        uint64_t degree_out = view->get_degree_out(vertex_id);
        if (degree_out == 0) continue;

        cout << "[vertex_id: " << vertex_id << "]\n";
        cout << degree_out << "    outgoing edges: ";
        if(degree_out > neighbours_sz){
            neighbours_sz = degree_out;
            neighbours = (weight_sid_t*) realloc(neighbours, sizeof(neighbours[0]) * degree_out);
        }

        view->get_nebrs_out(vertex_id, neighbours);
        for (uint64_t edge_id = 0; edge_id < degree_out; edge_id++) {
            if (edge_id > 0) cout << ", ";
            cout << get_sid(neighbours[edge_id]) << " [w=" << get_weight_int(neighbours[edge_id]) << "]";
        }
        cout << endl;
        /*
        uint64_t degree_in = view->get_degree_in(vertex_id);
        cout << degree_in << "    incoming edges: ";
        if(degree_in > neighbours_sz){
            neighbours_sz = degree_in;
            neighbours = (lite_edge_t*) realloc(neighbours, sizeof(neighbours[0]) * degree_in);
        }
        view->get_nebrs_in(vertex_id, neighbours);
        for(uint64_t edge_id = 0; edge_id < degree_in; edge_id++){
            if(edge_id > 0) cout << ", ";
            cout << get_sid(neighbours[edge_id]) << " [w=" << get_weight_int(neighbours[edge_id]) << "]";
        }
        cout << endl;
        */
    }

    free(neighbours); 
    neighbours = nullptr; 
    neighbours_sz = 0;

    delete_static_view(view);
}

void dump_simple() 
{
    p_ugraph_t* pgraph = (p_ugraph_t*) g->get_sgraph(1);
    //pgraph->compress_graph_baseline();
    auto view = create_static_view(pgraph, SIMPLE_MASK); 

    cout << "Vertex count: " << view->get_vcount() << "\n";

    weight_sid_t* neighbours = nullptr;
    uint64_t neighbours_sz = 0;

    for(sid_t vertex_id = 0; vertex_id < view->get_vcount(); vertex_id++) {
        cout << "[vertex_id: " << vertex_id << "]\n";
        uint64_t degree_out = view->get_degree_out(vertex_id);

        cout << degree_out << "    outgoing edges: ";
        if(degree_out > neighbours_sz){
            neighbours_sz = degree_out;
            neighbours = (weight_sid_t*) realloc(neighbours, sizeof(neighbours[0]) * degree_out);
        }

        view->get_nebrs_out(vertex_id, neighbours);
        for (uint64_t edge_id = 0; edge_id < degree_out; edge_id++) {
            if (edge_id > 0) cout << ", ";
            cout << get_sid(neighbours[edge_id]) << " [w=" << get_weight_int(neighbours[edge_id]) << "]";
        }
        cout << endl;
        /*
        uint64_t degree_in = view->get_degree_in(vertex_id);
        cout << degree_in << "    incoming edges: ";
        if(degree_in > neighbours_sz){
            neighbours_sz = degree_in;
            neighbours = (lite_edge_t*) realloc(neighbours, sizeof(neighbours[0]) * degree_in);
        }
        view->get_nebrs_in(vertex_id, neighbours);
        for(uint64_t edge_id = 0; edge_id < degree_in; edge_id++){
            if(edge_id > 0) cout << ", ";
            cout << get_sid(neighbours[edge_id]) << " [w=" << get_weight_int(neighbours[edge_id]) << "]";
        }
        cout << endl;
        */
    }

    free(neighbours); 
    neighbours = nullptr; 
    neighbours_sz = 0;

    delete_static_view(view);
}
