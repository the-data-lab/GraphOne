#include <iostream>

#include "plain_to_edge.h"

#include "stream_analytics.h"
#include "sstream_analytics.h"

using namespace std;

// Credits to :
// http://www.memoryhole.net/kyle/2012/06/a_use_for_volatile_in_multithr.html
float qthread_dincr(float *operand, float incr)
{
    //*operand = *operand + incr;
    //return incr;
    
    union {
       rank_t   d;
       uint32_t i;
    } oldval, newval, retval;
    do {
         oldval.d = *(volatile rank_t *)operand;
         newval.d = oldval.d + incr;
         //__asm__ __volatile__ ("lock; cmpxchgq %1, (%2)"
         __asm__ __volatile__ ("lock; cmpxchg %1, (%2)"
                                : "=a" (retval.i)
                                : "r" (newval.i), "r" (operand),
                                 "0" (oldval.i)
                                : "memory");
    } while (retval.i != oldval.i);
    return oldval.d;
}

double qthread_doubleincr(double *operand, double incr)
{
    //*operand = *operand + incr;
    //return incr;
    
    union {
       double   d;
       uint64_t i;
    } oldval, newval, retval;
    do {
         oldval.d = *(volatile double *)operand;
         newval.d = oldval.d + incr;
         //__asm__ __volatile__ ("lock; cmpxchgq %1, (%2)"
         __asm__ __volatile__ ("lock; cmpxchg %1, (%2)"
                                : "=a" (retval.i)
                                : "r" (newval.i), "r" (operand),
                                 "0" (oldval.i)
                                : "memory");
    } while (retval.i != oldval.i);
    return oldval.d;
}

struct estimate_t {
    degree_t durable_degree;
    degree_t delta_degree;
    degree_t degree;
    degree_t space_left;
    //index_t  io_read;
    //index_t  io_write;
    int      chain_count;
    int16_t      type;
};

#define HUB_TYPE 1
#define HUB_COUNT_TEST 32768 

#define ALIGN_MASK_32B 0xFFFFFFFFFFFFFFF0
#define ALIGN_MASK_4KB 0xFFFFFFFFFFFFFC00
//#define UPPER_ALIGN_32B(x) (((x) + 16) & ALIGN_MASK_32B)
//16+4-1=19
#define UPPER_ALIGN_32B(x) (((x) + 19) & ALIGN_MASK_32B)
//1024+4-1=1027
#define UPPER_ALIGN_4KB(x) (((x) + 1027) & ALIGN_MASK_4KB)


template <class T>
void estimate_chain(const string& idirname, const string& odirname)
{
    plaingraph_manager_t<dst_id_t> plaingraph_manager; 
    plaingraph_manager.schema(_dir);
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(_global_vcount);    
    
    propid_t          cf_id = g->get_cfid("friend");
    pgraph_t<dst_id_t>* ugraph = (pgraph_t<dst_id_t>*)g->cf_info[cf_id];
    blog_t<dst_id_t>*     blog = ugraph->blog;
    
    blog->blog_head  += read_idir<T>(idirname, &blog->blog_beg, false);
    
    double start = mywtime();

    index_t marker = blog->blog_head ;
    cout << "End marker = " << blog->blog_head;
    cout << "make graph marker = " << marker << endl;
    if (marker == 0) return;
    
    edge_t* edge = blog->blog_beg;
    estimate_t* est = (estimate_t*)calloc(sizeof(estimate_t), _global_vcount);
    index_t last = 0;
    vid_t src = 0;
    vid_t dst = 0;

    index_t total_used_memory = 0;
    index_t total_chain = 0;
    int max_chain = 0;
    vid_t total_hub_vertex = 0;
    index_t batching_size = (1 << residue); //65536;
    //index_t batching_size = marker;
    
    for (index_t i = 0; i < marker; i +=batching_size) {
        last = min(i + batching_size, marker);
        //do batching
        for (index_t j = i; j < last; ++j) {
            src = edge[j].src_id;
            dst = get_dst(edge[j]);
            est[src].degree++;
            est[dst].degree++;
        }

        index_t used_memory = 0;
        //Do memory allocation and cleaning
        #pragma omp parallel reduction(+:used_memory, total_chain) reduction(max:max_chain)
        {
        index_t  local_memory = 0;
        #pragma omp for  
        for (vid_t vid = 0; vid < _global_vcount; ++vid) {
            if (est[vid].degree == 0) continue;
            
            local_memory = est[vid].degree*sizeof(T) + 16;
            used_memory += local_memory;
            est[vid].chain_count++;
            max_chain = max(max_chain, est[vid].chain_count);
            total_chain++;
            est[vid].delta_degree += est[vid].degree;
            est[vid].degree = 0;
        }
        }
        total_used_memory += used_memory;
    }

    double end = mywtime ();
    cout << "Used Memory = " << total_used_memory << endl;
    cout << "Make graph time = " << end - start << endl;
    cout << "total_hub_vertex =" << total_hub_vertex << endl;
    cout << "total_chain_count =" << total_chain << endl;
    cout << "max_chain =" << max_chain << endl;

    /*
    degree_t* degree = (degree_t*)calloc(sizeof(degree),_global_vcount);
    
    #pragma omp parallel for
    for (vid_t vid = 0; vid < _global_vcount; ++vid) {
        degree[vid] = est[vid].delta_degree;
    }
    sort(degree, degree + _global_vcount);
    
    for (vid_t vid = 0; vid < _global_vcount; ++vid) {
        cerr << vid <<"  " << degree[vid] << endl;
    }
    */

}

//estimate the IO read and Write amount and number of chains
template <class T>
void estimate_chain_new(const string& idirname, const string& odirname)
{
    plaingraph_manager_t<dst_id_t> plaingraph_manager; 
    plaingraph_manager.schema(_dir);
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(_global_vcount);    

    propid_t          cf_id = g->get_cfid("friend");
    pgraph_t<dst_id_t>* ugraph = (pgraph_t<dst_id_t>*)g->cf_info[cf_id];
    blog_t<dst_id_t>*     blog = ugraph->blog;
    
    blog->blog_head  += read_idir<T>(idirname, &blog->blog_beg, false);
    
    double start = mywtime();

    index_t marker = blog->blog_head ;
    cout << "End marker = " << blog->blog_head;
    cout << "make graph marker = " << marker << endl;
    if (marker == 0) return;
    
    edge_t* edge = blog->blog_beg;
    estimate_t* est = (estimate_t*)calloc(sizeof(estimate_t), _global_vcount);
    index_t last = 0;
    vid_t src = 0;
    vid_t dst = 0;

    index_t total_used_memory = 0;
    index_t total_chain_count = 0;
    int max_chain = 0;
    vid_t total_hub_vertex = 0;

    
    for (index_t i = 0; i < marker; i +=65536) {
        last = min(i + 65536, marker);
        //do batching
        for (index_t j = i; j < last; ++j) {
            src = edge[j].src_id;
            dst = get_dst(edge[j]);
            est[src].degree++;
            est[dst].degree++;
        }
        index_t used_memory = 0;
        index_t total_chain = 0;

        //Do memory allocation and cleaning
        #pragma omp parallel reduction(+:used_memory, total_chain) reduction(max:max_chain)
        {
        index_t  local_memory = 0;
        degree_t total_degree = 0;
        degree_t new_count = 0;
        #pragma omp for  
        for (vid_t vid = 0; vid < _global_vcount; ++vid) {
            if (est[vid].degree == 0) continue;
            
            //---------------
            total_degree = est[vid].delta_degree + est[vid].degree;
            //Embedded case only
            if (total_degree <= 5) {
                est[vid].delta_degree += est[vid].degree;
                est[vid].degree = 0;
                continue;
            } else if (est[vid].delta_degree <= 5) {//total > 5
                local_memory = UPPER_ALIGN_32B(total_degree);
                
                est[vid].chain_count = 1;
                total_chain++;
                est[vid].space_left = local_memory - total_degree - 4;
                est[vid].delta_degree += est[vid].degree;
                used_memory += local_memory*sizeof(T);
                est[vid].degree = 0;
                continue;
            }
            //----embedded case ends

            //At least 0th chain exists or will be created
            if (est[vid].degree <= est[vid].space_left) {
                est[vid].space_left -= est[vid].degree; 
                est[vid].delta_degree += est[vid].degree;
                est[vid].degree = 0;
            //------hub vertices case
            } else if (total_degree >= 8192 || est[vid].degree >=256) {
                new_count = est[vid].degree - est[vid].space_left;
                local_memory = UPPER_ALIGN_4KB(new_count);
                
                est[vid].chain_count++;
                max_chain = max(max_chain, est[vid].chain_count);
                total_chain++;
                est[vid].space_left = local_memory - new_count - 4;
                est[vid].delta_degree += est[vid].degree;
                used_memory += local_memory*sizeof(T);
                est[vid].degree = 0;
            //------
            } else {
                new_count = est[vid].degree - est[vid].space_left;
                local_memory = UPPER_ALIGN_32B(new_count) ;
                
                est[vid].chain_count++;
                max_chain = max(max_chain, est[vid].chain_count);
                total_chain++;
                est[vid].space_left = local_memory - new_count - 4;
                est[vid].delta_degree += est[vid].degree;
                used_memory += local_memory*sizeof(T);
                est[vid].degree = 0;     
            }
        }
        }
        total_used_memory += used_memory;
        total_chain_count += total_chain;
    }

    double end = mywtime ();
    cout << "Used Memory = " << total_used_memory << endl;
    cout << "Make graph time = " << end - start << endl;
    cout << "total_hub_vertex =" << total_hub_vertex << endl;
    cout << "total_chain_count =" << total_chain_count << endl;
    cout << "max_chain =" << max_chain << endl;
}

//estimate the IO read and Write amount and number of chains
template <class T>
void estimate_IO(const string& idirname, const string& odirname)
{
    plaingraph_manager_t<dst_id_t> plaingraph_manager; 
    plaingraph_manager.schema(_dir);
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(_global_vcount);    
    
    propid_t          cf_id = g->get_cfid("friend");
    pgraph_t<dst_id_t>* ugraph = (pgraph_t<dst_id_t>*)g->cf_info[cf_id];
    blog_t<dst_id_t>*     blog = ugraph->blog;
    
    blog->blog_head  += read_idir<T>(idirname, &blog->blog_beg, false);
    
    double start = mywtime();
    double end;

    index_t marker = blog->blog_head ;
    cout << "End marker = " << blog->blog_head;
    cout << "make graph marker = " << marker << endl;
    if (marker == 0) return;
    
    edge_t* edge = blog->blog_beg;
    estimate_t* est = (estimate_t*)calloc(sizeof(estimate_t), _global_vcount);
    index_t last = 0;
    vid_t src = 0;
    vid_t dst = 0;

    index_t total_memory = (1L<<30L);//4GB
    index_t free_memory = 0;
    index_t cut_off = (4 << 20); //16MB
    //index_t cut_off2 = (64 << 20); //256MB
    index_t wbuf_count = (21); //32MB
    index_t total_used_memory = 0;

    cout << "allocating " << total_memory << endl;
    
    index_t total_io_read = 0;
    index_t total_io_write = 0;
    index_t total_io_write_count = 0;
    index_t total_io_read_count = 0;

    vid_t total_hub_vertex = 0;

    
    for (index_t i = 0; i < marker; i +=65536) {
        last = min(i + 65536, marker);
        //do batching
        for (index_t j = i; j < last; ++j) {
            src = edge[j].src_id;
            dst = get_dst(edge[j]);
            est[src].degree++;
            est[dst].degree++;
        }
        index_t used_memory = 0;

        //Do memory allocation and cleaning
        #pragma omp parallel reduction(+:used_memory) 
        {
        index_t  local_memory = 0;
        degree_t total_degree = 0;
        //degree_t new_count = 0;
        #pragma omp for  
        for (vid_t vid = 0; vid < _global_vcount; ++vid) {
            if (est[vid].degree == 0) continue;
            total_degree = est[vid].delta_degree + est[vid].degree;
            //---------------
            
            if (est[vid].durable_degree == 0) {
              if (total_degree <=7) {
                est[vid].delta_degree += est[vid].degree;
                est[vid].degree = 0;
                continue;
              } else if (est[vid].delta_degree <= 7){
                  local_memory += est[vid].delta_degree;
              }
            }
            local_memory = est[vid].degree + 1;
            used_memory += local_memory;
            est[vid].chain_count++;
            est[vid].delta_degree += est[vid].degree;
            est[vid].degree = 0;
            
            //---------------
            /*
            //Embedded case only
            //if (false)
            if (est[vid].durable_degree == 0) 
            {
                if (total_degree <= 7) {
                    est[vid].delta_degree += est[vid].degree;
                    est[vid].degree = 0;
                    continue;
                } else if (est[vid].delta_degree <= 7) {//total > 7
                    local_memory = UPPER_ALIGN_32B(total_degree + 1);
                    used_memory += local_memory;
                    
                    est[vid].chain_count = 1;
                    est[vid].space_left = local_memory - total_degree - 1;
                    est[vid].delta_degree += est[vid].degree;
                    est[vid].degree = 0;
                    continue;
                }
            }
            

            //At least 0th chain exists or will be created
            if (est[vid].degree <= est[vid].space_left) {
                est[vid].space_left -= est[vid].degree; 
                est[vid].delta_degree += est[vid].degree;
                est[vid].degree = 0;
            } else {
                new_count = est[vid].degree - est[vid].space_left;
                local_memory = UPPER_ALIGN_32B(new_count + 1) ;
                used_memory += local_memory;
                
                est[vid].chain_count++;
                est[vid].space_left = local_memory - new_count - 1;
                est[vid].delta_degree += est[vid].degree;
                est[vid].degree = 0;     
            }
            */
        }
        }
        total_used_memory += used_memory;
        free_memory = total_memory - total_used_memory; //edge count not bytes
        
        //Do durable phase
        if (free_memory > cut_off) {
            continue;
        }

        end = mywtime ();
        cout << "Total used Memory = " << total_used_memory << endl;
        cout << "time = " << end - start << endl;
        
        index_t total_chain_count = 0; 
        int max_chain = 0;
        const int chain = 64;
        index_t total_free_i[chain];
        memset(total_free_i, 0, sizeof(index_t)*chain);
        
        #pragma omp parallel 
        {
            index_t free_i[chain];
            int chain_count = 0;
            memset(free_i, 0, sizeof(index_t)*chain);

            #pragma omp for reduction(+:total_chain_count) reduction(max:max_chain)
            for (vid_t vid = 0; vid < _global_vcount; ++vid) {
                chain_count = est[vid].chain_count;
                max_chain = std::max(chain_count, max_chain);
                total_chain_count += chain_count; 
                if (chain_count < chain) {
                    free_i[chain_count] += est[vid].delta_degree + est[vid].space_left
                                         + chain_count;
                } else {
                    free_i[chain - 1] += est[vid].delta_degree + est[vid].space_left
                                         + chain_count;
                }
            }
            for (int i = 0; i < chain; ++i) {
                __sync_fetch_and_add(total_free_i + i, free_i[i]);
            }
        }
        int freed_memory = 0;
        int freed_chain = 0;
        //----------
        freed_memory= total_used_memory;
        //----------
        /*
        for (int i = chain - 1; i >= freed_chain; --i) {
            freed_memory += total_free_i[i];
        }
        */
        //----------
        /*
        for (int i = chain - 1; i >= 0; --i) {
            freed_memory += total_free_i[i];
            if (freed_memory + free_memory >= cut_off2) {
                freed_chain = i;
                break;
            }
        }*/
        
        cout << "i = " << i;
        cout << " total chain count = " << total_chain_count 
             << " max_chain = " << max_chain << endl;
        cout << "available free = " << free_memory;
        cout << " freeing memory = " << freed_chain << " " << freed_memory << endl;
        
        total_used_memory -= freed_memory;
        
        index_t io_read = 0;
        index_t io_write = 0;
        index_t io_write_count = 0;
        index_t io_read_count = 0;
        vid_t hub_vertex = 0;

        #pragma omp parallel for reduction(+:io_read,io_read_count,io_write,hub_vertex)
        for (vid_t vid = 0; vid < _global_vcount; ++vid) {
            if (est[vid].chain_count < freed_chain) continue;
            
            if (est[vid].durable_degree !=0 && est[vid].type != HUB_TYPE) {
                io_read += est[vid].durable_degree + 2;
                io_read_count++;
                /*
                if (est[vid].durable_degree + est[vid].delta_degree >= HUB_COUNT_TEST) {
                    est[vid].type = HUB_TYPE;
                    io_write += est[vid].durable_degree;
                    ++hub_vertex;
                }*/
            }
            if (est[vid].type == HUB_TYPE) {
                io_write += est[vid].delta_degree; 
            } else {
                io_write += est[vid].delta_degree + est[vid].durable_degree + 2; 
            }

            est[vid].durable_degree += est[vid].delta_degree;
            est[vid].space_left = 0;
            est[vid].delta_degree = 0;
            est[vid].chain_count = 0;
        }
        io_write_count = (io_write >> wbuf_count);
        if (io_write_count == 0) io_write_count = 1;
        
        total_io_read += io_read;
        total_io_read_count += io_read_count;
        total_io_write += io_write;
        total_io_write_count += io_write_count; 
        
        total_hub_vertex += hub_vertex;
        hub_vertex = 0;
        
        end = mywtime ();
        cout << "total_io_read =" << total_io_read << endl; 
        cout << "total_io_read_count =" << total_io_read_count << endl; 
        cout << "total_io_write =" << total_io_write << endl; 
        cout << "total_io_write_count =" << total_io_write_count << endl; 
        cout << "Make graph time = " << end - start << endl;
    }

    end = mywtime ();
    cout << "Used Memory = " << total_used_memory << endl;
    cout << "Make graph time = " << end - start << endl;
    cout << "total_io_read =" << total_io_read << endl; 
    cout << "total_io_read_count =" << total_io_read_count << endl; 
    cout << "total_io_write =" << total_io_write << endl; 
    cout << "total_io_write_count =" << total_io_write_count << endl; 
    cout << "total_hub_vertex =" << total_hub_vertex << endl;
}

//template <class T>
void weighted_dtest0(const string& idir, const string& odir)
{
    string graph_file = idir + "g.bin";
    string action_file = idir + "a.bin"; 
    //string graph_file = idir + "small_basegraph";
    //string action_file = idir + "small_action"; 
    
    int fd = open(graph_file.c_str(), O_RDONLY);
    assert(fd != -1);
    index_t size = fsize(fd);
    int64_t* buf = (int64_t*)malloc(size);
    pread(fd, buf, size, 0);
    int64_t little_endian = buf[0];
    assert(little_endian == 0x1234ABCD);

    int64_t nv = buf[1]; 
    int64_t ne = buf[2];
    int64_t* v_offset = buf + 3;
    int64_t* index = v_offset + nv + 1; //+1 for offset in CSR 
    int64_t* weight = index + ne;
    lite_edge_t* nebrs = (lite_edge_t*)malloc(ne*sizeof(lite_edge_t));

    for (int64_t i = 0; i < ne; i++) {
        set_sid(nebrs[i], index[i]);
        nebrs[i].second.value = weight[i];  
    }

    //Create number of vertex
    _global_vcount = nv;
    plaingraph_manager_t<lite_edge_t> manager;
    manager.schema(_dir);
    manager.setup_graph(_global_vcount);
    
    //Do ingestion
    /*
     * We are doing something to be compliant with stinger test
     * 1. the graph file is CSR file, we don't expect that
     * 2. The graph already stores the reverse edge, we don't expect that
     * 3. Stinger does something directly with the memory that is allocated 
     *      for graph read, for initial graph building.
     */
    pgraph_t<lite_edge_t>*    graph = (pgraph_t<lite_edge_t>*)manager.get_plaingraph();
    blog_t<lite_edge_t>*       blog = graph->blog;
    onegraph_t<lite_edge_t>* sgraph = graph->sgraph[0];
    
    double start = mywtime();
    
    #pragma omp parallel num_threads(THD_COUNT)
    {
        index_t   nebr_count = 0;

        #pragma omp for
        for (int64_t v = 0; v < nv; ++v) {
            nebr_count = v_offset[v+1] - v_offset[v];
            #ifndef BULK
            sgraph->increment_count_noatomic(v, nebr_count);
            #else
            sgraph->increment_count(v, nebr_count);
            #endif
        }
#ifdef BULK
        sgraph->setup_adjlist();
        #pragma omp barrier
#endif

        #pragma omp for
        for (int64_t v = 0; v < nv; ++v) {
            nebr_count = v_offset[v+1] - v_offset[v];
            if (nebr_count) {
                sgraph->add_nebr_bulk(v, nebrs + v_offset[v], nebr_count);
            }
        }
    }
    
    //faking a snapshot creation.
    index_t snap_marker = 0;
    blog->blog_head += ne;
    graph->create_marker(0);//latest head will be chosen
    if (eOK == graph->move_marker(snap_marker)) {
        graph->new_snapshot(snap_marker, snap_marker);
        graph->update_marker();
    }
    double end = mywtime();
    cout << "Make graph time = " << end - start << endl;

    free(buf);
    free(nebrs);

    //-------Run bfs and PR------
    snap_t<lite_edge_t>* snaph = create_static_view(graph, STALE_MASK|V_CENTRIC);
    
    uint8_t* level_array = (uint8_t*) calloc(_global_vcount, sizeof(uint8_t));
    mem_bfs<lite_edge_t>(snaph, level_array, 0);
    free(level_array);
    mem_pagerank_epsilon<lite_edge_t>(snaph, 1e-8);

    
    //-------Graph Updates-------
    g->create_threads(true, false);
    
    int fd1 = open(action_file.c_str(), O_RDONLY);
    assert(fd1 != -1);
    index_t size1 = fsize(fd1);
    int64_t* buf1 = (int64_t*)malloc(size1);
    pread(fd1, buf1, size1, 0);
    int64_t little_endian1 = buf1[0];
    assert(little_endian1 == 0x1234ABCD);

    int64_t na = buf1[1];
    edge_t* edges = (edge_t*)(buf1 + 2);

    //Do ingestion
    start = mywtime();
    int64_t del_count = 0;
    
    int64_t src, dst;
    edgeT_t<lite_edge_t> edge;

    for (int64_t i = 0; i < na; i++) {
        src = edges[i].src_id;
        edge.dst_id.second.value = 1;
        dst = get_dst(edges[i]);
        if (src >= 0) {
            edge.src_id = src;
            set_dst(edge, dst);
            graph->batch_edge(edge);
        } else {
            edge.src_id = DEL_SID(-src);
            set_dst(edge, DEL_SID(-dst));
            graph->batch_edge(edge);
            ++del_count;
        }
    }
    
    end = mywtime();
    cout << "batch Edge time = " << end - start << endl;
    g->waitfor_archive(); 
    //graph->create_marker(0);
    //graph->waitfor_archive();
    end = mywtime();
    cout << "Make graph time = " << end - start << endl;
    cout << "no of actions " << na << endl;
    cout << "del_count "<<del_count << endl;
    //----
    //graph->store_graph_baseline();
    //----
    

    //-------Run bfs and PR------
    snaph->update_view();
    
    level_array = (uint8_t*) calloc(_global_vcount, sizeof(uint8_t));
    mem_bfs_simple<lite_edge_t>(snaph, level_array, 0);
    free(level_array);
    
    start = mywtime();
    graph->compress_graph_baseline();
    end = mywtime();
    cout << "Compress time = " << end - start << endl;
    
    level_array = (uint8_t*) calloc(_global_vcount, sizeof(uint8_t));
    mem_bfs<lite_edge_t>(snaph, level_array, 0);
    free(level_array);
    //manager.run_bfs(0);
    return;
}

template <class T>
void prior_snap_test(const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    g->read_graph_baseline();
    pgraph_t<T>* pgraph = manager.get_plaingraph();
    //manager.run_bfs();
    
    //Run using prior static view.
    prior_snap_t<T>* snaph = create_prior_static_view(pgraph, 0, 33554432);
    uint8_t* level_array = (uint8_t*)calloc(sizeof(uint8_t), snaph->get_vcount());
    mem_bfs_simple(snaph, level_array, 1);

    return ;
}

//recover from durable adj list
template <class T>
void recover_test(const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    g->read_graph_baseline();
    manager.run_bfs();
    return ;
}

//recover from durable edge log
template <class T>
void recover_test0(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    //do some setup for plain graphs
    manager.setup_graph(_global_vcount);    
    manager.recover_graph_adj(idir, odir);
    
    //Run BFS
    manager.run_bfs();
}

template <class T>
void split_files(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    //do some setup for plain graphs
    manager.setup_graph(_global_vcount);    
    manager.split_files(idir, odir);    
}

template <class T>
void llama_test(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    //do some setup for plain graphs
    manager.setup_graph(_global_vcount);
    manager.prep_graph(idir, odir);
    manager.run_bfs();
    manager.run_bfs();
    manager.run_bfs();
    manager.run_pr();
    manager.run_pr();
    manager.run_pr();
}

template <class T>
void ingestion_full(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    //do some setup for plain graphs
    manager.setup_graph_vert_nocreate(_global_vcount); 
    manager.prep_graph(idir, odir); 
    manager.run_bfs();    
    //g->store_graph_baseline();
    //cout << "stroing done" << endl;
}

template <class T>
void test_ingestion_full(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    //do some setup for plain graphs
    manager.setup_graph_vert_nocreate(_global_vcount);    
    manager.prep_graph2(idir, odir); 
    manager.run_bfs();    
    //g->store_graph_baseline();
    cout << "stroing done" << endl;
}

/*
void plain_test6(const string& odir)
{
    plaingraph_manager.schema(_dir);
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(_global_vcount);    
    
    g->read_graph_baseline();
    
    //call mem_bfs
    propid_t cf_id = g->get_cfid("friend");
    ugraph_t* ugraph = (ugraph_t*)g->cf_info[cf_id];
    vert_table_t<dst_id_t>* graph = ugraph->sgraph[0]->get_begpos();
    index_t edge_count = (_global_vcount << 5);
    uint8_t* level_array = (uint8_t*) calloc(_global_vcount, sizeof(uint8_t));
    
    snap_bfs<dst_id_t>(graph, graph, _global_vcount, edge_count, level_array, 1, 1);
    return ;
}
*/

void paper_test_chain_bfs(const string& idir, const string& odir)
{
    plaingraph_manager_t<dst_id_t> plaingraph_manager; 
    plaingraph_manager.schema(_dir);
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(_global_vcount);    
    plaingraph_manager.prep_graph_adj(idir, odir);
    
    for (int i = 0; i < 10; i++) {
        plaingraph_manager.run_bfs();
    }
}
void paper_test_pr_chain(const string& idir, const string& odir)
{
    plaingraph_manager_t<dst_id_t> plaingraph_manager; 
    plaingraph_manager.schema(_dir);
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(_global_vcount);    
    plaingraph_manager.prep_graph_adj(idir, odir);
    
    for (int i = 0; i < 10; i++) {
        plaingraph_manager.run_pr();
    }
}

void paper_test_pr(const string& idir, const string& odir)
{
    plaingraph_manager_t<dst_id_t> plaingraph_manager; 
    plaingraph_manager.schema(_dir);
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(_global_vcount);    
    plaingraph_manager.prep_graph_adj(idir, odir);
    
    plaingraph_manager.run_pr();
}

void paper_test_hop1_chain(const string& idir, const string& odir)
{
    plaingraph_manager_t<dst_id_t> plaingraph_manager; 
    plaingraph_manager.schema(_dir);
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(_global_vcount);    
    plaingraph_manager.prep_graph_adj(idir, odir);
    
    plaingraph_manager.run_1hop();
}

void paper_test_hop1(const string& idir, const string& odir)
{
    plaingraph_manager_t<dst_id_t> plaingraph_manager; 
    plaingraph_manager.schema(_dir);
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(_global_vcount);    
    plaingraph_manager.prep_graph_adj(idir, odir);
    
    plaingraph_manager.run_1hop();
}

void paper_test_hop2_chain(const string& idir, const string& odir)
{
    plaingraph_manager_t<dst_id_t> plaingraph_manager; 
    plaingraph_manager.schema(_dir);
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(_global_vcount);    
    plaingraph_manager.prep_graph_adj(idir, odir);
    
    plaingraph_manager.run_2hop();
}

void paper_test_hop2(const string& idir, const string& odir)
{
    plaingraph_manager_t<dst_id_t> plaingraph_manager; 
    plaingraph_manager.schema(_dir);
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(_global_vcount);    
    plaingraph_manager.prep_graph_adj(idir, odir);
    
    plaingraph_manager.run_2hop();
}

template <class T>
void gen_kickstarter_files(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    manager.setup_graph(_global_vcount);    
    manager.prep_graph_mix(idir, odir);    
    //manager.prep_graph_adj(idir, odir);    
    manager.run_bfs(1); 

    pgraph_t<T>* pgraph1 = (pgraph_t<T>*)manager.get_plaingraph();
    snap_t<T>* snaph = create_static_view(pgraph1, STALE_MASK|V_CENTRIC);
    degree_t nebr_count = 0;
    degree_t prior_sz = 65536;
    T* local_adjlist = (T*)malloc(prior_sz*sizeof(T));
    index_t  offset = 0;
    sid_t sid = 0;
    
    assert(_global_vcount == snaph->get_vcount());
    
    string vtfile = odir + "inputGraph.adj"; 
	FILE* vtf = fopen(vtfile.c_str(), "w");
    assert(vtf != 0);
    char text[256];
    
    index_t edge_count = (2L << residue);
    sprintf(text,"AdjacencyGraph\n%u\n%lu\n", _global_vcount, edge_count);
    fwrite(text, sizeof(char), strlen(text), vtf);
	
    for (vid_t v = 0; v < _global_vcount; v++) {
        nebr_count = snaph->get_degree_out(v);
        sprintf(text,"%lu\n", offset);
        fwrite(text, sizeof(char), strlen(text), vtf);
        //cerr << offset  << endl;
        offset += nebr_count;
    }

	for (vid_t v = 0; v < _global_vcount; v++) {
        nebr_count = snaph->get_degree_out(v);
        if (nebr_count == 0) {
            continue;
        } else if (nebr_count > prior_sz) {
            prior_sz = nebr_count;
            free(local_adjlist);
            local_adjlist = (T*)malloc(prior_sz*sizeof(T));
        }
        snaph->get_nebrs_out(v, local_adjlist);
        for (degree_t i = 0; i < nebr_count; ++i) {
            sid = get_sid(local_adjlist[i]);
            sprintf(text,"%u\n", sid);
            fwrite(text, sizeof(char), strlen(text), vtf);
            //cerr << sid << endl;
        }
    }
    fclose(vtf);
    string etfile = odir + "additionsFile.snap";
	FILE* etf = fopen(etfile.c_str(), "w");
    vid_t src, dst;
    edgeT_t<T>* edges;
    index_t count = snaph->get_nonarchived_edges(edges);
    
    for (index_t i = 0; i < count; ++i) {
        src = edges[i].src_id;
        dst = get_dst(edges+i);
        sprintf(text,"%u %u\n", src, dst);
        fwrite(text, sizeof(char), strlen(text), etf);
        //cerr << src << " " << dst << endl;
    }
    fclose(etf);
}

template <class T>
void test_logging(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    //do some setup for plain graphs
    manager.setup_graph(_global_vcount);    
    manager.prep_graph_edgelog2(idir, odir);
    
    //Run BFS
    for (int i = 0; i < 1; i++){
        manager.run_bfs();
    }
}

template <class T>
void test_mix(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    //do some setup for plain graphs
    manager.setup_graph(_global_vcount);    
    
    manager.prep_graph_mix(idir, odir);
    
    //Run BFS
    for (int i = 0; i < 1; i++){
        manager.run_bfs();
    }

    //g->store_graph_baseline();
    /*
    //Run PageRank
    for (int i = 0; i < 5; i++){
        manager.run_pr();
        manager.run_pr_simple();
    }
    
    //Run 1-HOP query
    for (int i = 0; i < 1; i++){
        manager.run_1hop();
    }*/
}

template <class T>
void test_archive(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    //do some setup for plain graphs
    manager.setup_graph(_global_vcount);    
    
    manager.prep_graph_adj(idir, odir);
    //g->store_graph_baseline();
    
    //Run BFS
    for (int i = 0; i < 1; i++){
        manager.run_bfs();
    }

    //Run PageRank
    for (int i = 0; i < 1; i++){
        manager.run_pr();
        manager.run_pr_simple();
    }
    
    //Run 1-HOP query
    for (int i = 0; i < 1; i++){
        manager.run_1hop();
    }
}

void test_user1(const string& idir, const string& odir)
{
    plaingraph_manager_t<weight_sid_t> manager;
    manager.schema(_dir);
    //do some setup for plain graphs
    manager.setup_graph(_global_vcount);    
    g->create_threads(true, false);
    
    p_ugraph_t* pgraph = (p_ugraph_t*) g->get_sgraph(1);
    
    vid_t v0 = 0;
    vid_t v1= 1;
    vid_t v2 = 2;
    status_t status;

    // insert two edges
    weight_edge_t edge;
    set_src(edge, v0); // src
    set_dst(edge, v1); // dst
    set_weight_int(edge, 102);// weight
    status = pgraph->batch_edge(edge);
    cout << "insert " << v0 << " - " << v1 << ": " << 102 << "\n";

    set_src(edge, v0); // src
    set_dst(edge, v2); // dst
    set_weight_int(edge, 103); // weight
    status = pgraph->batch_edge(edge);
    cout << "insert " << v0 << " - " << v2 << ": " << 103 << "\n";

    g->waitfor_archive();
    dump();

    set_src(edge, DEL_SID(v0)); // src
    set_dst(edge, v2); // dst
    set_weight_int(edge, 104); // weight, not required
    status = pgraph->batch_edge(edge);
    cout << "remove " << v0 << " - " << v2 << ": " << 104 << "\n";

    g->waitfor_archive();
    dump();

    set_src(edge, v0); // src
    set_dst(edge, v2); // dst
    set_weight_int(edge, 1010); // weight
    status = pgraph->batch_edge(edge);
    cout << "insert " << v0 << " - " << v2 << ": " << 1010 << "\n";

    g->waitfor_archive();
    dump();
}

void test_user2(const string& idir, const string& odir)
{
    plaingraph_manager_t<weight_sid_t> manager;
    manager.schema(_dir);
    //do some setup for plain graphs
    manager.setup_graph(_global_vcount);    
    g->create_threads(true, false);
    
    p_ugraph_t* pgraph = (p_ugraph_t*) g->get_sgraph(1);
    
    vid_t v0 = 0;
    vid_t v1= 1;
    vid_t v2 = 2;
    status_t status;

    // insert two edges
    weight_edge_t edge;
    set_src(edge, v0); // src
    set_dst(edge, v1); // dst
    set_weight_int(edge, 102);// weight
    status = pgraph->batch_edge(edge);
    cout << "insert " << v0 << " - " << v1 << ": " << 102 << "\n";

    set_src(edge, v0); // src
    set_dst(edge, v2); // dst
    set_weight_int(edge, 103); // weight
    status = pgraph->batch_edge(edge);
    cout << "insert " << v0 << " - " << v2 << ": " << 103 << "\n";

    //g->waitfor_archive();
    dump_simple();

    set_src(edge, DEL_SID(v0)); // src
    set_dst(edge, v2); // dst
    set_weight_int(edge, 104); // weight, not required
    status = pgraph->batch_edge(edge);
    cout << "remove " << v0 << " - " << v2 << ": " << 104 << "\n";

    //g->waitfor_archive();
    dump_simple();

    set_src(edge, v0); // src
    set_dst(edge, v2); // dst
    set_weight_int(edge, 1010); // weight
    status = pgraph->batch_edge(edge);
    cout << "insert " << v0 << " - " << v2 << ": " << 1010 << "\n";

    //g->waitfor_archive();
    dump_simple();
}

template <class T>
void test_ingestion(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    
    manager.schema(_dir);
    //do some setup for plain graphs
    manager.setup_graph(_global_vcount);    
    manager.prep_graph2(idir, odir);
    manager.run_bfs();
    
    /*
    pgraph_t<T>* pgraph = manager.get_plaingraph();
    double start = mywtime();
    pgraph->compress_graph_baseline();
    double end = mywtime();
    cout << "Compress time = " << end - start << endl;
    manager.run_bfs();
    */
}

template <class T>
void test_ingestion_snb(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    
    manager.schema(_dir);
    //do some setup for plain graphs
    manager.setup_graph(_global_vcount, eSNB);    
    manager.prep_graph2(idir, odir);
    manager.run_bfs_snb();
}

template <class T>
void ingestion(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    //do some setup for plain graphs
    manager.setup_graph(_global_vcount);    
    manager.prep_graph(idir, odir); 
    manager.run_bfs();
    //g->store_graph_baseline();    
}

void stream_netflow_aggregation(const string& idir, const string& odir)
{
    plaingraph_manager_t<netflow_dst_t> manager;
    manager.schema(_dir);
    //do some setup for plain graphs
    manager.setup_graph(_global_vcount);
    pgraph_t<netflow_dst_t>* pgraph = manager.get_plaingraph();
    
    stream_t<netflow_dst_t>* streamh = reg_stream_view(pgraph, do_stream_netflow_aggr,
                                                       E_CENTRIC|C_THREAD);
    netflow_post_reg(streamh); 
    manager.prep_graph_and_compute(idir, odir, streamh); 
    //netflow_finalize(streamh); 
}

void test_stream_wcc(const string& idir, const string& odir)
{
    plaingraph_manager_t<dst_id_t> manager; 
    manager.schema(_dir);
    //do some setup for plain graphs
    manager.setup_graph(_global_vcount);    
    pgraph_t<dst_id_t>* pgraph = manager.get_plaingraph();
    
    /*
    stream_t<dst_id_t>* streamh = reg_stream_view(pgraph, stream_wcc, E_CENTRIC);
    wcc_post_reg(streamh); 
    manager.prep_graph_and_compute(idir, odir, streamh); 
    print_wcc_summary(streamh);
    */
    
    stream_t<dst_id_t>* streamh = reg_stream_view(pgraph, do_stream_wcc, E_CENTRIC|C_THREAD);
    manager.prep_graph_edgelog(idir, odir);
    void* ret;
    pthread_join(streamh->thread, &ret);
}

template <class T>
void multi_stream_bfs(const string& idir, const string& odir,
                     typename callback<T>::sfunc stream_fn, int count)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    //do some setup for plain graphs
    manager.setup_graph(_global_vcount);    
    
    pgraph_t<T>* pgraph = manager.get_plaingraph();
    sstream_t<T>** sstreamh = (sstream_t<T>**)malloc(sizeof(sstream_t<T>*)*count);
    
    for (int i = 0; i < count; ++i) {
        sstreamh[i] = reg_sstream_view(pgraph, stream_fn, 
                            STALE_MASK|V_CENTRIC|C_THREAD, (void*)(i+1));
    }
    
    //CorePin(0);
    manager.prep_graph(idir, odir);
    for (int i = 0; i < count; ++i) {
        void* ret;
        pthread_join(sstreamh[i]->thread, &ret);
    }
}

template <class T>
void test_serial_stream(const string& idir, const string& odir,
                     typename callback<T>::sfunc stream_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir);
    manager.setup_graph(_global_vcount);    
    pgraph_t<T>* graph = manager.get_plaingraph();
    
    sstream_t<T>* sstreamh = reg_sstream_view(graph, stream_fn, STALE_MASK|V_CENTRIC|C_THREAD);
    
    manager.prep_graph_edgelog(idir, odir);
    
    void* ret;
    pthread_join(sstreamh->thread, &ret);
}

void plain_test(vid_t v_count1, const string& idir, const string& odir, int job)
{
    switch (job) {
        //plaingrah benchmark testing    
        case 0: 
            test_ingestion<dst_id_t>(idir, odir);
            //test_ingestion<snb_t>(idir, odir);
            break;
        case 1:
            test_logging<dst_id_t>(idir, odir);
            break;
        case 2: 
            test_archive<dst_id_t>(idir, odir);
            break;
        case 3://leave some in the edge format
            test_mix<dst_id_t>(idir, odir);
            break;
        case 4://recover from durable edge log
            recover_test0<dst_id_t>(idir, odir);
            break;
        case 5:
            recover_test<dst_id_t>(odir);
            break;
        case 6:
            prior_snap_test<dst_id_t>(odir);
            break;
        case 7://SNB
            test_ingestion_snb<dst_id_t>(idir, odir);
            break;
        //plain graph in text format with ID. 
        case 9: //Not for performance
            ingestion<dst_id_t>(idir, odir);
            break;

        //netflow graph testing
        case 10:
            test_ingestion<netflow_dst_t>(idir, odir);
            break;
        case 11:
            test_logging<netflow_dst_t>(idir, odir);
            break;
        case 12: 
            test_archive<netflow_dst_t>(idir, odir);
            break;
        case 13:
            recover_test0<netflow_dst_t>(idir, odir);
            break;
        case 14:
            recover_test<netflow_dst_t>(odir);
            break;
        case 15:
            prior_snap_test<netflow_dst_t>(odir);
            break;
        case 16://text to our format
            test_ingestion_full<netflow_dst_t>(idir, odir);
            break;
        case 17://text to our format, not for performance
            ingestion_full<netflow_dst_t>(idir, odir);
            break;
        
        case 20: 
            test_ingestion<weight_sid_t>(idir, odir);
            break;
        case 21: 
            test_logging<weight_sid_t>(idir, odir);
            break;
        case 22: 
            test_archive<weight_sid_t>(idir, odir);
            break;
        case 23://llama
            llama_test<weight_sid_t>(idir, odir);
            break;
        case 24:
            //stinger test
            weighted_dtest0(idir, odir);
            break;
        case 25:
            split_files<weight_sid_t>(idir, odir);
            break;
        case 26://generate kickstarter files
            gen_kickstarter_files<dst_id_t>(idir, odir);
            break;
        case 27://text to binary file not for performance
            //ingestion_full<netflow_dst_t>(idir, odir, parsefile_to_bin);
            break;
        
        case 30:
            multi_stream_bfs<dst_id_t>(idir, odir, stream_bfs, residue);
            break;
        case 31:
            test_serial_stream<dst_id_t>(idir, odir, stream_serial_bfs);
            break;
        case 32:
            stream_netflow_aggregation(idir, odir);
            break;
        case 33:
            test_stream_wcc(idir, odir);
            break;

	case 40:
	    test_user1(idir, odir);
	    break;
	case 41:
	    test_user2(idir, odir);
	    break;
        case 50:
            paper_test_pr(idir, odir);
            break;
        case 51:
            paper_test_hop1(idir, odir);
            break;
        case 52:
            paper_test_hop2(idir, odir);
            break;
        case 53:
            paper_test_chain_bfs(idir, odir);
            break;
        case 54:
            paper_test_pr_chain(idir, odir);
            break;
        case 55:
            paper_test_hop1_chain(idir, odir);
            break;
        case 56:
            paper_test_hop2_chain(idir, odir);
            break;
        case 57:
            estimate_chain_new<dst_id_t>(idir, odir);
            break; 
        case 58:
            estimate_chain<dst_id_t>(idir, odir);
            break; 
        case 59:
            estimate_IO<dst_id_t>(idir, odir);
            break; 
        default:
            break;
    }
}
