#include <iostream>

#include "plain_to_edge.h"
#include "all.h"
#include "util.h"

/*
#include "mem_iterative_analytics.h"
#include "snap_iterative_analytics.h"
#include "ext_iterative_analytics.h"
*/

#include "stream_analytics.h"
#include "sstream_analytics.h"
#include "scopy_analytics.h"
#include "scopy1d_analytics.h"
#include "scopy2d_analytics.h"


using namespace std;

extern index_t residue;

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

vid_t v_count = 0;
plaingraph_manager_t<sid_t> plaingraph_manager; 

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
    plaingraph_manager.schema_plaingraph();
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(v_count);    
    
    propid_t          cf_id = g->get_cfid("friend");
    pgraph_t<sid_t>* ugraph = (pgraph_t<sid_t>*)g->cf_info[cf_id];
    blog_t<sid_t>*     blog = ugraph->blog;
    
    blog->blog_head  += read_idir<T>(idirname, &blog->blog_beg, false);
    
    double start = mywtime();

    index_t marker = blog->blog_head ;
    cout << "End marker = " << blog->blog_head;
    cout << "make graph marker = " << marker << endl;
    if (marker == 0) return;
    
    edge_t* edge = blog->blog_beg;
    estimate_t* est = (estimate_t*)calloc(sizeof(estimate_t), v_count);
    index_t last = 0;
    vid_t src = 0;
    vid_t dst = 0;

    index_t total_used_memory = 0;
    index_t total_chain = 0;
    int max_chain = 0;
    vid_t total_hub_vertex = 0;
    //index_t batching_size = 65536;
    index_t batching_size = marker;
    
    for (index_t i = 0; i < marker; i +=batching_size) {
        last = min(i + batching_size, marker);
        //do batching
        for (index_t j = i; j < last; ++j) {
            src = edge[j].src_id;
            dst = edge[j].dst_id;
            est[src].degree++;
            est[dst].degree++;
        }

        index_t used_memory = 0;
        //Do memory allocation and cleaning
        #pragma omp parallel reduction(+:used_memory, total_chain) reduction(max:max_chain)
        {
        index_t  local_memory = 0;
        #pragma omp for  
        for (vid_t vid = 0; vid < v_count; ++vid) {
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
    degree_t* degree = (degree_t*)calloc(sizeof(degree),v_count);
    
    #pragma omp parallel for
    for (vid_t vid = 0; vid < v_count; ++vid) {
        degree[vid] = est[vid].delta_degree;
    }
    sort(degree, degree + v_count);
    
    for (vid_t vid = 0; vid < v_count; ++vid) {
        cerr << vid <<"  " << degree[vid] << endl;
    }
    */

}

//estimate the IO read and Write amount and number of chains
template <class T>
void estimate_chain_new(const string& idirname, const string& odirname)
{
    plaingraph_manager.schema_plaingraph();
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(v_count);    

    propid_t          cf_id = g->get_cfid("friend");
    pgraph_t<sid_t>* ugraph = (pgraph_t<sid_t>*)g->cf_info[cf_id];
    blog_t<sid_t>*     blog = ugraph->blog;
    
    blog->blog_head  += read_idir<T>(idirname, &blog->blog_beg, false);
    
    double start = mywtime();

    index_t marker = blog->blog_head ;
    cout << "End marker = " << blog->blog_head;
    cout << "make graph marker = " << marker << endl;
    if (marker == 0) return;
    
    edge_t* edge = blog->blog_beg;
    estimate_t* est = (estimate_t*)calloc(sizeof(estimate_t), v_count);
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
            dst = edge[j].dst_id;
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
        for (vid_t vid = 0; vid < v_count; ++vid) {
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
    plaingraph_manager.schema_plaingraph();
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(v_count);    
    
    propid_t          cf_id = g->get_cfid("friend");
    pgraph_t<sid_t>* ugraph = (pgraph_t<sid_t>*)g->cf_info[cf_id];
    blog_t<sid_t>*     blog = ugraph->blog;
    
    blog->blog_head  += read_idir<T>(idirname, &blog->blog_beg, false);
    
    double start = mywtime();
    double end;

    index_t marker = blog->blog_head ;
    cout << "End marker = " << blog->blog_head;
    cout << "make graph marker = " << marker << endl;
    if (marker == 0) return;
    
    edge_t* edge = blog->blog_beg;
    estimate_t* est = (estimate_t*)calloc(sizeof(estimate_t), v_count);
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
            dst = edge[j].dst_id;
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
        for (vid_t vid = 0; vid < v_count; ++vid) {
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
            for (vid_t vid = 0; vid < v_count; ++vid) {
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
        for (vid_t vid = 0; vid < v_count; ++vid) {
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
        nebrs[i].first = index[i];
        nebrs[i].second.value = weight[i];  
    }

    //Create number of vertex
    v_count = nv;
    plaingraph_manager_t<lite_edge_t> manager;
    manager.schema_plaingraph();
    manager.setup_graph(v_count);
    
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
    
    uint8_t* level_array = (uint8_t*) calloc(v_count, sizeof(uint8_t));
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
        dst = edges[i].dst_id;
        if (src >= 0) {
            edge.src_id = src;
            edge.dst_id.first = dst;
            graph->batch_edge(edge);
        } else {
            edge.src_id = DEL_SID(-src);
            edge.dst_id.first = DEL_SID(-dst);
            graph->batch_edge(edge);
            ++del_count;
        }
    }
    
    end = mywtime();
    cout << "batch Edge time = " << end - start << endl;
    
    graph->create_marker(0);
    graph->waitfor_archive();
    end = mywtime();
    cout << "Make graph time = " << end - start << endl;
    cout << "no of actions " << na << endl;
    cout << "del_count "<<del_count << endl;
    //----
    //graph->store_graph_baseline();
    //----
    

    //-------Run bfs and PR------
    snaph->update_view();
    
    level_array = (uint8_t*) calloc(v_count, sizeof(uint8_t));
    mem_bfs_simple<lite_edge_t>(snaph, level_array, 0);
    free(level_array);
    
    start = mywtime();
    graph->compress_graph_baseline();
    end = mywtime();
    cout << "Compress time = " << end - start << endl;
    
    level_array = (uint8_t*) calloc(v_count, sizeof(uint8_t));
    mem_bfs<lite_edge_t>(snaph, level_array, 0);
    free(level_array);
    //manager.run_bfs(0);
    return;
}

template <class T>
void prior_snap_testu(const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    g->read_graph_baseline();
    pgraph_t<T>* pgraph = manager.get_plaingraph();
    //manager.run_bfs();
    
    //Run using prior static view.
    prior_snap_t<T>* snaph = create_prior_static_view(pgraph, 0, 33554432);
    uint8_t* level_array = (uint8_t*)calloc(sizeof(uint8_t), snaph->get_vcount());
    mem_bfs_simple(snaph, level_array, 1);

    return ;
}

template <class T>
void prior_snap_testuni(const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraphuni();
    g->read_graph_baseline();
    //manager.run_bfs();
    
    //Run using prior static view.
    pgraph_t<T>* pgraph = manager.get_plaingraph();
    prior_snap_t<T>* snaph = create_prior_static_view(pgraph, 0, 33554432);
    uint8_t* level_array = (uint8_t*)calloc(sizeof(uint8_t), snaph->get_vcount());
    mem_bfs_simple(snaph, level_array, 1);

    return ;
}

template <class T>
void recover_testu(const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    g->read_graph_baseline();
    manager.run_bfs();

    return ;
}

template <class T>
void recover_testuni(const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraphuni();
    g->read_graph_baseline();
    manager.run_bfs();

    return ;
}

template <class T>
void split_files(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph_memory(v_count);    
    manager.split_files(idir, odir);    
}

template <class T>
void llama_test(const string& idir, const string& odir,
                typename callback<T>::parse_fn_t parsefile_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph(v_count);
    g->create_threads(true, false);
    manager.prep_graph_fromtext(idir, odir, parsefile_fn);
    manager.run_bfs();
    manager.run_pr();    
}

template <class T>
void llama_testd(const string& idir, const string& odir,
                typename callback<T>::parse_fn_t parsefile_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraphd();
    //do some setup for plain graphs
    manager.setup_graph(v_count);
    g->create_threads(true, false);
    manager.prep_graph_fromtext(idir, odir, parsefile_fn);
    manager.run_bfs();
    manager.run_bfs();
    manager.run_bfs();
    manager.run_pr();
    manager.run_pr();
    manager.run_pr();
}

template <class T>
void ingestion_fulluni(const string& idir, const string& odir,
                     typename callback<T>::parse_fn_t parsefile_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraphuni();
    //do some setup for plain graphs
    manager.setup_graph_vert_nocreate(v_count);
    g->create_threads(true, false);
    manager.prep_graph_fromtext(idir, odir, parsefile_fn); 
    //manager.run_bfsd();    
    //g->store_graph_baseline();
    cout << "stroing done" << endl;
    
    /*
    //Run using prior static view.
    prior_snap_t<T>* snaph;
    manager.create_prior_static_view(&snaph, 0, 33554432);
    uint8_t* level_array = (uint8_t*)calloc(sizeof(uint8_t), snaph->v_count);
    mem_bfs_simple(snaph, level_array, 1);
    */
}

template <class T>
void ingestion_fulld(const string& idir, const string& odir,
                     typename callback<T>::parse_fn_t parsefile_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraphd();
    //do some setup for plain graphs
    manager.setup_graph_vert_nocreate(v_count); 
    g->create_threads(true, false);   
    manager.prep_graph_fromtext(idir, odir, parsefile_fn); 
    manager.run_bfs();    
    //g->store_graph_baseline();
    //cout << "stroing done" << endl;
}

template <class T>
void test_ingestion_fulld(const string& idir, const string& odir,
                     typename callback<T>::parse_fn2_t parsebuf_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraphd();
    //do some setup for plain graphs
    manager.setup_graph_vert_nocreate(v_count);    
    g->create_threads(true, false);   
    manager.prep_graph_fromtext2(idir, odir, parsebuf_fn); 
    manager.run_bfs();    
    //g->store_graph_baseline();
    cout << "stroing done" << endl;
}

/*
void plain_test6(const string& odir)
{
    plaingraph_manager.schema_plaingraph();
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(v_count);    
    
    g->read_graph_baseline();
    
    //call mem_bfs
    propid_t cf_id = g->get_cfid("friend");
    ugraph_t* ugraph = (ugraph_t*)g->cf_info[cf_id];
    vert_table_t<sid_t>* graph = ugraph->sgraph[0]->get_begpos();
    index_t edge_count = (v_count << 5);
    uint8_t* level_array = (uint8_t*) calloc(v_count, sizeof(uint8_t));
    
    snap_bfs<sid_t>(graph, graph, v_count, edge_count, level_array, 1, 1);
    return ;
}
*/

void paper_test_chain_bfs(const string& idir, const string& odir)
{
    plaingraph_manager.schema_plaingraph();
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(v_count);    
    plaingraph_manager.prep_graph_adj(idir, odir);
    
    for (int i = 0; i < 10; i++) {
        plaingraph_manager.run_bfs();
    }
}
void paper_test_pr_chain(const string& idir, const string& odir)
{
    plaingraph_manager.schema_plaingraph();
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(v_count);    
    plaingraph_manager.prep_graph_adj(idir, odir);
    
    for (int i = 0; i < 10; i++) {
        plaingraph_manager.run_pr();
    }
}

void paper_test_pr(const string& idir, const string& odir)
{
    plaingraph_manager.schema_plaingraph();
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(v_count);    
    plaingraph_manager.prep_graph_adj(idir, odir);
    
    plaingraph_manager.run_pr();
}

void paper_test_hop1_chain(const string& idir, const string& odir)
{
    plaingraph_manager.schema_plaingraph();
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(v_count);    
    plaingraph_manager.prep_graph_adj(idir, odir);
    
    plaingraph_manager.run_1hop();
}

void paper_test_hop1(const string& idir, const string& odir)
{
    plaingraph_manager.schema_plaingraph();
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(v_count);    
    plaingraph_manager.prep_graph_adj(idir, odir);
    
    plaingraph_manager.run_1hop();
}

void paper_test_hop2_chain(const string& idir, const string& odir)
{
    plaingraph_manager.schema_plaingraph();
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(v_count);    
    plaingraph_manager.prep_graph_adj(idir, odir);
    
    plaingraph_manager.run_2hop();
}

void paper_test_hop2(const string& idir, const string& odir)
{
    plaingraph_manager.schema_plaingraph();
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(v_count);    
    plaingraph_manager.prep_graph_adj(idir, odir);
    
    plaingraph_manager.run_2hop();
}


template <class T>
void test_archived(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraphd();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    manager.prep_graph_adj(idir, odir);    
    
    //Run BFS
    for (int i = 0; i < 1; i++){
        manager.run_bfs();
    }
    /*
    //Run PageRank
    for (int i = 0; i < 1; i++){
        manager.run_pr();
        manager.run_pr_simple();
    }
    
    //Run 1-HOP query
    for (int i = 0; i < 1; i++){
        manager.run_1hop();
    }
    */
}

template <class T>
void gen_kickstarter_files(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    manager.setup_graph(v_count);    
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
    
    assert(v_count == snaph->get_vcount());
    
    string vtfile = odir + "inputGraph.adj"; 
	FILE* vtf = fopen(vtfile.c_str(), "w");
    assert(vtf != 0);
    char text[256];
    
    index_t edge_count = (2L << residue);
    sprintf(text,"AdjacencyGraph\n%u\n%lu\n", v_count, edge_count);
    fwrite(text, sizeof(char), strlen(text), vtf);
	
    for (vid_t v = 0; v < v_count; v++) {
        nebr_count = snaph->get_degree_out(v);
        sprintf(text,"%lu\n", offset);
        fwrite(text, sizeof(char), strlen(text), vtf);
        //cerr << offset  << endl;
        offset += nebr_count;
    }

	for (vid_t v = 0; v < v_count; v++) {
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
            sid = get_nebr(local_adjlist, i);
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

void stream_netflow_aggregation(const string& idir, const string& odir)
{
    plaingraph_manager_t<netflow_dst_t> manager;
    manager.schema_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph(v_count);
    pgraph_t<netflow_dst_t>* pgraph = manager.get_plaingraph();
    
    stream_t<netflow_dst_t>* streamh = reg_stream_view(pgraph, do_stream_netflow_aggr);
    netflow_post_reg(streamh, v_count); 
    manager.prep_graph_and_compute(idir, odir, streamh); 
    //netflow_finalize(streamh); 
}

template <class T>
void test_logging(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    manager.prep_graph_edgelog(idir, odir);
    
    //Run BFS
    for (int i = 0; i < 1; i++){
        manager.run_bfs();
    }
}

template <class T>
void test_loggingd(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraphd();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    manager.prep_graph_edgelog(idir, odir);
    
    //Run BFS
    for (int i = 0; i < 1; i++){
        manager.run_bfs();
    }
}

template <class T>
void test_logging_fromtext(const string& idir, const string& odir,
                    typename callback<T>::parse_fn2_t parsefile_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    manager.prep_graph_edgelog_fromtext(idir, odir, parsefile_fn);
    
    //Run BFS
    for (int i = 0; i < 1; i++){
        manager.run_bfs();
    }
}

template <class T>
void test_mix(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    
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
    manager.schema_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    
    manager.prep_graph_adj(idir, odir);
    
    //Run BFS
    for (int i = 0; i < 1; i++){
        manager.run_bfs();
    }

    //g->store_graph_baseline();
    
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

template <class T>
void recover_test0(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    manager.recover_graph_adj(idir, odir);
    
    //Run BFS
    plaingraph_manager.run_bfs();
}

template <class T>
void recover_test0d(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraphd();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    manager.recover_graph_adj(idir, odir);    
    
    //Run BFS
    manager.run_bfs();
}

template <class T>
void test_ingestiond(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraphd();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    g->create_threads(true, true);   
    manager.prep_graph_durable(idir, odir); 
    manager.run_bfs();    
    g->store_graph_baseline();
}

template <class T>
void test_ingestionuni(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraphuni();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    g->create_threads(true, true);   
    manager.prep_graph_durable(idir, odir); 
    manager.run_bfs();    
    g->store_graph_baseline();
}

template <class T>
void test_ingestion_memory(const string& idir, const string& odir)
{
    if ( 0 != residue) {
        LOCAL_DELTA_SIZE = residue;
        cout << "local delta size  << " << residue << endl; 
    }
    plaingraph_manager_t<T> manager;
    pgraph_t<T>* pgraph = manager.get_plaingraph();
    
    manager.schema_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph_memory(v_count);    
    
    //g->create_threads(true, false);   
    //manager.prep_graph(idir, odir);

   
    manager.run_bfs();
    double start = mywtime();
    pgraph->compress_graph_baseline();
    double end = mywtime();
    cout << "Compress time = " << end - start << endl;
    manager.run_bfs();
}

template<class T>
void test_ingestion_memoryd(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraphd();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    g->create_threads(true, false);   
    manager.prep_graph(idir, odir); 
    manager.run_bfs();    
    
    pgraph_t<T>* graph = manager.get_plaingraph();
    double start = mywtime();
    graph->compress_graph_baseline();
    double end = mywtime();
    cout << "Compress time = " << end - start << endl;
    manager.run_bfs();
}


template <class T>
void ingestion_fromtext(const string& idir, const string& odir,
                     typename callback<T>::parse_fn_t parsefile_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    g->create_threads(true, false);
    manager.prep_graph_fromtext(idir, odir, parsefile_fn); 
    manager.run_bfs();
    g->store_graph_baseline();    
}

template <class T>
void ingestion_fromtextd(const string& idir, const string& odir,
                     typename callback<T>::parse_fn_t parsefile_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraphd();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    g->create_threads(true, false);
    manager.prep_graph_fromtext(idir, odir, parsefile_fn); 
    manager.run_bfs();
    g->store_graph_baseline();    
}

template <class T>
void test_ingestion_fromtext(const string& idir, const string& odir,
                     typename callback<T>::parse_fn2_t parsebuf_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph(v_count);
    g->create_threads(true, false);   
    manager.prep_graph_fromtext2(idir, odir, parsebuf_fn); 
    manager.run_bfs();
    g->store_graph_baseline();    
}

template <class T>
void test_ingestion(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    g->create_threads(true, true);   
    manager.prep_graph_durable(idir, odir); 
    manager.run_bfs();    
    g->store_graph_baseline();
}

void stream_wcc(const string& idir, const string& odir)
{
    plaingraph_manager.schema_plaingraph();
    //do some setup for plain graphs
    plaingraph_manager.setup_graph(v_count);    
    pgraph_t<sid_t>* pgraph = plaingraph_manager.get_plaingraph();
    
    stream_t<sid_t>* streamh = reg_stream_view(pgraph, do_stream_wcc);
    wcc_post_reg(streamh, v_count); 
    plaingraph_manager.prep_graph_and_compute(idir, odir, streamh); 
    wcc_finalize(streamh); 
}

template <class T>
void test_stream(const string& idir, const string& odir,
                     typename callback<T>::sfunc stream_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    g->create_threads(true, false);   
    pgraph_t<T>* pgraph = manager.get_plaingraph();
    
    sstream_t<T>* sstreamh = reg_sstream_view(pgraph, stream_fn, STALE_MASK|V_CENTRIC|C_THREAD);
    //sstream_t<T>* sstreamh = reg_sstream_view(pgraph, &stream_pagerank_epsilon<T>, STALE_MASK|V_CENTRIC);
    
    void* ret;
    pthread_join(sstreamh->thread, &ret);
    //--------
    //manager.prep_graph_and_scompute(idir, odir, sstreamh);
}

template <class T>
void multi_stream_bfsd(const string& idir, const string& odir,
                     typename callback<T>::sfunc stream_fn, int count)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraphd();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    g->create_threads(true, false);   
    pgraph_t<T>* pgraph = manager.get_plaingraph();
    
    sstream_t<T>** sstreamh = (sstream_t<T>**)malloc(sizeof(sstream_t<T>*)*count);
    
    for (int i = 0; i < count; ++i) {   
        sstreamh[i] = reg_sstream_view(pgraph, stream_fn,               
                        STALE_MASK|V_CENTRIC|C_THREAD, (void*)i);
    }
    manager.prep_graph_fromtext(idir, odir, parsefile_and_insert);
    for (int i = 0; i < count; ++i) { 
        void* ret;
        pthread_join(sstreamh[i]->thread, &ret);
    } 
}

template <class T>
void multi_stream_bfs(const string& idir, const string& odir,
                     typename callback<T>::sfunc stream_fn, int count)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    pgraph_t<T>* pgraph = manager.get_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph_memory(v_count);    
    g->create_threads(true, false);   
    
    sstream_t<T>** sstreamh = (sstream_t<T>**)malloc(sizeof(sstream_t<T>*)*count);
    
    for (int i = 0; i < count; ++i) {
        sstreamh[i] = reg_sstream_view(pgraph, stream_fn, 
                            STALE_MASK|V_CENTRIC|C_THREAD, (void*)(i+1));
    }
    
    //CorePin(0);
    manager.prep_graph_fromtext(idir, odir, parsefile_and_insert);

    for (int i = 0; i < count; ++i) {
        void* ret;
        pthread_join(sstreamh[i]->thread, &ret);
    }
    
}

template <class T>
void serial_scopy_bfs(const string& idir, const string& odir,
               typename callback<T>::sfunc stream_fn,
               typename callback<T>::sfunc scopy_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    pgraph_t<T>* pgraph = manager.get_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph_memory(v_count);    
    
#ifdef _MPI

    if (_rank == 0) {
        //g->create_threads(true, false);   
        //create scopy_server
        scopy_server_t<T>* scopyh = reg_scopy_server(pgraph, scopy_fn, 
                                            STALE_MASK|V_CENTRIC|C_THREAD);
        //CorePin(0);
        manager.prep_log_fromtext(idir, odir, parsefile_and_insert);
        void* ret;
        pthread_join(scopyh->thread, &ret);

    } else {
        //create scopy_client
        scopy_client_t<T>* sclienth = reg_scopy_client(pgraph, stream_fn, 
                                                STALE_MASK|V_CENTRIC|C_THREAD);
        void* ret;
        pthread_join(sclienth->thread, &ret);
    
    }
#endif
}

template <class T>
void serial_scopy2d_bfs(const string& idir, const string& odir,
               typename callback<T>::sfunc stream_fn,
               typename callback<T>::sfunc scopy_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    pgraph_t<T>* pgraph = manager.get_plaingraph();
    _global_vcount = v_count;
    
#ifdef _MPI
    // extract the original group handle
    create_2d_comm();
    if (_rank == 0) {
        //do some setup for plain graphs
        manager.setup_graph_memory(v_count);    
        //g->create_threads(true, false);   
        
        //create scopy_server
        scopy2d_server_t<T>* scopyh = reg_scopy2d_server(pgraph, scopy_fn, 
                                            STALE_MASK|V_CENTRIC|C_THREAD);
        manager.prep_log_fromtext(idir, odir, parsefile_and_insert);
        void* ret;
        pthread_join(scopyh->thread, &ret);

    } else {
        //do some setup for plain graphs
        vid_t local_vcount = (v_count/(_numtasks - 1));
        vid_t v_offset = (_rank - 1)*local_vcount;
        if (_rank == _numtasks - 1) {//last rank
            local_vcount = v_count - v_offset;
        }
        local_vcount = v_count;
        switch (_numtasks - 1) {
        case 4:
            local_vcount = v_count/2;
            break;
        case 9:
            local_vcount = v_count/3;
            break;
        default:
            v_offset = v_count;
            break;
        }
        manager.setup_graph_memory(local_vcount);    
        //create scopy_client
        scopy2d_client_t<T>* sclienth = reg_scopy2d_client(pgraph, stream_fn, 
                                                STALE_MASK|V_CENTRIC|C_THREAD);
        void* ret;
        pthread_join(sclienth->thread, &ret);
    }
#endif
}

template <class T>
void serial_scopy1d_bfs(const string& idir, const string& odir,
               typename callback<T>::sfunc stream_fn,
               typename callback<T>::sfunc scopy_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    pgraph_t<T>* pgraph = manager.get_plaingraph();
    _global_vcount = v_count;
    
#ifdef _MPI
    // extract the original group handle
    create_1d_comm();
    
    if (_rank == 0) {
        //do some setup for plain graphs
        manager.setup_graph_memory(v_count);    
        //g->create_threads(true, false);   
        
        //create scopy_server
        scopy1d_server_t<T>* scopyh = reg_scopy1d_server(pgraph, scopy_fn, 
                                            STALE_MASK|V_CENTRIC|C_THREAD);
        //CorePin(0);
        manager.prep_log_fromtext(idir, odir, parsefile_and_insert);
        void* ret;
        pthread_join(scopyh->thread, &ret);

    } else {
        //do some setup for plain graphs
        vid_t local_vcount = (v_count/(_numtasks - 1));
        vid_t v_offset = (_rank - 1)*local_vcount;
        if (_rank == _numtasks - 1) {//last rank
            local_vcount = v_count - v_offset;
        }
        manager.setup_graph_memory(local_vcount);    
        //create scopy_client
        scopy1d_client_t<T>* sclienth = reg_scopy1d_client(pgraph, stream_fn, 
                                                STALE_MASK|V_CENTRIC|C_THREAD);
        void* ret;
        pthread_join(sclienth->thread, &ret);
    
    }
#endif
}

template <class T>
void test_serial_stream(const string& idir, const string& odir,
                     typename callback<T>::sfunc stream_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraph();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    pgraph_t<T>* graph = manager.get_plaingraph();
    
    //sstream_t<T>* sstreamh = reg_sstream_view(graph, &stream_pagerank_epsilon1<T>, 0, 0 ,0);
    sstream_t<T>* sstreamh = reg_sstream_view(graph, stream_fn, STALE_MASK|V_CENTRIC|C_THREAD);
    
    manager.prep_log_fromtext(idir, odir, parsefile_and_insert);
    
    void* ret;
    pthread_join(sstreamh->thread, &ret);
    //--------
    
    //manager.prep_graph_serial_scompute(idir, odir, sstreamh);
}

template <class T>
void test_serial_streamd(const string& idir, const string& odir,
                     typename callback<T>::sfunc stream_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraphd();
    //do some setup for plain graphs
    manager.setup_graph(v_count);    
    pgraph_t<T>* graph = manager.get_plaingraph();
    
    //sstream_t<T>* sstreamh = reg_sstream_view(graph, &stream_pagerank_epsilon1<T>, STALE_MASK|V_CENTRIC);
    sstream_t<T>* sstreamh = reg_sstream_view(graph, stream_fn, STALE_MASK|V_CENTRIC|C_THREAD);
    
    manager.prep_log_fromtext(idir, odir, parsefile_and_insert);
    
    void* ret;
    pthread_join(sstreamh->thread, &ret);
    //--------
    
    //manager.prep_graph_serial_scompute(idir, odir, sstreamh);
}

void plain_test(vid_t v_count1, const string& idir, const string& odir, int job)
{
    v_count = v_count1; 
    switch (job) {
        case 1:
            test_mix<sid_t>(idir, odir);
            break;
        case 2:
            recover_testu<sid_t>(odir);
            break;
        case 3:
            prior_snap_testu<sid_t>(odir);
            break;
        case 6:
            recover_testuni<netflow_dst_t>(odir);
            break;
        case 7:
            recover_test0<sid_t>(idir, odir);
            break;
        case 8:
            recover_test0d<sid_t>(idir, odir);
            break;
        
        case 9:
            //stinger test
            weighted_dtest0(idir, odir);
            break;
        case 10:
            stream_wcc(idir, odir);
            break;
        
        case 11:
            paper_test_pr(idir, odir);
            break;
        case 12:
            paper_test_hop1(idir, odir);
            break;
        case 13:
            paper_test_hop2(idir, odir);
            break;
        case 14:
            paper_test_chain_bfs(idir, odir);
            break;
        case 15:
            paper_test_pr_chain(idir, odir);
            break;
        case 16:
            paper_test_hop1_chain(idir, odir);
            break;
        case 17:
            paper_test_hop2_chain(idir, odir);
            break;
        //plain graph in text format with ID. Not for performance
        case 18: 
            ingestion_fromtext<sid_t>(idir, odir, parsefile_and_insert);
            break;
        case 19: 
            test_ingestion_fromtext<sid_t>(idir, odir, parsebuf_and_insert);
            break;
        case 20: 
            test_logging_fromtext<sid_t>(idir, odir, parsebuf_and_insert);
            break;

        //plaingrah benchmark testing    
        case 21: 
            test_archive<sid_t>(idir, odir);
            break;
        case 22: 
            test_ingestion<sid_t>(idir, odir);
            break;
        case 23: 
            test_archived<sid_t>(idir, odir);
            break;
        case 24: 
            test_ingestiond<sid_t>(idir, odir);
            break;
        case 25:
            test_ingestion_memory<sid_t>(idir, odir);
            break;
        case 26:
            test_ingestion_memoryd<sid_t>(idir, odir);
            break;
        case 27:
            test_logging<sid_t>(idir, odir);
            break;
        case 28:
            test_serial_stream<sid_t>(idir, odir, stream_serial_bfs);
            //test_stream<sid_t>(idir, odir, stream_bfs);
            break;
        case 29:
            test_serial_streamd<sid_t>(idir, odir, stream_serial_bfs);
            break;

        //netflow graph testing
        case 30:
            test_ingestion_memoryd<netflow_dst_t>(idir, odir);
            break;
        case 31:
            test_ingestiond<netflow_dst_t>(idir, odir);
            break;
        case 32: 
            test_archived<netflow_dst_t>(idir, odir);
            break;
        case 33:
            test_loggingd<netflow_dst_t>(idir, odir);
            break;
        case 34:
            recover_test0d<netflow_dst_t>(idir, odir);
            break;
        //These are not for performance.    
        case 35://text to our format
            ingestion_fulld<netflow_dst_t>(idir, odir, parsefile_and_insert);
            break;
        case 36://text to binary file
            ingestion_fulld<netflow_dst_t>(idir, odir, parsefile_to_bin);
            break;
        case 37://text to our format
            ingestion_fulluni<netflow_dst_t>(idir, odir, parsefile_and_insert);
            break;
        case 38:
            prior_snap_testuni<netflow_dst_t>(odir);
            break;
        case 39://text to our format
            test_ingestion_fulld<netflow_dst_t>(idir, odir, parsebuf_and_insert);
            break;
        
        case 40:
            split_files<weight_sid_t>(idir, odir);
            break;
        case 41://llama
            llama_test<weight_sid_t>(idir, odir, parsefile_and_insert);
            break;
        case 42://llama
            llama_testd<weight_sid_t>(idir, odir, parsefile_and_insert);
            break;
        case 43: 
            test_archived<weight_sid_t>(idir, odir);
            break;
        case 44://generate kickstarter files
            gen_kickstarter_files<sid_t>(idir, odir);
            break;
        case 45:
            stream_netflow_aggregation(idir, odir);
            break;
        case 92:
            serial_scopy2d_bfs<sid_t>(idir, odir, scopy2d_serial_bfs, scopy2d_server);
            break;
        case 93:
            serial_scopy1d_bfs<sid_t>(idir, odir, scopy1d_serial_bfs, scopy1d_server);
            break;
        case 94:
            serial_scopy_bfs<sid_t>(idir, odir, scopy_serial_bfs, scopy_server);
            break;
        case 95:
            multi_stream_bfs<sid_t>(idir, odir, stream_bfs, residue);
            break;
        case 96:
            multi_stream_bfsd<sid_t>(idir, odir, stream_bfs, residue);
            break;
        case 97:
            estimate_chain_new<sid_t>(idir, odir);
            break; 
        case 98:
            estimate_chain<sid_t>(idir, odir);
            break; 
        case 99:
            estimate_IO<sid_t>(idir, odir);
            break; 
        default:
            break;
    }
}
