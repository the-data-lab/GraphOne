
#pragma once
#include <omp.h>
#include <algorithm>

#include "graph_view.h"

using std::min;

static vid_t MAX_VID = -1L;
struct bfs_info_t {
    vid_t*   parent;
    uint8_t* lstatus;
    uint8_t* rstatus;
    Bitmap*  lbitmap;
    Bitmap*  rbitmap;
    vid_t    root;
};

template <class T> 
void init_bfs1(gview_t<T>* viewh, bool symm = true)
{
    vid_t v_count = viewh->get_vcount();
    index_t meta = (index_t)viewh->get_algometa();
    vid_t root = _arg;

    bfs_info_t* bfs_info = (bfs_info_t*)malloc(sizeof(bfs_info_t));
    vid_t* parent = (vid_t*)malloc(v_count*sizeof(vid_t));
    uint8_t* status = (uint8_t*)malloc(v_count*sizeof(uint8_t));
    memset(parent, 255, v_count*sizeof(vid_t));
    memset(status, 255, v_count);
    
    status[root] = 0;
    cout << "root:" << root << endl;
    bfs_info->lstatus = status;
    if (true == symm) {
        bfs_info->rstatus = bfs_info->lstatus;
    } else {
        bfs_info->rstatus = (uint8_t*)malloc(v_count*sizeof(uint8_t));
    }
    bfs_info->parent = parent;
    bfs_info->root = root;
    
    viewh->set_algometa(bfs_info);
}

template <class T>
void print_bfs1(gview_t<T>* viewh) 
{
    bfs_info_t* bfs_info = (bfs_info_t*)viewh->get_algometa();
    uint8_t* status = bfs_info->lstatus;
    vid_t    v_count = viewh->get_vcount();

    //cout << "root:" << bfs_info->root << endl;
    vid_t vid_count = 0;
    int l = 0;
    do {
        vid_count = 0;
        #pragma omp parallel for reduction (+:vid_count) 
        for (vid_t v = 0; v < v_count; ++v) {
            if (status[v] == l) {
                //cerr << v << endl;
                ++vid_count;
            }
        }
        cout << " Level = " << l << " count = " << vid_count << endl;
        ++l;
    } while (vid_count !=0);
}

template <class T> 
void do_streambfs(sstream_t<T>* viewh) 
{
    uint8_t* status = (uint8_t*)viewh->get_algometa();
    vid_t   v_count = viewh->get_vcount();

    index_t frontier = 0;
    do {
        frontier = 0;
        //double start = mywtime();
        #pragma omp parallel num_threads (THD_COUNT) reduction(+:frontier)
        {
        sid_t sid;
        uint8_t level = 0;
        degree_t nebr_count = 0;
        degree_t prior_sz = 65536;
        T* local_adjlist = (T*)malloc(prior_sz*sizeof(T));

        #pragma omp for nowait
        for (vid_t v = 0; v < v_count; v++) {
            if(false == viewh->has_vertex_changed_out(v) || status[v] == 255) continue;
            viewh->reset_vertex_changed_out(v);
            level = status[v];
            nebr_count = viewh->get_degree_out(v);
            if (nebr_count == 0) {
                continue;
            } else if (nebr_count > prior_sz) {
                prior_sz = nebr_count;
                free(local_adjlist);
                local_adjlist = (T*)malloc(prior_sz*sizeof(T));
            }

            viewh->get_nebrs_out(v, local_adjlist);

            for (degree_t i = 0; i < nebr_count; ++i) {
                sid = get_sid(local_adjlist[i]);
                if (status[sid] > level + 1) {
                    status[sid] = level + 1;
                    viewh->set_vertex_changed_out(sid);
                    ++frontier;
                    //cout << " " << sid << endl;
                }
            }
        }
        }
    } while (frontier);

}

template <class T> 
void init_bfs(gview_t<T>* viewh)
{
    vid_t v_count = viewh->get_vcount();
    index_t meta = (index_t)viewh->get_algometa();
    vid_t root = meta;
    uint8_t* status = (uint8_t*)malloc(v_count*sizeof(uint8_t));
    memset(status, 255, v_count);
    status[root] = 0;
    
    viewh->set_algometa(status);
}

template <class T>
void print_bfs(gview_t<T>* viewh) 
{
    uint8_t* status = (uint8_t*)viewh->get_algometa();
    vid_t    v_count = viewh->get_vcount();
    int l = 0;
    vid_t vid_count = 0;

    do {
        vid_count = 0;
        #pragma omp parallel for reduction (+:vid_count) 
        for (vid_t v = 0; v < v_count; ++v) {
            if (status[v] == l) ++vid_count;
        }
        cout << " Level = " << l << " count = " << vid_count << endl;
        ++l;
    } while(vid_count !=0);
}

template <class T>
void stream_bfs(gview_t<T>* viewh)
{
    //
    double start = mywtime ();
    double end = 0;
    int update_count = 0;
   
    init_bfs(viewh);
    sstream_t<T>* sstreamh = dynamic_cast<sstream_t<T>*>(viewh);
    vid_t v_count = sstreamh->get_vcount();

    while (sstreamh->get_snapmarker() < _edge_count) {
        if (eOK != sstreamh->update_view()) continue;
        ++update_count;
        do_streambfs(sstreamh);
    }
    print_bfs(viewh);
    cout << " update_count = " << update_count 
         << " snapshot count = " << sstreamh->get_snapid() << endl;
}

template<class T>
void stream_serial_bfs(gview_t<T>* viewh)
{
    sstream_t<T>* sstreamh = dynamic_cast<sstream_t<T>*>(viewh);
    vid_t v_count = sstreamh->get_vcount();
    pgraph_t<T>* pgraph  = sstreamh->pgraph;
    
    //index_t beg_marker = 0;//(_edge_count >> 1);
    //index_t rest_edges = _edge_count - beg_marker;
    //index_t do_count = residue;
    //index_t batch_size = rest_edges/do_count;
    //index_t marker = 0; 

    //cout << "starting BFS" << endl;
    viewh->set_algometa((void*)1);
    init_bfs(viewh);

    int update_count = 1;
    double start = mywtime();
    double end = 0;
    
    while (pgraph->get_archived_marker() < _edge_count) {
        //marker = beg_marker + update_count*batch_size;
        //if (update_count == do_count - 1) marker = _edge_count;

        //pgraph->create_marker(marker);
        pgraph->create_snapshot();
        //end = mywtime();
        //cout << "Make Graph Time = " << end - start << " at Batch " << update_count << endl; 
        
        //update the sstream view
        sstreamh->update_view();
        ++update_count;
        //start = mywtime();
	    do_streambfs(sstreamh);	
        end = mywtime();
        //cout << "BFS Time at Batch " << update_count << " = " << end - start << endl;
    } 
    print_bfs(viewh); 

    cout << "update_count = " << update_count << endl;
}

template<class T>
void stream_pagerank_epsilon1(gview_t<T>* viewh)
{
    sstream_t<T>* sstreamh = dynamic_cast<sstream_t<T>*>(viewh);
    vid_t v_count = sstreamh->get_vcount();
    pgraph_t<T>* ugraph  = sstreamh->pgraph;
    assert(ugraph);
    
    blog_t<T>* blog = ugraph->blog;
    index_t marker = 0; 
    double start = mywtime ();
    
    double epsilon =  1e-8;
    double  delta = 1.0;
    double  inv_count = 1.0/v_count;
    double inv_v_count = 0.15/v_count;
    
    double* rank_array = (double*)calloc(v_count, sizeof(double));
    double* prior_rank_array = (double*)calloc(v_count, sizeof(double));

    #pragma omp parallel for
    for (vid_t v = 0; v < v_count; ++v) {
        prior_rank_array[v] = inv_count;
    }

    int iter = 0;

    while (blog->blog_head < 65536) {
        usleep(1);
    }

    double end = 0;
    cout << "starting pr" << endl;

    while (blog->blog_tail < blog->blog_head || delta > epsilon) {
        if (blog->blog_tail < blog->blog_head) {
            marker = blog->blog_head;
            //cout << "marker = " << marker << endl;
            ugraph->create_marker(marker);
            ugraph->create_snapshot();
        } else {
            end = mywtime();
            cout << blog->blog_tail << " " << blog->blog_head ;
            cout << " Make graph time = " << end - start << endl;
        }

        //update the sstream view
        sstreamh->update_view();
        //double start1 = mywtime();
        #pragma omp parallel 
        {
            sid_t sid;
            degree_t      delta_degree = 0;
            degree_t        nebr_count = 0;
            degree_t      local_degree = 0;
            delta_adjlist_t<T>* delta_adjlist;
            T* local_adjlist = 0;

            double rank = 0.0; 
            
            #pragma omp for 
            for (vid_t v = 0; v < v_count; v++) {
                delta_adjlist = sstreamh->get_nebrs_archived_in(v);
                if (0 == delta_adjlist) continue;
                nebr_count = sstreamh->get_degree_in(v);
                rank = 0.0;
                
                //traverse the delta adj list
                delta_degree = nebr_count;
                while (delta_adjlist != 0 && delta_degree > 0) {
                    local_adjlist = delta_adjlist->get_adjlist();
                    local_degree = delta_adjlist->get_nebrcount();
                    degree_t i_count = min(local_degree, delta_degree);
                    for (degree_t i = 0; i < i_count; ++i) {
                        sid = get_sid(local_adjlist[i]);
                        rank += prior_rank_array[sid];
                    }
                    delta_adjlist = delta_adjlist->get_next();
                    delta_degree -= local_degree;
                }
                rank_array[v] = rank;
            }
            
            double mydelta = 0;
            double new_rank = 0;
            delta = 0;
            
            #pragma omp for reduction(+:delta)
            for (vid_t v = 0; v < v_count; v++ ) {
                if (sstreamh->get_degree_out(v) == 0) continue;
                new_rank = inv_v_count + 0.85*rank_array[v];
                mydelta = new_rank - prior_rank_array[v]*sstreamh->get_degree_out(v);
                if (mydelta < 0) mydelta = -mydelta;
                delta += mydelta;

                rank_array[v] = new_rank/sstreamh->get_degree_out(v);
                prior_rank_array[v] = 0;
            } 
        }
        swap(prior_rank_array, rank_array);
        ++iter;
    }
    
    assert (blog->blog_tail == blog->blog_head); 
    
    #pragma omp for
    for (vid_t v = 0; v < v_count; v++ ) {
        rank_array[v] = rank_array[v]*sstreamh->get_degree_out(v);
    }

    end = mywtime();

	cout << "Iteration count = " << iter << endl;
    cout << "PR Time = " << end - start << endl;

    free(rank_array);
    free(prior_rank_array);
}

template<class T>
void 
stream_pagerank_epsilon(gview_t<T>* sstreamh)
{
    double   epsilon  =  1e-8;
    double    delta   = 1.0;
    vid_t   v_count   = sstreamh->get_vcount();
    double  inv_count = 1.0/v_count;
    double inv_v_count = 0.15/v_count;
    
    double* rank_array = (double*)calloc(v_count, sizeof(double));
    double* prior_rank_array = (double*)calloc(v_count, sizeof(double));

    #pragma omp parallel for
    for (vid_t v = 0; v < v_count; ++v) {
        prior_rank_array[v] = inv_count;
    }

    while (eOK != sstreamh->update_view()) {
        usleep(5);
    }

    double start = mywtime();
    int iter = 0;

	//let's run the pagerank
	while(delta > epsilon) {
        //double start1 = mywtime();
        #pragma omp parallel 
        {
            sid_t sid;
            degree_t      delta_degree = 0;
            degree_t        nebr_count = 0;
            degree_t      local_degree = 0;

            delta_adjlist_t<T>* delta_adjlist;
            T* local_adjlist = 0;

            double rank = 0.0; 
            
            #pragma omp for 
            for (vid_t v = 0; v < v_count; v++) {

                delta_adjlist = sstreamh->get_nebrs_archived_in(v);
                if (0 == delta_adjlist) continue;
                
                nebr_count = sstreamh->get_degree_in(v);
                rank = 0.0;
                
                //traverse the delta adj list
                delta_degree = nebr_count;
                while (delta_adjlist != 0 && delta_degree > 0) {
                    local_adjlist = delta_adjlist->get_adjlist();
                    local_degree = delta_adjlist->get_nebrcount();
                    degree_t i_count = min(local_degree, delta_degree);
                    for (degree_t i = 0; i < i_count; ++i) {
                        sid = get_sid(local_adjlist[i]);
                        rank += prior_rank_array[sid];
                    }
                    delta_adjlist = delta_adjlist->get_next();
                    delta_degree -= local_degree;
                }
                rank_array[v] = rank;
            }
        
            
            double mydelta = 0;
            double new_rank = 0;
            delta = 0;
            
            #pragma omp for reduction(+:delta)
            for (vid_t v = 0; v < v_count; v++ ) {
                if (sstreamh->get_degree_out(v) == 0) continue;
                new_rank = inv_v_count + 0.85*rank_array[v];
                mydelta = new_rank - prior_rank_array[v]*sstreamh->get_degree_out(v);
                if (mydelta < 0) mydelta = -mydelta;
                delta += mydelta;

                rank_array[v] = new_rank/sstreamh->get_degree_out(v);
                prior_rank_array[v] = 0;
            } 
        }
        swap(prior_rank_array, rank_array);
        ++iter;
        
        //update the sstream view
        sstreamh->update_view();
        
        //double end1 = mywtime();
        //cout << "Delta = " << delta << "Iteration Time = " << end1 - start1 << endl;
    }	

    #pragma omp for
    for (vid_t v = 0; v < v_count; v++ ) {
        rank_array[v] = rank_array[v]*sstreamh->get_degree_out(v);
    }

    double end = mywtime();

	cout << "Iteration count" << iter << endl;
    cout << "PR Time = " << end - start << endl;

    free(rank_array);
    free(prior_rank_array);
	cout << endl;
}

//stream bfs when data has deletions

template <class T> 
index_t do_streambfs1(sstream_t<T>* viewh, Bitmap* bmap_out, Bitmap* bmap_in, Bitmap* all_bmap)
{
    vid_t v_count = viewh->get_vcount();
    bfs_info_t* bfs_info = (bfs_info_t*)viewh->get_algometa();
    uint8_t* rstatus = bfs_info->rstatus;
    uint8_t* lstatus = bfs_info->lstatus;

    Bitmap* bitmap_in = viewh->bitmap_in;

    all_bmap->reset();
    index_t frontiers = 0;
    index_t total = 0;

    double start = mywtime();
    edgeT_t<T>* edges = 0;
    index_t edge_count = viewh->get_new_edges(edges);
    #pragma omp parallel num_threads(THD_COUNT) reduction(+:frontiers)
    {
    degree_t adj_count = 0;
    vid_t parent;
    vid_t new_parent;
    vid_t nebr;
    vid_t v;
    bool is_del = false;
    uint16_t level = 255;
    uint16_t nebr_level = 255;

    //1
    #pragma omp for schedule (dynamic, 1024) nowait
    for (index_t e = 0; e < edge_count; ++e) {
        nebr = TO_VID(get_src(edges[e]));
        v =  TO_VID(get_dst(edges+e));
        is_del = IS_DEL(get_src(edges[e]));

        if ((false == is_del) || (rstatus[v] == 255)) continue;
            
        parent = bfs_info->parent[v];
        if (nebr == parent) {
            bmap_in->set_bit_atomic(v);
            ++frontiers;
            all_bmap->set_bit_atomic(v);
        }
    }
    }
    total += frontiers;
    double end1 = mywtime();
    while (frontiers) {//
        frontiers = 0;
        #pragma omp parallel num_threads(THD_COUNT) reduction(+:frontiers)
        {
        degree_t prior_sz = 65536;
        T* adj_list = (T*)malloc(prior_sz*sizeof(T));
        degree_t adj_count = 0;
        vid_t parent;
        vid_t new_parent;
        vid_t nebr;
        uint16_t level = 255;
        uint16_t nebr_level = 255;
        //1b
        #pragma omp for schedule (dynamic, 1024) nowait
        for (vid_t v = 0; v < v_count; ++v) {
            if (!bmap_in->get_bit(v)) continue;
            bmap_in->reset_bit(v);

            adj_count = viewh->get_degree_in(v);
            if (adj_count > prior_sz) {
                prior_sz = adj_count;
                adj_list = (T*)realloc(adj_list, prior_sz*sizeof(T));
            }

            adj_count = viewh->get_nebrs_in(v, adj_list);
            level = 255;//rstatus[v];
            parent = bfs_info->parent[v];
            for (degree_t i = 0; i < adj_count; ++i) {
                nebr = TO_VID(get_sid(adj_list[i]));
                nebr_level = lstatus[nebr];
                if (nebr_level < level) {
                    level = nebr_level;
                    new_parent = nebr;
                }
            }
            //== level+1, update parent, do nothing
            //< level+1, update parent, because of added edge, will be taken care later
            if (level == rstatus[v] && v != new_parent && parent != new_parent) { //traverse its children
                rstatus[v]= level+1;
                assert(rstatus[v] != 0);
                bfs_info->parent[v] = new_parent;
                bmap_out->set_bit(v);
                ++frontiers;
                continue;
            } else
            if (level >= rstatus[v]) { //traverse its children
                rstatus[v]=255;
                bfs_info->parent[v] = MAX_VID;
                bmap_out->set_bit(v);
                ++frontiers;
                continue;
            }
            rstatus[v] = level + 1;
            bfs_info->parent[v] = new_parent;
        }
        free(adj_list);
        }

        //bmap_in->reset();
        if (frontiers == 0) break;
        frontiers = 0;
        //double end2 = mywtime();

        //2
        #pragma omp parallel num_threads(THD_COUNT) reduction(+:frontiers)
        {
        degree_t prior_sz = 65536;
        T* adj_list = (T*)malloc(prior_sz*sizeof(T));
        degree_t adj_count = 0;
        vid_t parent;
        vid_t new_parent;
        vid_t nebr;
        uint16_t level = 255;
        uint16_t nebr_level = 255;
        #pragma omp for schedule (dynamic, 1024) nowait
        for (vid_t v = 0; v < v_count; ++v) {
            if (!bmap_out->get_bit(v)) continue;
            bmap_out->reset_bit(v);
            adj_count = viewh->get_degree_out(v);
            if (adj_count == 0) continue;
            else if (adj_count > prior_sz) {
                prior_sz = adj_count;
                adj_list = (T*)realloc(adj_list, prior_sz*sizeof(T));
            }
            adj_count = viewh->get_nebrs_out(v, adj_list);
            for (degree_t i = 0; i < adj_count; ++i) {
                nebr = TO_VID(get_sid(adj_list[i]));
                if (bfs_info->parent[nebr] == v) {
                    //mark nebr for 1b processing
                    bmap_in->set_bit_atomic(nebr);
                    ++frontiers;
                    all_bmap->set_bit_atomic(nebr);
                }
            }
        }
        free(adj_list);
        }
        total += frontiers;
        //bmap_out->reset();
    }

    double end2 = mywtime();
    frontiers = 0;
    if (total) {
        //3. Pull once more for all affected vertex
        #pragma omp parallel num_threads(THD_COUNT) reduction(+:frontiers)
        {
        degree_t prior_sz = 65536;
        T* adj_list = (T*)malloc(prior_sz*sizeof(T));
        degree_t adj_count = 0;
        vid_t parent;
        vid_t new_parent;
        vid_t nebr;
        uint16_t level = 255;
        uint16_t nebr_level = 255;
        #pragma omp for schedule (dynamic, 1024) nowait
        for (vid_t v = 0; v < v_count; ++v) {
            if (!all_bmap->get_bit(v)) continue;
            adj_count = viewh->get_degree_in(v);
            if (adj_count == 0) continue;
            else if (adj_count > prior_sz) {
                prior_sz = adj_count;
                adj_list = (T*)realloc(adj_list, prior_sz*sizeof(T));
            }
            adj_count = viewh->get_nebrs_in(v, adj_list);
            level = rstatus[v];
            nebr_level = 255;
            for (degree_t i = 0; i < adj_count; ++i) {
                nebr = TO_VID(get_sid(adj_list[i]));
                nebr_level = lstatus[nebr];
                if (nebr_level+1 < level) {
                    level = nebr_level + 1;
                    new_parent = nebr;
                }
            }
            if (level < rstatus[v]) {
                rstatus[v] = level;
                bfs_info->parent[v] = new_parent;
                frontiers++;
            }
        }
        free(adj_list);
        }
    }

    double end3 = mywtime();
    //4.
    //switch to traditional incremental version
    Bitmap* lbitmap = all_bmap;//viewh->bitmap_out;
    Bitmap* rbitmap = bmap_in;
    rbitmap->reset();
    lbitmap->do_or(viewh->bitmap_out);
    
    do {
        frontiers = 0;
        #pragma omp parallel num_threads (THD_COUNT) reduction(+:frontiers)
        {
        sid_t sid;
        uint16_t level = 0;
        degree_t nebr_count = 0;
        degree_t prior_sz = 65536;
        T* local_adjlist = (T*)malloc(prior_sz*sizeof(T));

        #pragma omp for schedule (dynamic, 1024) nowait
        for (vid_t v = 0; v < v_count; v++) {
            if(false == lbitmap->get_bit(v) || lstatus[v] == 255) continue;
            lbitmap->reset_bit(v);
            level = lstatus[v];

            nebr_count = viewh->get_degree_out(v);
            if (nebr_count == 0) continue;
            else if (nebr_count > prior_sz) {
                prior_sz = nebr_count;
                local_adjlist = (T*)realloc(local_adjlist, prior_sz*sizeof(T));
            }

            viewh->get_nebrs_out(v, local_adjlist);

            for (degree_t i = 0; i < nebr_count; ++i) {
                sid = TO_VID(get_sid(local_adjlist[i]));
                if (rstatus[sid] > level + 1) {
                    rstatus[sid] = level + 1;
                    bfs_info->parent[sid] = v; 
                    rbitmap->set_bit_atomic(sid);
                    ++frontiers;
                    //cout << " " << sid << endl;
                }
            }
        }
        free(local_adjlist);
        }
        //lbitmap->reset();
        std::swap(lbitmap, rbitmap);
    } while (frontiers);
    double end4 = mywtime();

    //cout << end1 - start << ":" << end2 - end1 << ":" << end3 - end2 << ":" << end4 - end3 << endl;
}

template<class T>
void stream_serial_bfs1(gview_t<T>* viewh)
{
    sstream_t<T>* sstreamh = dynamic_cast<sstream_t<T>*>(viewh);
    vid_t v_count = sstreamh->get_vcount();
    pgraph_t<T>* pgraph  = sstreamh->pgraph;
    
    Bitmap bmap_in1(v_count);
    Bitmap bmap_out1(v_count);
    Bitmap all_bmap1(v_count);
    
    index_t batch_size = residue;
    index_t marker = 0; 

    //cout << "starting BFS" << endl;
    init_bfs1(viewh);

    int update_count = 1;
    status_t ret = eOK; 
    double start, end, end1, end2;
    
    double startn = mywtime();
    while (true) {
        start = mywtime();
        marker = min(_edge_count, marker+batch_size); 
        pgraph->create_marker(marker);
        pgraph->create_snapshot();
        end = mywtime();
        
        //update the sstream view
        ret = sstreamh->update_view();
        end1 = mywtime();
        if (ret == eEndBatch) break;
        ++update_count;
	    
        //do_streambfs(sstreamh);	
        do_streambfs1(sstreamh, &bmap_out1, &bmap_in1, &all_bmap1);
        end2 = mywtime();
        
        //cout << "BFS Time at Batch " << update_count << " = " << end - start << endl;
        cout << " update_count = " << update_count
             << ":" << viewh->get_snapmarker()
             << ":" << end - start << ":" << end1 - end  
             << ":" << end2 - end1 << endl; 
    } 
    double endn = mywtime();
    print_bfs1(viewh); 

    cout << "update_count = " << update_count << "Time = "<< endn - startn << endl;
}

template<class T>
void stream_bfs1(gview_t<T>* viewh)
{
    sstream_t<T>* sstreamh = dynamic_cast<sstream_t<T>*>(viewh);
    vid_t v_count = sstreamh->get_vcount();
    pgraph_t<T>* pgraph  = sstreamh->pgraph;
    
    Bitmap bmap_in1(v_count);
    Bitmap bmap_out1(v_count);
    Bitmap all_bmap1(v_count);
    
    index_t batch_size = residue;
    index_t marker = 0; 

    //cout << "starting BFS" << endl;
    init_bfs1(viewh);

    int update_count = 1;
    status_t ret = eOK; 
    double start, end, end1, end2;
    
    double startn = mywtime();
    while (true) {
        
        //update the sstream view
        start = mywtime();
        ret = sstreamh->update_view();
        end = mywtime();
        if (ret == eEndBatch) break;
        if (ret == eNoWork) {
            usleep(100);
            continue;
        }
        ++update_count;
	    
        end1 = mywtime();
        do_streambfs1(sstreamh, &bmap_out1, &bmap_in1, &all_bmap1);
        end2 = mywtime();
        
        //cout << "BFS Time at Batch " << update_count << " = " << end - start << endl;
        cout << " update_count = " << update_count
             << ":" << viewh->get_snapmarker()
             << ":" << end - start << ":" << end1 - end  
             << ":" << end2 - end1 << endl; 
    } 
    double endn = mywtime();
    print_bfs1(viewh); 

    cout << "update_count = " << update_count << "Time = "<< endn - startn << endl;
}
