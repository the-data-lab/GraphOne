#pragma once
#include <omp.h>
#include "graph_view.h"
#include "sstream_analytics.h"

template <class T>
void diff_streambfs(gview_t<T>* view)
{
    //
    double start = mywtime ();
    double end = 0;
    int update_count = 0;
    sstream_t<T>* viewh = dynamic_cast<sstream_t<T>*>(view);
   
    vid_t v_count = viewh->get_vcount();
    init_bfs(viewh);


    while (viewh->get_snapmarker() < _edge_count) {
        if (eOK != viewh->update_view()) continue;
        if (update_count == 0) {
            do_streambfs(viewh);
        } else {
            do_diffbfs(viewh);
        }
        ++update_count;
        cout << " update_count = " << update_count << endl;
    }
    print_bfs(viewh);
    cout << " update_count = " << update_count 
         << " snapshot count = " << viewh->get_snapid() << endl;
}

template <class T> 
void do_diffbfs(sstream_t<T>* viewh) 
{
    uint8_t* status = (uint8_t*)viewh->get_algometa();
    vid_t   v_count = viewh->get_vcount();
    uint8_t level = 1;//start from second iteration

    index_t frontier = 0;
    do {
        frontier = 0;
        //double start = mywtime();
        #pragma omp parallel num_threads (THD_COUNT) reduction(+:frontier)
        {
        sid_t sid;
        uint8_t backup_level = 0;
        uint8_t new_level = 255;
        degree_t nebr_count = 0;
        degree_t prior_sz = 65536;
        T* local_adjlist = (T*)malloc(prior_sz*sizeof(T));

        #pragma omp for nowait
        for (vid_t v = 0; v < v_count; v++) {
            if(false == viewh->has_vertex_changed_out(v) || status[v] < level ) continue;
            
            backup_level = status[v];
            new_level =  255;

            //handle the in-edges
            nebr_count = viewh->get_degree_in(v);
            if (nebr_count == 0) {
                continue;
            } else if (nebr_count > prior_sz) {
                prior_sz = nebr_count;
                free(local_adjlist);
                local_adjlist = (T*)malloc(prior_sz*sizeof(T));
            }

            viewh->get_nebrs_in(v, local_adjlist);

            for (degree_t i = 0; i < nebr_count; ++i) {
                sid = get_sid(local_adjlist[i]);
                new_level = min(new_level, status[sid]);
            }

            if (new_level+1 == level && backup_level >= level) {//upgrade case
                status[v] = level;
                viewh->reset_vertex_changed_out(v);
                ++frontier;
            } else if (new_level+1 != level &&  backup_level == level) { //infinity case
                status[v] == 255;
                viewh->reset_vertex_changed_out(v);
                ++frontier;
            } else if (backup_level > level && backup_level != 255) {
                ++frontier;
                continue;
            } else {
                continue;
            }

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
                viewh->set_vertex_changed_out(sid);
            }
        }
        }
        ++level;
    } while (frontier);
}

struct diff_rank_t {
    rank_t  rank[5];
};

template <class T> 
void init_pr(gview_t<T>* viewh) {
    vid_t v_count = viewh->get_vcount();

    index_t r = 5; 
    index_t c = v_count;
    index_t len = 0; 
    float *ptr, **arr; 
              
    len = sizeof(float *) *r + sizeof(float) * c * r; 
    arr = (float **)calloc(len, sizeof(uint8_t));

    // ptr is now pointing to the first element in 2D array 
    ptr = (float *)(arr + r);

    // for loop to point rows pointer to appropriate location in 2D array 
    for(vid_t i = 0; i < r; i++) {
        arr[i] = (ptr + c * i);
    }
    viewh->set_algometa(arr);
}

template <class T> 
void do_diffpr(sstream_t<T>* viewh, float epsilon)
{
    vid_t v_count = viewh->get_vcount();
    float** pr_rank = (float**)viewh->get_algometa();
    float* rank_array = 0;
    float* prior_rank_array = 0;

    //these two are backup ranks
    float* rank_array1 = (float*)malloc(v_count*sizeof(float));
    float* prior_rank_array1 = (float*)malloc(v_count*sizeof(float));
    
    //normalize the starting rank, it is like iteration 0;
    prior_rank_array = pr_rank[0];
    rank_array =  pr_rank[1];
    #pragma omp parallel
    {
        degree_t degree_out = 0;
        #pragma omp for
        for (vid_t v = 0; v < v_count; ++v) {
            if (!viewh->has_vertex_changed_out(v)) continue;

            prior_rank_array1[v] = prior_rank_array[v];
            degree_out = viewh->get_degree_out(v);
            if (degree_out) {
                prior_rank_array[v] = 1.0/degree_out;//normalizing
            } else {
                prior_rank_array[v] = 0;
            }
        }
    }

    int iter = 1;
    float delta = epsilon + epsilon;//any value greater
	while(delta > epsilon) {
        //double start1 = mywtime();
        prior_rank_array = pr_rank[iter - 1];
        rank_array =  pr_rank[iter];
        delta = 0;
        #pragma omp parallel 
        {
            sid_t sid;
            degree_t  nebr_count = 0;

            degree_t prior_sz = 2048;
            T* local_adjlist = (T*) malloc(sizeof(T)*prior_sz);;

            double rank = 0.0; 
            
            #pragma omp for 
            for (vid_t v = 0; v < v_count; v++) {
                if (!viewh->has_vertex_changed_out(v)) continue;
                
                nebr_count = viewh->get_degree_out(v);
                if (nebr_count == 0) {
                    continue;
                } else if (nebr_count > prior_sz) {
                    prior_sz = nebr_count;
                    free(local_adjlist);
                    local_adjlist = (T*)malloc(prior_sz*sizeof(T));
                }

                viewh->get_nebrs_out(v, local_adjlist);
                rank = prior_rank_array[v] - prior_rank_array1[v];//delta, normalized

                for (degree_t i = 0; i < nebr_count; ++i) {
                    sid = get_sid(local_adjlist[i]);
                    sid = TO_SID(sid);
                    if (!IS_DEL(sid)) {
                        qthread_dincr(rank_array1 + sid, rank);
                    } else {
                        qthread_dincr(rank_array1 + sid, -rank);
                    }
                }
            }
            
            double mydelta = 0;
            double new_rank = 0;
            degree_t degree_out;
            degree_t prior_degree_out = 0;//XXX
            
            #pragma omp for reduction(+:delta)
            for (vid_t v = 0; v < v_count; v++ ) {
                degree_out = viewh->get_degree_out(v);
                if (degree_out == 0 || rank_array1[v] == 0) continue;
                
                viewh->set_vertex_changed_out(sid);
                new_rank = (rank_array[v]*prior_degree_out //old rank
                            + 0.85*rank_array1[v] //new delta rank
                           )/degree_out; //normalized

                mydelta =  new_rank - prior_rank_array[v];//normalized diff
                if (mydelta < 0) mydelta = -mydelta;
                delta += mydelta;

                prior_rank_array1[v] = rank_array[v];//used for next iter
                rank_array1[v] = 0;//to be used for next iter;
                rank_array[v] = new_rank;//updated rank 
            } 
        }
        ++iter;
        //double end1 = mywtime();
        //cout << "Delta = " << delta << "Iteration Time = " << end1 - start1 << endl;
    }	
}
template <class T> 
void do_streampr(sstream_t<T>* viewh, float epsilon)
{
    vid_t v_count = viewh->get_vcount();
	//let's run the pagerank
    float** pr_rank = (float**)viewh->get_algometa();
    float* rank_array = pr_rank[0];
    
    //normalize the starting rank, it is like iteration 0;
    #pragma omp parallel
    { 
        degree_t degree_out = 0;
        #pragma omp for
        for (vid_t v = 0; v < v_count; ++v) {
            degree_out = viewh->get_degree_out(v);
            if (degree_out) {
                rank_array[v] = 1.0/degree_out;//normalizing
            } else {
                rank_array[v] = 0; 
            }
        }
    }

    int iter = 1;
    float delta = epsilon + epsilon;
    float* prior_rank_array = 0;
	while(delta > epsilon) {
        //double start1 = mywtime();
        prior_rank_array = pr_rank[iter - 1];
        rank_array =  pr_rank[iter];
        delta = 0;
        #pragma omp parallel 
        {
            sid_t sid;
            degree_t degree_out = 0;
            degree_t prior_sz = 2048;
            T* local_adjlist = (T*) malloc(sizeof(T)*prior_sz);;

            double rank = 0.0; 
            
            #pragma omp for 
            for (vid_t v = 0; v < v_count; v++) {
                degree_out = viewh->get_degree_out(v);
                if (degree_out == 0) {
                    continue;
                } else if (degree_out > prior_sz) {
                    prior_sz = degree_out;
                    free(local_adjlist);
                    local_adjlist = (T*)malloc(prior_sz*sizeof(T));
                }

                viewh->get_nebrs_out(v, local_adjlist);
                rank = prior_rank_array[v];

                for (degree_t i = 0; i < degree_out; ++i) {
                    sid = get_sid(local_adjlist[i]);
                    qthread_dincr(rank_array + sid, rank);
                }
            }
            
            double mydelta = 0;
            double new_rank = 0;
            
            #pragma omp for reduction(+:delta)
            for (vid_t v = 0; v < v_count; v++ ) {
                degree_out = viewh->get_degree_out(v);
                if (degree_out == 0) continue;
                
                new_rank = (0.15 + 0.85*rank_array[v])/degree_out;//normalized
                mydelta =  new_rank - prior_rank_array[v];//normalized diff
                if (mydelta < 0) mydelta = -mydelta;
                delta += mydelta;

                rank_array[v] = new_rank;
            } 
        }
        ++iter;
        //double end1 = mywtime();
        //cout << "Delta = " << delta << "Iteration Time = " << end1 - start1 << endl;
    }	
}

template <class T>
void diff_streampr(gview_t<T>* view)
{
    double start = mywtime ();
    double end = 0;
    int update_count = 0;
    
    float epsilon = 1e-6;
    sstream_t<T>* viewh = dynamic_cast<sstream_t<T>*>(view);
    vid_t v_count = viewh->get_vcount();
    init_pr(viewh);

    while (viewh->get_snapmarker() < _edge_count) {
        if (eOK != viewh->update_view()) continue;
        if (update_count == 0) {
            do_streampr(viewh, epsilon);
        } else {
            do_diffpr(viewh, epsilon);
        }
        ++update_count;
        cout << " update_count = " << update_count << endl;
    }
    print_bfs(viewh);
    cout << " update_count = " << update_count 
         << " snapshot count = " << viewh->get_snapid() << endl;
}
