#include <omp.h>
#include <iostream>
#include <getopt.h>
#include <stdlib.h>

#include "graph.h"
#include "sgraph.h"
#include "typekv.h"
#include "graph_view.h"

static const long kNumVertices = 1000;

index_t residue = 1;
int THD_COUNT = 0;
vid_t _global_vcount = 0;
index_t _edge_count = 0;
int _dir = 1;//directed
int _persist = 0;//no
int _source = 0;//text

graph* g;

int add_edge(pgraph_t<dst_id_t>* pg, unsigned int src, unsigned int dst)
{
    edgeT_t<dst_id_t> edge;
    edge.src_id = src;
    set_dst(&edge, dst);
    return pg->batch_edge(edge);
}

pgraph_t<dst_id_t> *pgraph;

void schema(graph *g){
    g->cf_info = new cfinfo_t *[2];
    g->p_info = new pinfo_t[2];

    pinfo_t *p_info = g->p_info;
    cfinfo_t *info = 0;
    const char *longname = 0;
    const char *shortname = 0;

    longname = "gtype";
    shortname = "gtype";
    info = new typekv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    ++p_info;

    longname = "friend";
    shortname = "friend";
    info = new dgraph<dst_id_t>;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = 1;
    info->flag2 = 1;
    ++p_info;
    pgraph = static_cast<pgraph_t<dst_id_t>*>(info);
}

void setup() 
{
    THD_COUNT = omp_get_max_threads() - 1;// - 3;
    cout << "Threads Count = " << THD_COUNT << endl;


    // construct graph similar to main.cpp
    g = new graph;
    g->set_odir("");// Lets not make the edges durable
    schema(g);

    // create archiving/snapshot thread and persist/dis-write threads.
    g->create_threads(true, false); // Lets not make edges durable
    g->prep_graph_baseline();
    g->file_open(true);

    typekv_t* typekv = g->get_typekv();
    typekv->manual_setup(kNumVertices, true);
    pgraph->prep_graph_baseline(eADJ);
}

void check_progress(snap_t<dst_id_t>* snaph)
{
    edgeT_t<dst_id_t>* edgePtr;
    std::cout << "Archived_edges = " << snaph->get_snapmarker() << std::endl; //archived edges
    std::cout << "Non-archived edges : " << snaph->get_nonarchived_edges(edgePtr) << std::endl;
    std::cout << "Degree Out of 1 = " << snaph->get_degree_out(1) << std::endl;
    cout << "----------------" << endl;
}

int main(int argc, char* argv[]) {
    setup();
    
    snap_t<dst_id_t>* snaph = create_static_view(pgraph, SIMPLE_MASK|V_CENTRIC);
    edgeT_t<dst_id_t>* edgePtr;
            
    cout << "----------------" << endl;
    check_progress(snaph);

    //batch_update is not working yet, 
    //you need to use typekv to convert the string to id, and use pgraph->batch_edges
    //status_t s = pgraph->batch_update("1", "2");
    
    //correct way of adding edges
    add_edge(pgraph, 1,10);
    add_edge(pgraph, 10,1);
    add_edge(pgraph, 2,4);
    add_edge(pgraph, 3,4);

    // update view does not include information about new archived edges, why?? 
    // This is because archive is triggered only after (1<<16) edges, and I am sure it has not been triggered.
    // So, how can user trigger it: 
    // 1) use g->waitfor_archive();
    // 2) use SIMPLE_MASK during view creation. Note: there is a bug in master, fixed in feature branch, so degree_out may still be stale.
    snaph->update_view();
    check_progress(snaph);

    //---- More data insertion -----//
    // random number for graph init.
    srand (time(NULL));
    for(long i = 4; i < (1 << 18); i++){
        auto src_v = rand() % kNumVertices;
        auto dest_v = rand() % kNumVertices;
        add_edge(pgraph, src_v, dest_v);

        // archiving happens after every batch of (1 << 16) edge inserts, but there is a time-out also.
        if ((i % (1 << 16)) == 0) {
            snaph->update_view();
            check_progress(snaph);
        }
    }
    g->waitfor_archive();//Lets wait for all the edges to archive
    snaph->update_view();
    check_progress(snaph);

    return 0;
}
