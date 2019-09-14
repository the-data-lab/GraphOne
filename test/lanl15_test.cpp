#include "all.h"
#include <string>
#include <dirent.h>
#include <assert.h>
#include <string>
#include <unistd.h>

#include "type.h"
#include "graph.h"
#include "typekv.h"
#include "sgraph.h"
#include "util.h"

void lanl15_schema()
{
    g->cf_info = new cfinfo_t*[6];
    g->p_info = new pinfo_t[6];
    
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
    
    
    longname = "auth15";
    shortname = "auth15";
    info = new unigraph<auth15_dst_t>;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = 1;
    info->flag2 = 1;
    ++p_info;
    
    longname = "proc15";
    shortname = "proc15";
    info = new unigraph<proc15_dst_t>;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->setup_str(1<<30);
    info->flag1 = 1;
    info->flag2 = 2;
    ++p_info;
    
    longname = "flows15";
    shortname = "flows15";
    info = new unigraph<flow15_dst_t>;
    //info = new stringkv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = 2;
    info->flag2 = 2;
    ++p_info;
    
    longname = "dns15";
    shortname = "dns15";
    info = new unigraph<dns15_dst_t>;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->setup_str(1<<30);
    info->flag1 = 2;
    info->flag2 = 2;
    ++p_info;
    
    longname = "redteam15";
    shortname = "redteam15";
    info = new unigraph<redteam15_dst_t>;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->setup_str(1<<30);
    info->flag1 = 1;
    info->flag2 = 2;
    ++p_info;
}

void lanl15_setup()
{
    g->create_threads(true, false);
    typekv_t* typekv = g->get_typekv();
    typekv->manual_setup(1<<27, false, "user");//users are tid 0
    typekv->manual_setup(1<<27, false, "computer");//computers are tid 1

    g->prep_graph_baseline();
    g->file_open(true);
}

void parse_redteam15_file(FILE* file, char* buf)
{
    int line_count = 0;
    char  sss[512];
    char* line =  sss;
    char* token =0;
    const char* delim = ",\n"; 
    
    //FILE* file = fopen(filename.c_str(), "r");
    assert(file);

    redteam15_edge_t edge;
    pgraph_t<redteam15_dst_t>* redteam15 = (pgraph_t<redteam15_dst_t>*)g->get_sgraph(5); 

    while (fgets(sss, sizeof(sss), file)) {
        line = sss;
        //timestamp
        token = strtok_r(line, delim, &line);
        edge.dst_id.second.time = atoi(token);

        //src and dst computer 
        token = strtok_r(line, delim, &line);
        edge.src_id = g->type_update(token, 0);//"user" are type id 0
        token = strtok_r(line, delim, &line);
        set_dst(edge, g->type_update(token, 1));//"computer" are type id 1
        
        //Some more fields are pending
        redteam15->batch_edge(edge);
        
        ++line_count;
        //if (line_count == 10000000) break;
    }
    cout << "redteam=" << line_count << endl;
}

void parse_dns15_file(FILE* file, char* buf)
{
    int line_count = 0;
    char  sss[512];
    char* line =  sss;
    char* token =0;
    const char* delim = ",\n"; 
    
    //FILE* file = fopen(filename.c_str(), "r");
    assert(file);

    dns15_edge_t edge;
    pgraph_t<dns15_dst_t>* dns15 = (pgraph_t<dns15_dst_t>*)g->get_sgraph(4); 

    while (fgets(sss, sizeof(sss), file)) {
        line = sss;
        //timestamp
        token = strtok_r(line, delim, &line);
        edge.dst_id.second.time = atoi(token);

        //src and dst computer 
        token = strtok_r(line, delim, &line);
        edge.src_id = g->type_update(token, 1);//"computer" are type id 1
        token = strtok_r(line, delim, &line);
        set_dst(edge, g->type_update(token, 1));//"computer" are type id 1
        
        //Some more fields are pending
        dns15->batch_edge(edge);
        
        ++line_count;
        //if (line_count == 10000000) break;
    }
    cout << "dns=" << line_count << endl;
}

void parse_flow15_file(FILE* file, char* buf)
{
    int line_count = 0;
    char  sss[512];
    char* line =  sss;
    char* token =0;
    const char* delim = ",\n"; 
    
    //FILE* file = fopen(filename.c_str(), "r");
    assert(file);

    flow15_edge_t edge;
    pgraph_t<flow15_dst_t>* flow15 = (pgraph_t<flow15_dst_t>*)g->get_sgraph(3); 

    while (fgets(sss, sizeof(sss), file)) {
        line = sss;
        //timestamp
        token = strtok_r(line, delim, &line);
        edge.dst_id.second.time = atoi(token);

        //src and dst computer 
        token = strtok_r(line, delim, &line);
        edge.src_id = g->type_update(token, 1);//"computer" are type id 1
        token = strtok_r(line, delim, &line);
        set_dst(edge, g->type_update(token, 1));//"computer" are type id 1
        
        //Some more fields are pending
        flow15->batch_edge(edge);
        
        ++line_count;
        //if (line_count == 10000000) break;
    }
    cout << "flow=" << line_count << endl;
}

void parse_proc15_file(FILE* file, char* buf)
{
    int line_count = 0;
    char  sss[512];
    char* line =  sss;
    char* token =0;
    const char* delim = ",\n"; 

    proc15_edge_t edge;
    pgraph_t<proc15_dst_t>* proc15 = (pgraph_t<proc15_dst_t>*)g->get_sgraph(2); 

    //FILE* file = fopen(filename.c_str(), "r");
    assert(file);

    while (fgets(sss, sizeof(sss), file)) {
        //timestamp
        line = sss;
        token = strtok_r(line, delim, &line);
        edge.dst_id.second.time = atoi(token);

        //user and computer 
        token = strtok_r(line, delim, &line);
        edge.src_id = g->type_update(token, 0);//"user" are type id 0
        token = strtok_r(line, delim, &line);
        set_dst(edge, g->type_update(token, 1));//"computer" are type id 1
        
        //Some more fields are pending
        proc15->batch_edge(edge);    
        
        ++line_count;
        //if (line_count == 10000000) break;
    }
    cout << "proc=" << line_count << endl;
}

void parse_auth15_file(FILE* file, char* buf1)
{
    int line_count = 0;
    char  sss[512];
    char* line =  sss;
    char* token =0;
    const char* delim = ",\n"; 
    //FILE* file = fopen(filename.c_str(), "r");
    assert(file);

    auth15_edge_t edge;
    pgraph_t<auth15_dst_t>* auth15 = (pgraph_t<auth15_dst_t>*)g->get_sgraph(1); 

    /*
    //first line is waste
    char* buf = strchr(buf1, '\n');
    if(!buf) return;

    char* start = 0;
    char* end = 0;
    while (buf) {
        start = buf + 1;
        buf = strchr(buf+1, '\n');
        end = buf;
        memcpy(sss, start, end - start); 
        sss[end-start] = '\0';
    //}*/
    fgets(sss, sizeof(sss), file);//first line is waste
    while (fgets(sss, sizeof(sss), file)) {
        //timestamp
        line = sss;
        token = strtok_r(line, delim, &line);
        edge.dst_id.second.time = atoi(token);

        //src user and dst user
        token = strtok_r(line, delim, &line);
        edge.src_id = g->type_update(token, 0);//"user" are type id 0
        token = strtok_r(line, delim, &line);
        set_dst(edge, g->type_update(token, 0));//"user" are type id 0
        
        //src computer and dst computer
        token = strtok_r(line, delim, &line);
        edge.dst_id.second.src_computer = g->type_update(token, 1);//"computers" are type id 1
        token = strtok_r(line, delim, &line);
        edge.dst_id.second.dst_computer = g->type_update(token, 1);//"computers" are type id 1
    
        //Some more fields are pending
        auth15->batch_edge(edge);    
        
        ++line_count;
        //if (line_count == 10000000) break;
    }
    cout << "atuh=" << line_count << endl;
}

typedef void (*parse_file_fn)(FILE* fileh, char* buf);
struct file_list_t {
    FILE*    fileh;
    char*    buf;
    parse_file_fn   fn;
};

void prep_graph_lanl15(const string& idirname, const string& odirname)
{
    string filename;
    FILE* fileh = 0;
    char* buf = 0;
    file_list_t file_list[5];

    filename = idirname + "auth.txt";
    fileh = fopen(filename.c_str(),"r");
    assert(fileh);
    
    /*
    index_t size = fsize(filename);
    cout << "file size =" << size << endl;
    size = 10000000L*128L;
    buf = (char*)malloc(size*sizeof(char));
    index_t sz_rd = fread(buf, sizeof(char), size, fileh);
    if (size != sz_rd) {
        cout << size << endl;
        cout << sz_rd << endl;
        assert(0);
    }*/
    
    file_list[0].fileh = fileh;
    file_list[0].buf = buf;
    file_list[0].fn = parse_auth15_file;
    
    filename = idirname + "flows.txt";
    file_list[1].fileh = fopen(filename.c_str(),"r");
    assert(file_list[1].fileh);
    file_list[1].fn = parse_flow15_file;

    filename = idirname + "proc.txt";
    file_list[2].fileh = fopen(filename.c_str(),"r");
    assert(file_list[2].fileh);
    file_list[2].fn = parse_proc15_file;

    filename = idirname + "dns.txt";
    file_list[3].fileh = fopen(filename.c_str(),"r");
    assert(file_list[3].fileh);
    file_list[3].fn = parse_dns15_file;

    filename = idirname + "redteam.txt";
    file_list[4].fileh = fopen(filename.c_str(),"r");
    assert(file_list[4].fileh);
    file_list[4].fn = parse_redteam15_file;

    //Batch and Make Graph
    double start = mywtime();
   
    #pragma omp parallel for num_threads(5) 
    for (int i = 0; i < 5; ++i) {
        file_list[i].fn(file_list[i].fileh, file_list[i].buf);
    }
    
    double end = mywtime ();
    cout << "Batch Update Time = " << end - start << endl;
   
    //g->make_graph_baseline();
    g->waitfor_archive();
    end = mywtime();
    cout << "Make graph time = " << end - start << endl;
    //---------
    g->store_graph_baseline();
    end = mywtime();
    cout << "Store graph time = " << end - start << endl;
    
}

void lanl15_test0(const string& idir, const string& odir)
{
    lanl15_schema();
    lanl15_setup();
    prep_graph_lanl15(idir, odir);

    //run query

    //store the graph
    //g->store_graph_baseline();

}
