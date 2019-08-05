
#include "multi_graph.h"
#include "all.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
using namespace rapidjson;

multi_graph_t::multi_graph_t()
{
}

void wls_schema()
{
    g->cf_info = new cfinfo_t*[5];
    g->p_info = new pinfo_t[5];
    
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
    
    
    longname = "proc2parent";
    shortname = "proc2parent";
    info = new unigraph<dst_id_t>;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = 1;
    info->flag2 = 1;
    ++p_info;
    
    longname = "user2proc";
    shortname = "user2proc";
    info = new unigraph<wls_dst_t>;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->setup_str(1<<30);
    info->flag1 = 2;
    info->flag2 = 1;
    ++p_info;
    
    longname = "userlabel";
    shortname = "userlabel";
    info = new stringkv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = 2;
    //info->flag2 = 1;
    ++p_info;
    
    longname = "proclabel";
    shortname = "proclabel";
    info = new numberkv_t<proc_label_t>;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->setup_str(1<<30);
    info->flag1 = 1;
    //info->flag2 = 1;
    ++p_info;
}

void wls_setup()
{
    typekv_t* typekv = g->get_typekv();
    typekv->manual_setup(1<<28, false, "process");//processes are tid 0
    typekv->manual_setup(1<<20, false, "user");//users are tid 1
    g->prep_graph_baseline();
    g->file_open(true);

    //Create threads. Remember the index in schema
    g->cf_info[1]->create_snapthread();
    g->cf_info[1]->create_wthread();
    g->cf_info[2]->create_snapthread();
    g->cf_info[2]->create_wthread();
    usleep(1000);
}

inline index_t parse_wls_line(char* line) 
{
    if (line[0] == '%') {
        return eNotValid;
    }
    
    edgeT_t<dst_id_t> edge;
    edgeT_t<wls_dst_t> wls;
    edgeT_t<char*> str_edge;
    edgeT_t<proc_label_t> proc_edge;
    
    pgraph_t<dst_id_t>* proc2parent = (pgraph_t<dst_id_t>*)g->get_sgraph(1);
    pgraph_t<wls_dst_t>* user2proc = (pgraph_t<wls_dst_t>*)g->get_sgraph(2);
    stringkv_t* vlabel = (stringkv_t*)g->get_sgraph(3);
    numberkv_t<proc_label_t>* proc_label = (numberkv_t<proc_label_t>*)g->get_sgraph(4);
    
    Document d;
    d.Parse(line);
    string log_host, proc_id;
    string proc_name, parent_proc_name;
    index_t process_id = 0, parent_process_id = 0;;


    Value::ConstMemberIterator itr = d.FindMember("ProcessID");
    if (itr != d.MemberEnd()) {
        proc_id = itr->value.GetString();
        process_id = strtol(proc_id.c_str(), NULL, 0); 
        log_host = d["LogHost"].GetString();
        proc_id += "@";
        proc_id += log_host + "@";
        proc_id += d["LogonID"].GetString();
        set_dst(wls, g->type_update(proc_id.c_str(), 0));//"process" are type id 0.

        proc_name = d["ProcessName"].GetString();
    } else {
        return eNotValid;
    }

    string user_name = d["UserName"].GetString();
    user_name += "@";
    
    itr = d.FindMember("DomainName");
    if (itr != d.MemberEnd()) {
        user_name += d["DomainName"].GetString();
    } else {
        return eNotValid;
    }
    
    //Value& s = d["Time"];
    //int i = s.GetInt();
     
    wls.src_id = g->type_update(user_name.c_str(), 1);//"user" are type id 1.
    wls.dst_id.second.time = d["Time"].GetInt();
    wls.dst_id.second.event_id = d["EventID"].GetInt();

    string logon_id = d["LogonID"].GetString();
    wls.dst_id.second.logon_id = strtol(logon_id.c_str(), NULL, 0); 
    
    //test the string/blob value
    wls.dst_id.second.user_name = user2proc->copy_str(user_name.c_str());
    //Other ways-- not recommeded
    /*
    index_t offset;
    char* u_name = user2proc->alloc_str(strlen(user_name.c_str()) + 1, offset);
    memcpy(u_name, user_name.c_str(), strlen(user_name.c_str())+1);
    wls.dst_id.second.user_name = offset;
    */
        
    itr = d.FindMember("ParentProcessID");
    if (itr != d.MemberEnd()) {
        proc_id = itr->value.GetString();
        parent_process_id = strtol(proc_id.c_str(), NULL, 0); 
        proc_id = proc_id + "@";
        proc_id += log_host;
        proc_id += logon_id;
        set_dst(edge, g->type_update(proc_id.c_str(), 0));//"process" are type id 0.
        edge.src_id = get_dst(wls);
        
        parent_proc_name = d["ParentProcessName"].GetString();
    } else {
        return eNotValid;
    }
    
    //insert
    user2proc->batch_edge(wls);
    proc2parent->batch_edge(edge);
   
    //label for user
    str_edge.src_id = wls.src_id;
    str_edge.dst_id = const_cast<char*>(logon_id.c_str());
    vlabel->batch_edge(str_edge);
    
    //label for process
    proc_edge.src_id = edge.src_id;
    proc_edge.dst_id.proc_name = proc_label->copy_str(proc_name.c_str()); 
    proc_edge.dst_id.proc_id = process_id;
    proc_label->batch_edge(proc_edge);

    //label for parent process 
    proc_edge.src_id = get_dst(edge);
    proc_edge.dst_id.proc_name = proc_label->copy_str(parent_proc_name.c_str()); 
    proc_edge.dst_id.proc_id = parent_process_id;
    proc_label->batch_edge(proc_edge);

    return eOK;
}

index_t parsefile_and_multi_insert(const string& textfile, const string& ofile) 
{
    FILE* file = fopen(textfile.c_str(), "r");
    assert(file);
    
    index_t icount = 0;
	char sss[512];
    char* line = sss;

    while (fgets(sss, sizeof(sss), file)) {
        line = sss;
        if (eOK == parse_wls_line(line)) {
            icount++;
        }
    }
    
    fclose(file);
    return 0;
}

static void read_idir_text(const string& idirname, const string& odirname, 
                    parse_fn_t parsefile_and_insert)
{
    struct dirent *ptr;
    DIR *dir;
    int file_count = 0;
    string filename;
    string ofilename;
    vector<string> file_list;

    //Read files from the idir
    dir = opendir(idirname.c_str());
    while (NULL != (ptr = readdir(dir))) {
        if (ptr->d_name[0] == '.') continue;
        filename  = string(ptr->d_name);
        //filename = idirname + "/" + string(ptr->d_name);
        //ofilename = odirname + "/" + string(ptr->d_name);
        cout << "ifile= "  << filename << endl;
                //<<" ofile=" << ofilename << endl;

        file_list.push_back(filename);
        file_count++;
    }
    closedir(dir);
        
    //Read graph files and process
    double start = mywtime();
    for (int i = 0; i < file_count; ++i) {
        filename = idirname + file_list[i];
        ofilename = odirname + file_list[i];
        parsefile_and_insert(filename, ofilename);
    }
    double end = mywtime();
    cout <<" Time = "<< end - start;
    
    return;
}


void multi_graph_t::prep_graph_fromtext(const string& idirname, const string& odirname, 
                                        parse_fn_t parsefile_fn)
{
    //Batch and Make Graph
    double start = mywtime();
    read_idir_text(idirname, odirname, parsefile_fn);    
    double end = mywtime ();
    cout << "Batch Update Time = " << end - start << endl;
    
    /*
    blog_t<T>* blog = ugraph->blog;
    index_t marker = blog->blog_head;

    //----------
    if (marker != blog->blog_marker) {
        ugraph->create_marker(marker);
    }

    //Wait for make graph
    while (blog->blog_tail != blog->blog_head) {
        usleep(1);
    }
    */
    end = mywtime();
    cout << "Make graph time = " << end - start << endl;
    //---------
    
}
