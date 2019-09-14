#include <algorithm>
#include <fstream>
#include <iostream>
#include <dirent.h>
#include <assert.h>
#include <string>
#include <map>
#include "nt_to_edge.h"
#include "graph.h"
#include "type.h"
#include "wtime.h"

using std::ifstream;

void ntriple_manager::prep_type(const string& typefile, const string& odir)
{
    string subject, predicate, object, useless_dot;
    int file_count = 0;
    
    //Read typefile for types 
    ifstream file(typefile.c_str());
    int nt_count= 0;
    file_count++;
    file >> subject >> predicate >> object >> useless_dot;
    file >> subject >> predicate >> object >> useless_dot;
    propid_t pid;
    map<string, propid_t>::iterator str2pid_iter;
    while (file >> subject >> predicate >> object >> useless_dot) {
        str2pid_iter = g->str2pid.find(predicate);
        if (str2pid_iter == g->str2pid.end()) {
            assert(0);
        }
        pid = str2pid_iter->second;
        if( pid == 0) { // type
            g->type_update(subject, object);
            ++nt_count;
        }
    }

    //g->type_store(odir);
}

void ntriple_manager::prep_graph(const string& idirname, const string& odirname)
{
    struct dirent *ptr;
    DIR *dir;
    string subject, predicate, object, useless_dot;
    int file_count = 0;
    g->prep_graph_baseline();
    g->file_open(true);
    
    //g->create_snapthread();
    //usleep(1000);
    
    
    //Read graph file
    long nt_count= 0;
    dir = opendir(idirname.c_str());
    double start = mywtime();
    while (NULL != (ptr = readdir(dir))) {
        if (ptr->d_name[0] == '.') continue;
        
        ifstream file((idirname + "/" + string(ptr->d_name)).c_str());
        file_count++;
        file >> subject >> predicate >> object >> useless_dot;
        file >> subject >> predicate >> object >> useless_dot;
        
        while (file >> subject >> predicate >> object >> useless_dot) {
            g->batch_update(subject, object, predicate);
            ++nt_count;
        }
    }
    closedir(dir);
    g->make_graph_baseline();
    double end = mywtime();
    cout << "graph make time = " << end - start  
         << " triple cout = " << nt_count << endl;
    g->store_graph_baseline();
    
}

status_t ntriple_manager::remove_edge(const string& idirname, const string& odirname)
{
    /*
    struct dirent *ptr;
    DIR *dir;
    string subject, predicate, object, useless_dot;
    int file_count = 0;
    sid_t src_id, dst_id;
    pedge_t* edges;
    sid_t index;
    //propid_t cf_id;
    
    //Read graph file
    dir = opendir(idirname.c_str());
    while (NULL != (ptr = readdir(dir))) {
        if (ptr->d_name[0] == '.') continue;
        
        ifstream file((idirname + "/" + string(ptr->d_name)).c_str());
        int nt_count= 0;
        file_count++;
        file >> subject >> predicate >> object >> useless_dot;
        file >> subject >> predicate >> object >> useless_dot;
        propid_t pid;
        map<string, propid_t>::iterator str2pid_iter;
        while (file >> subject >> predicate >> object >> useless_dot) {
            str2pid_iter = g->str2pid.find(predicate);
            if (str2pid_iter == g->str2pid.end()) {
                assert(0);//can't delete
            }
            pid = str2pid_iter->second;
            //cf_id = get_cfid(pid);
            if (pid != 0) { //non-type
                if (batch_info1[batch_count1].count == MAX_PECOUNT) {
                    void* mem = alloc_buf();
                    if (mem == 0) return eEndBatch;
                    ++batch_count1;
                    batch_info1[batch_count1].count = 0;
                    batch_info1[batch_count1].buf = mem; 
                }
                
                map<string, vid_t>::iterator str2vid_iter = str2vid.find(subject);
                if (str2vid.end() == str2vid_iter) {
                    assert(0);
                } else {
                    src_id = str2vid_iter->second;
                }
                //tid_t type_id = TO_TID(src_id);
                //flag1 |= (1L << type_id);

                str2vid_iter = str2vid.find(object);
                if (str2vid.end() == str2vid_iter) {
                    assert(0);
                } else {
                    dst_id = str2vid_iter->second;
                }
                
                index = batch_info1[batch_count1].count++;
                edges = (pedge_t*) batch_info1[batch_count1].buf;
                edges[index].pid = pid;
                edges[index].src_id = src_id; 
                edges[index].dst_id.value_sid = dst_id;
                
                //cf_info[cf_id]->batch_update(subject, object, pid);
                ++nt_count;
            //} else { //don't delete a type!!!
            }
        }
    }
    closedir(dir);
    */
    return eOK;
}
