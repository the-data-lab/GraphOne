#pragma once

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

using namespace std;

typedef index_t (parse_fn_t)(const string& textfile, const string& ofile); 

class multi_graph_t {
 public:
     multi_graph_t();
     void schema();
     void prep_graph_fromtext(const string& idirname, const string& odirname, 
                              parse_fn_t);

};

index_t parsefile_and_multi_insert(const string& textfile, const string& ofile); 
void  wls_schema();
void  wls_setup();
