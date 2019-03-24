#pragma once

#include <string>
#include "type.h"
using std::string;

class ntriple_manager {
 public:
    static void prep_type(const string& typefile, const string& odir);
    static void prep_graph(const string& idir, const string& odir);
    static status_t remove_edge(const string& idir, const string& odir);
};
