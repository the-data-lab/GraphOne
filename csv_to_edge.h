#pragma once

#include <string>
#include "type.h"
using std::string;


class csv_manager {
 public:
    static void prep_graph(const string& conf_file, 
                           const string& idir, const string& odir);
    static void prep_vtable(const string& filename, string predicate, const string& odir);
    static void prep_vlabel(const string& filename, string predicate, const string& odir);
    static void prep_etable(const string& filename, const econf_t& e_conf, const string& odir);
};
