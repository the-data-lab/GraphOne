#pragma once

void plain_test(vid_t v_count, const string& idir, const string& odir, int job);
void multigraph_test(vid_t v_count, const string& idir, const string& odir, int job);

void lubm_test(const string& typefile, const string& idir, const string& odir, int job);
void ldbc_test(const string& conf_file, const string& idir, const string& odir, int job);
void darshan_test0(const string& conf_file, const string& idir, const string& odir);

