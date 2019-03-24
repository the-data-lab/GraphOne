#include "numberkv.h"
#include "graph.h"
template <>
status_t numberkv_t<int>::batch_update(const string& src, const string& dst, propid_t pid /* = 0 */)
{
    edgeT_t<int> edge; 
    edge.src_id = g->get_sid(src.c_str());
    edge.dst_id = strtol(src.c_str(),0, 10);
    return batch_edge(edge);
}

template <>
status_t numberkv_t<uint64_t>::batch_update(const string& src, const string& dst, propid_t pid /* = 0 */)
{
    edgeT_t<uint64_t> edge; 
    edge.src_id = g->get_sid(src.c_str());
    edge.dst_id = strtol(src.c_str(),0, 10);
    return batch_edge(edge);
}
