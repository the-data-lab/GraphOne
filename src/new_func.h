#include "type.h"

//-----Two high level functions can be used by many single stream graph ----
//----- The generic versions are for binary files/buffers.------
//----- Specialized versions are for text files/buffers.------

//uses buffered read, one binary edge at a time, and then insert
template <class T>
index_t file_and_insert(const string& textfile, const string& ofile, pgraph_t<T>* pgraph) 
{
    FILE* file = fopen(textfile.c_str(), "r");
    assert(file);
    
    edgeT_t<T> edge;
    index_t icount = 0;
    ssize_t size = 0;
    //tmp_blog_t<T> edges(64);
    while((size = fread(&edge, sizeof(edge), 1, file)) > 0) {
        //pgraph->batch_edge(&edges, edge);
        pgraph->batch_edge(edge);
        icount++;
    }
    if (size < 0) {
        assert(0);
    }
    
    fclose(file);
    return icount;
}

template <class T>
index_t buf_and_insert(const char* buf , pgraph_t<T>* pgraph, index_t count) 
{
    index_t edge_count = count/sizeof(edgeT_t<T>);
    edgeT_t<T>* edges = (edgeT_t<T>*)buf;
    for (index_t i = 0; i < edge_count; ++i) {
        pgraph->batch_edge(edges[i]);
    }
    return edge_count;
}

template <class T>
index_t parsefile_and_insert(const string& textfile, const string& ofile, pgraph_t<T>* pgraph) 
{
    cout << "No plugin found for reading and parsing the buffers" << endl;
    assert(0);
    return 0;
}

template <class T>
index_t parsebuf_and_insert(const char* buf , pgraph_t<T>* pgraph, index_t count) 
{
    cout << "No plugin found for reading and parsing the buffers" << endl;
    assert(0);
    return 0;
}

//--------------- netflow functions ------------------
// Actual parse function, one line at a time
inline index_t parse_netflow_line(char* line, edgeT_t<netflow_dst_t>& netflow) 
{
    if (line[0] == '%' || line[0]=='#') {
        return eNotValid;
    }
    
    //const char* del = ",\n";
    char* token = 0;
    
    token = strtok_r(line, ",\n", &line);
    netflow.dst_id.second.time = atoi(token);
    token = strtok_r(line, ",\n", &line);
    netflow.dst_id.second.duration = atoi(token);
    
    token = strtok_r(line, ",\n", &line);
    netflow.src_id = g->type_update(token);
    token = strtok_r(line, ",\n", &line);
    set_dst(netflow, g->type_update(token));
    
    token = strtok_r(line, ",\n", &line);
    netflow.dst_id.second.protocol = atoi(token);
    
    token = strtok_r(line, ",\n", &line);
    if (token[0] == 'P') {
        netflow.dst_id.second.src_port = atoi(token+4);
    } else {
        netflow.dst_id.second.src_port = atoi(token);
    }
    token = strtok_r(line, ",\n", &line);
    if (token[0] == 'P') {
        netflow.dst_id.second.dst_port = atoi(token+4);
    } else {
        netflow.dst_id.second.dst_port = atoi(token);
    }
    token = strtok_r(line, ",\n", &line);
    
    netflow.dst_id.second.src_packet = atoi(token);
    token = strtok_r(line, ",\n", &line);
    netflow.dst_id.second.dst_packet = atoi(token);
    token = strtok_r(line, ",\n", &line);
    netflow.dst_id.second.src_bytes = atoi(token);
    token = strtok_r(line, ",\n", &line);
    netflow.dst_id.second.dst_bytes = atoi(token);
        
    return eOK;
}

// Actual parse function, one line at a time
inline index_t parse_plaingraph_line(char* line, edgeT_t<dst_id_t>& edge)
{
    if (line[0] == '%' || line[0]=='#') {
        return eNotValid;
    }
    
    const char* del = " \t\n";
    char* token = 0;
    
    #ifdef B32
    token = strtok_r(line, del, &line);
    sscanf(token, "%u", &edge.src_id);
    token = strtok_r(line, del, &line);
    sscanf(token, "%u", &edge.dst_id.sid);
    #else
    token = strtok_r(line, del, &line);
    sscanf(token, "%lu", &edge.src_id);
    token = strtok_r(line, del, &line);
    sscanf(token, "%lu", &edge.dst_id.sid);
    #endif

    return eOK;
}

template <>
inline index_t parsebuf_and_insert<netflow_dst_t>(const char* buf, pgraph_t<netflow_dst_t>* pgraph, index_t count) 
{
    
    if (0 == buf) {
        return 0;
    }
    
    edgeT_t<netflow_dst_t> netflow;
    index_t icount = 0;
    const char* start = 0;
    const char* end = 0;
    //const char* buf_backup_for_debug = buf; 
    char  sss[512];
    char* line = sss;
        
    start = buf;
    buf = strchr(start, '\n');

    while (buf) {
        end = buf;
        memcpy(sss, start, end - start); 
        sss[end-start] = '\0';
        line = sss;
        if (eOK == parse_netflow_line(line, netflow)) {
            pgraph->batch_edge(netflow);
        }
        start = buf + 1;
        buf = strchr(start, '\n');
        icount++;
    }
    return icount;
}

template <>
inline index_t parsefile_and_insert<netflow_dst_t>(const string& textfile, const string& ofile, pgraph_t<netflow_dst_t>* pgraph) 
{
    FILE* file = fopen(textfile.c_str(), "r");
    assert(file);
    
    edgeT_t<netflow_dst_t> netflow;
    index_t icount = 0;
	char sss[512];
    char* line = sss;
    tmp_blog_t<netflow_dst_t> edges(64);
    while (fgets(sss, sizeof(sss), file)) {
        line = sss;
        if (eOK == parse_netflow_line(line, netflow)) {
            pgraph->batch_edge(&edges, netflow);
            //pgraph->batch_edge(netflow);
        }
        icount++;
    }
    
    fclose(file);
    return icount;
}
//---------------netflow functions done---------

template <>
inline index_t parsebuf_and_insert<dst_id_t>(const char* buf, pgraph_t<dst_id_t>* pgraph, index_t count) 
{
    
    if (0 == buf) {
        return 0;
    }
    
    edgeT_t<dst_id_t> edge;
    index_t icount = 0;
    const char* start = 0;
    const char* end = 0;
    //const char* buf_backup_for_debug = buf; 
    char  sss[512];
    char* line = sss;
        
    start = buf;
    buf = strchr(start, '\n');

    while (buf) {
        end = buf;
        memcpy(sss, start, end - start); 
        sss[end-start] = '\0';
        line = sss;
        if (eOK == parse_plaingraph_line(line, edge)) {
            pgraph->batch_edge(edge);
        }
        start = buf + 1;
        buf = strchr(start, '\n');
        icount++;
    }
    return icount;
}

template <>
inline index_t parsefile_and_insert<dst_id_t>(const string& textfile, const string& ofile, pgraph_t<dst_id_t>* pgraph) 
{
    FILE* file = fopen(textfile.c_str(), "r");
    assert(file);
    
    edgeT_t<dst_id_t> edge;
    index_t icount = 0;
	char sss[512];
    char* line = sss;

    while (fgets(sss, sizeof(sss), file)) {
        line = sss;
        
        if (eOK == parse_plaingraph_line(line, edge)) {
            pgraph->batch_edge(edge);
        }
        icount++;
    }
    
    fclose(file);
    return icount;
}


/* Use this function to convert text file to binary file*/
template <class T>
index_t parsefile_to_bin(const string& textfile, const string& ofile, pgraph_t<T>*pgraph)
{
	size_t file_size = fsize(textfile.c_str());
    size_t estimated_count = file_size/sizeof(edgeT_t<T>);
    
    
    FILE* file = fopen(textfile.c_str(), "r");
    assert(file);
    
    edgeT_t<T>* netflow_array = (edgeT_t<T>*)calloc(estimated_count, 
                                                      sizeof(edgeT_t<T>));
    assert(netflow_array);

    edgeT_t<T>* netflow = netflow_array;
    
    index_t icount = 0;
	char sss[512];
    char* line = sss;

    while (fgets(sss, sizeof(sss), file)) {
        line = sss;
        parse_netflow_line(line, *netflow);
        netflow++;
        icount++;
    }
    
    fclose(file);
    
    //write the binary file
    file = fopen(ofile.c_str(), "wb");
    fwrite(netflow_array, sizeof(edgeT_t<T>), icount, file);
    fclose(file);
    
    free(netflow_array);
    return 0;
}

