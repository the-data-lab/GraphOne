#include "type.h"



//--------------- netflow functions ------------------
// Actual parse function, one line at a time
inline index_t parse_netflow_line(char* line, edgeT_t<netflow_dst_t>& netflow) 
{
    if (line[0] == '%') {
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
    netflow.dst_id.first = g->type_update(token);
    
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
//---------------netflow functions done---------


//-----Two high level functions can be used by many single stream graph ----
template <class T>
index_t parsefile_and_insert(const string& textfile, const string& ofile, pgraph_t<T>* pgraph) 
{
    FILE* file = fopen(textfile.c_str(), "r");
    assert(file);
    
    edgeT_t<T> netflow;
    index_t icount = 0;
	char sss[512];
    char* line = sss;

    while (fgets(sss, sizeof(sss), file)) {
        line = sss;
        if (eOK == parse_netflow_line(line, netflow)) {
            pgraph->batch_edge(netflow);
        }
        icount++;
    }
    
    fclose(file);
    return 0;
}


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

