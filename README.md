# GraphOne
This repository is for following FAST'19 paper: "GraphOne: A Data Store for Real-time Analytics on Evolving Graphs"

The repository is a storage engine (i.e. Data Store) for dynamic/evolving/streaming graph data. As of GraphOne, we only discussed ingestion for a single type of edges, called single stream graph. E.g. Twitter's follower-followee graph, or facebook's friendship graph. However, this code-base can ingest multi-stream graph, i.e. property graph. However, that a paper for that portion of codebase is under review. So, I am going to explain single stream graph. Send me an email, if your graph is multi-stream graph.

Two main attraction of the GraphOne are Data Visibility and GraphView abstractions. They are described below. 

## Data Visibility:
GraphOne offers a fine-grained ingestion, but leaves the visibility of data ingestion to analytics writer, called data visibility. That is within the same system an analytics can choose to work on fine-grained ingestion or coarse-grained ingestion.

## GraphView:
GraphOne offers a set of vertex-centric API to access the data. However different analytics may need different access pattern. We provide many access patterns. But two access patterns are highlighted. One is data access over whole data (Batch analytics), and anohter over a time window (Streaming analytics).  GraphView simplifies this access pattern. Kindly read the paper for different kind of APIs.


This piece of code is an academic prototype, and currently does not have a full-fledged query engine integrated. Your query or analytics need to use the GraphView APIs. We are working to improve the code quality, as well as support more type of analytics and Data. Please let us know if you have found any bug, or need a new feature. You can also contribute to the code. 

# Input Graph Data
GraphOne is designed to work with variety of input graph data. As of now, we only have interface with data from a file. We treat that as streaming/evolving/dynamic data. It would be possible to integrate with other data sources such as Apache Kafka, database change logs etc. I have listed few data types here that we support:

## Text Edge Data (With or Without Weights):
If you have downloaded some popular Graph Data sets from the Internet, it is possbily in this format. 

### Examaple 1: Data already contains IDs 
If you have downloaded subdomain graph, the data will look like following:
```
  0       28724358
  2       32513855
  2       33442782
  2       39494429
  2       49244790
```
This data already contains the ID, so we can directly use these as vertex IDs. We do have this option.



### Example 2: Data is in completely raw format
If you have downloaded LANL netflow data, the data will look like following:
```
172800,0,Comp348305,Comp370444,6,Port02726,80,0,5,0,784
172800,0,Comp817584,Comp275646,17,Port97545,53,1,0,77,0
172800,0,Comp654013,Comp685925,6,Port26890,Port94857,6,5,1379,1770
172800,0,Comp500631,Comp275646,17,Port62938,53,1,0,64,0
172800,0,Comp500631,Comp275646,17,Port52912,53,1,0,64,0
```

Here the vertices are in string format, and also have a complex weight. We do provide interface to convert these string to ID. We do support such graphs. We have written many plug-in to parse the text file and ingest one edge at a time.

## Binary Edge Data (With or Without Weights): 
It is same as above, but data is not in text format, but in binary format. This is not much different than handling above data. The parsing code is much simplified. 

# Input Graph Data (With Variable Size Weight)
Discription will be updated later.

# Help
## Building
If you want to install intel tbb, then current MakeFile is sufficient. Just run following command to build GraphOne:

  `make`
  
  It will produce two executables: graphone32 and graphone64 using -O3 flag. Otherwise, remove -DTBB and -ltbb from the command line flags at line 5.
  
  If you need to build the graph in debug mode. Then comment the line 5, and uncomment the line 6 of Makefile. and call `make` again.
  
  
  ## Running
  If you have used getopt.h in any of your c++ coding, then you will be able to figure out following command-line options. 
  ```
  {"vcount",      required_argument,  0, 'v'},
  {"help",        no_argument,        0, 'h'},
  {"idir",        required_argument,  0, 'i'},
  {"odir",        required_argument,  0, 'o'},
  {"category",    required_argument,  0, 'c'},
  {"job",         required_argument,  0, 'j'},
  {"residue",     required_argument,  0, 'r'},
  {"qfile",       required_argument,  0, 'q'},
  {"fileconf",    required_argument,  0, 'f'},
  {"threadcount", required_argument,  0, 't'},
  ```
       
`Example1 (Can be used as Benchmark):`
   `./graphone32 -i ./kron21_16/edge_file/ -o ./del2/ -c 0 -j 25 -v 2097152`

This command ingests the data from binary files present in kron21_16/edge_file/ directory, where vertex count is 2097152. `c` is just another way for us to separate some categories of testcases. `del2` is the output directory where we write the data.  The files inside kron21_16/edge_file/ directory have graph data in binary edge list format.

You can generate such a graph file using https://github.com/pradeep-k/gConv/tree/master/g500_gen code.

`Example 2 (No Benchmarking):`
   `./graphone32 -i kron_21_16/text_file/ -o ./del2/ -c 0 -j 18 -v 2097152`

This command ingests the data from text files present in kron21_16/text_file/ directory, where vertex count is 2097152. `c` is just another way for us to separate some categories of testcases. `del2` is the output directory where we write the data. The files inside kron21_16/text_file/ directory have graph data in text edge list format. The data does not need to convert to ID format, as they are already in ID format. This test case is good for all the data that you would download from Internet such as twitter, orkut etc. that are available as benchmarks.

`Example 3 (No Benchmarking):`
   `./graphone32 -i lanl_2017/text_file/ -o ./del2/ -c 0 -j 35 -v 162315`

This command ingests the data from text files present in lanl_2017/text_file/ directory, where vertex count is 162315. `c` is just another way for us to separate some categories of testcases. `del2` is the output directory where we write the data. The files inside lanl_2017/text_file/ directory have graph data in text raw edge list format. The data need to convert to ID format, as the edges are in string format. This test case is good for all the data that you would download from Internet where data is format but are in string format, such as LANL 2017, etc. that are available as benchmarks.

 ## Ingestion Your Own Data (No Benchmarking)
 You possibly have your own graph data, and want to use GraphOne as a data store. This section describes how to write ingestion testcases. For writing analytics please refer to next section. 
 
 We have providede a very simple plugin so that you have to only change/write fewer stuffs to ingest your data. We will use LANL 2017 data to let you know, how to write a new plug-in. LANL 2017 (https://csr.lanl.gov/data/2017.html) have netflow logs, which looks like following:
 ```
172800,0,Comp348305,Comp370444,6,Port02726,80,0,5,0,784
172800,0,Comp817584,Comp275646,17,Port97545,53,1,0,77,0
172800,0,Comp654013,Comp685925,6,Port26890,Port94857,6,5,1379,1770
172800,0,Comp500631,Comp275646,17,Port62938,53,1,0,64,0
172800,0,Comp500631,Comp275646,17,Port52912,53,1,0,64,0
```

These have following meaning:
```
  Field Name	 Description
  Time	       The start time of the event in epoch time format
  Duration	   The duration of the event in seconds.
  SrcDevice	   The device that likely initiated the event.
  DstDevice	   The receiving device.
  Protocol	   The protocol number.
  SrcPort	     The port used by the SrcDevice.
  DstPort	     The port used by the DstDevice.
  SrcPackets	 The number of packets the SrcDevice sent during the event.
  DstPackets	 The number of packets the DstDevice sent during the event.
  SrcBytes	   The number of bytes the SrcDevice sent during the event.
  DstBytes	   The number of bytes the DstDevice sent during the event.
 ```
 You need to perform following steps to ingest this data (the code is already present). 
  
 - Identify Vertices :  Lets say that we want to use srcDevice and dstDevice as source vertex ID and destination vertex ID, and rest other as edge weight. 
 
 - Write a C++ Structure for the Weight: In the file new_type.h, write following structure, and two typedef.

```
//------- LANL 2017 -----
struct netflow_weight_t {
    uint32_t time;
    uint32_t duration;
    uint32_t protocol;
    uint16_t src_port;
    uint16_t dst_port;
    uint32_t src_packet;
    uint32_t dst_packet;
    uint32_t src_bytes;
    uint32_t dst_bytes;
};

typedef dst_weight_t<netflow_weight_t> netflow_dst_t;
typedef edgeT_t<netflow_dst_t> netflow_edge_t;
```

- Write a Parser: In the file new_func.h, write a specialized template function for this function:

```
  //-----Two high level functions can be used by many single stream graph ----
  template <class T>
  index_t parsefile_and_insert(const string& textfile, const string& ofile, pgraph_t<T>* pgraph)
  {
      FILE* file = fopen(textfile.c_str(), "r");
      assert(file);
      cout << "No plugin found for reading and parsing the input files" << endl;
      assert(0);
      return 0;
  }
```
  
Here is how I wrote a plug-in for netflow data:
```
  template <>
  inline index_t parsefile_and_insert<netflow_dst_t>(const string& textfile, const string& ofile, pgraph_t<netflow_dst_t>* pgraph)
  {
      FILE* file = fopen(textfile.c_str(), "r");
      assert(file);

      edgeT_t<netflow_dst_t> netflow;
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
```

- Write Down a Testcase to Ingest: In the file plaingraph_test.cpp, see the huge switch-case statement, choose a number that is un-used as switch value. Let's choose 35, and write down following code. We will implement `ingestion_fulld()` next.
```
        case 35://text to our format
            ingestion_fulld<netflow_dst_t>(idir, odir, parsefile_and_insert);
            break;
```   

The function ingestion_fulld() have some nomenclature, you don't have to worry about that. The function is implemented like this:
```
template <class T>
void ingestion_fulld(const string& idir, const string& odir, typename callback<T>::parse_fn_t parsefile_fn)
{
    plaingraph_manager_t<T> manager;
    manager.schema_plaingraphd(); \\Schema is already defined for directed single stream graph. We have three types of schema: directed, undirected and unidirected. You don't have to write it.
    manager.setup_graph_vert_nocreate(v_count); \\ saying that your plugin will be coverting string to vertex ID
    //g->create_wthread(); \\This is durable phase. Uncomment it if you want the edges to be made durable.
    g->create_snapthread();\\ This does the archiving in the background
    manager.prep_graph_fromtext(idir, odir, parsefile_fn);\\ parsefile_fn is the plug-in you wrote in new_func.h
    //g->store_graph_baseline(); \\ If you want to store the adjacency store data at the end.
}
```

- Executing the Testcase: Run this command: 
 `./graphone32 -i lanl_2017/text_file/ -o ./del2/ -c 0 -j 35 -v 162315`
