# GraphOne
This repository is for following two papers:  
**FAST'19 paper**: ["GraphOne: A Data Store for Real-time Analytics on Evolving Graphs"](https://www.usenix.org/system/files/fast19-kumar.pdf)  
**ACM Transaction on Storage'20**: "GraphOne: A Data Store for Real-time Analytics on Evolving Graphs".  Discusses the concurrent anlaytics mechanism, and many new things and results. 
See at the end for bibliography for citation.


**Latest revision V0.1 contains simplified options than V0.0 release.** We are continously improving GraphOne through code changes as well as  disseminating those information through peer reviewed papers and journals. A number of changes have been made, and are summarized here. A detailed description can be found in this README documents. We now have less number of command line options to work with, clear in-memory/durable execution options. The GraphView APIs are simplified, now there is just one flag to pass with different pre-defined flag values. Allowed values of flags include a thread creation flag. Hope to write more about it in future. **Feel free to raise an issue in github, if you have any problem in using GraphOne.** 

**Coming Soon:** Version V0.2 coming soon with automatic compaction and eviction, as well as APIs for differntial data access.  
Distributed GraphOne is coming soon as well.


The repository is a storage engine (i.e. Data Store) for dynamic/evolving/streaming graph data. As of GraphOne, we only discussed ingestion for a single type of edges, called single stream graph. E.g. Twitter's follower-followee graph, or facebook's friendship graph. However, this code-base can ingest multi-stream graph, i.e. multi-stream property graph. However, a paper for that portion of codebase is under review. So, I am going to explain single stream graph. Send me an email, if your graph is multi-stream graph.

Two main attraction of the GraphOne are Data Visibility and GraphView abstractions. They are described below. 

## Data Visibility:
GraphOne offers a fine-grained ingestion, but leaves the visibility of data ingestion to analytics writer, called data visibility. That is within the same system an analytics can choose to work on fine-grained ingestion or coarse-grained ingestion.

## GraphView:
GraphOne offers a set of vertex-centric API to access the data. However different analytics may need different access pattern. We provide many access patterns. But two access patterns are highlighted.  
- **Batch Analytics using Static View**: When an user want to run an analytic over whole data, treating the current data as static, it is called batch analytics. We provide *Static View* for such analytics even while the underlying data store is evolving due to continous arrival of data.  
- **Stream Analytics using Stream View**: The new focus of today's anlaytics world is to continously run computations so that you will always have updated results. The main idea here is to reuse the results of previously run analytics on slightly older data to get the latest results. The implemention technique in GraphOne is use *Stream View* for such computation. You call an API to update the view to latest data, and then run your increment computation steps on it utilizing the results of the older computation. The features in stream view will let you identify the sets of vertices and/or edges that have changed so that you know how your computation can take advantage of what has changed. We additionally provide a time-window implementation for stream view. And customize it for stateless and stateful analytics both.

- **Concurrent Real-time Analytics**: As we separate computation from data management using GraphView, we are able to run many different analytics, including any of batch or stream analytics together from the same data-store, yes, no separate memory footprint for data if you running more than one analytics.

**`Example:`**
- Static View: `snap_t<T>* snaph = create_static_view(pgraph, STALE_MASK|V_CENTRIC);`
- Stream View: `sstream_t<T>* sstreamh = reg_sstream_view(pgraph, stream_fn, STALE_MASK|V_CENTRIC|C_THREAD);`

GraphView simplifies all this access pattern, and many other stuffs by using `static` and `sstreamh` for all your later access. Kindly read the papers for different kind of APIs.

**`Graph View Flags`**: There are three important flags in create-\*-view() APIs, STALE_MASK, SIMPLE_MASK, PRIVATE_MASK.

**Neighther STALE nor SIMPLE:** APIs do have access to non-archived edges, but get-nebrs-\*() won't return it. Get the non-archived edges using get-nonarchieved-edges() API, and process yourself. 

**STALE_MASK:** Use this if you do not want access of non-archived edges. The edges are not available internally to the API.

**SIMPLE_MASK:** Use this if you do wnat to access of non-archived edges using get-nebrs-\*() API.

**PRIVATE_MASK:** It will copy the non-archived edges in a buffer. Useful for first option above.

Don't mix stale and simple flags.  


This piece of code is an academic prototype, and currently does not have a full-fledged query engine integrated. Your query or analytics need to use the GraphView APIs. We are working to improve the code quality, as well as support more type of analytics and Data. Please let us know if you have found any bug, or need a new feature. **You can also contribute to the code.** 

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
This data already contains the ID, so we can directly use these as vertex IDs. Some of them will also have weights. We do have options to handle such data. However, be aware that the numbers that you see here must be in the range of *\[0 vertex count\)*. If not, treat this dataset as next one, i.e. wothout IDs.



### Example 2: Data is in completely raw format (without IDs)
If you have downloaded LANL netflow data, the data will look like following:
```
172800,0,Comp348305,Comp370444,6,Port02726,80,0,5,0,784
172800,0,Comp817584,Comp275646,17,Port97545,53,1,0,77,0
172800,0,Comp654013,Comp685925,6,Port26890,Port94857,6,5,1379,1770
172800,0,Comp500631,Comp275646,17,Port62938,53,1,0,64,0
172800,0,Comp500631,Comp275646,17,Port52912,53,1,0,64,0
```

Here the vertices are in string format, and also have a complex weight. We do provide interface to convert these string to ID. We do support such graphs. We have written many plug-in to parse the text file and ingest one edge at a time. You may have to write one based on how the data looks like.

## Binary Edge Data (With or Without Weights): 
It is same as above, but data is not in text format, but in binary format. This is not much different than handling above data. The parsing code is much simplified. 

# Input Graph Data (With Variable Size Weight)
Discription will be updated later.

# Help
## Building
We need to install Intel tbb on your machine. Then, Just run following command to build GraphOne:

  `mkdir build`
  `cd build`
  `cmake ../`
  `make`
  
  It will produce two executables: graphone32 and graphone64 using -O3 flag. 
  
 **Building without Intel TBB**: We have a replacement for intel TBB, but it is only for prototype, as its performance is very slow. TBB is used only when your data contains no IDs. So, we use TBB for converting the text to vertex ID. If you don't need that use the one provided by us. An example is our debug version If you need to build the graph in debug mode. Then comment the line 9, and uncomment the line 10, and remove -ltbb from line 14 of CMakeLists.txt, and call `above four commands`. 
  
  
  ## Running
  We assume you are in build folder.  If you have used getopt.h in any of your c++ coding, then you will be able to figure out following command-line options. 
  ```
  {"vcount",      required_argument,  0, 'v'},
  {"help",        no_argument,        0, 'h'},
  {"idir",        required_argument,  0, 'i'},
  {"odir",        required_argument,  0, 'o'},
  {"job",         required_argument,  0, 'j'},
  {"residue",     required_argument,  0, 'r'},
  {"threadcount", required_argument,  0, 't'},
  {"edgecount",  required_argument,  0, 'e'},
  {"direction",  required_argument,  0, 'd'},
  {"source",  required_argument,  0, 's'},
  ```
    
 Here is explantion:
 ```
 --help -h: This message.
 --vcount -v: Vertex count
 --edgecount -e: Edge count, required only for streaming analytics as exit criteria.
 --idir -i: input directory
 --odir -o: output directory. This option will also persist the edge log.
 --job -j: job number. Default: 0
 --threadcount --t: Thread count. Default: Cores in your system - 1
 --direction -d: Direction, 0 for undirected, 1 for directed, 2 for unidirected. Default: 0(undirected)
 --source  -s: Data source. 0 for text files, 1 for binary files. Default: text files
 --residue or -r: Various meanings.
 ```   

`Example1 (Batch Analytics at the end of Ingestion, Single Machine execution):`
 -  In Memory, undirected, text input:   `./graphone32 -i kron21_16/text_file/  -j 0 -v 2097152`
 -  Durable, undirected, text input:     `./graphone32 -i kron21_16/text_file/ -o ./db_dir/  -j 0 -v 2097152`
 -  Durable, directed, text input:       `./graphone32 -i kron21_16/text_file/ -o ./db_dir/  -j 0 -v 2097152 -d 1`
 -  In Memory, undirected, binary input: `./graphone32 -i kron21_16/edge_file/ -j 0 -v 2097152 -s 1`
 -  Durable, unidrected, binary input: `./graphone32 -i kron21_16/edge_file/ -o ./db_dir/  -j 0 -v 2097152 -s 1`
 -  Durable, directed, binary input: `./graphone32 -i kron21_16/edge_file/ -o ./db_dir/  -j 0 -v 2097152 -s 1 -d 1`


This command ingests the data from binary files present in kron21_16/edge_file/ directory, where vertex count is 2097152. `db_dir` is the output directory where we write the data.  The files inside kron21_16/text_file/ directory have graph data in text edge list format. The files inside kron21_16/edge_file/ directory have graph data in binary edge list format. This job (0) runs bfs from a fixed root treating the graph as undirected and directed(if -d1 is supplied).

You can generate a binary graph file using https://github.com/pradeep-k/gConv/tree/master/g500_gen code. And a text graph file from https://github.com/the-data-lab/gstore/tree/master/graph500-generator.

`Example 2: (Batch Analytics at the end of ingestion)`
   `./graphone32 -i lanl_2017/text_file/ -o ./db_dir/ -j 16 -v 162315`

This command ingests the data from text files present in lanl_2017/text_file/ directory, where vertex count is 162315.  `db_dir` is the output directory where we write the data. The files inside lanl_2017/text_file/ directory have graph data in text raw edge list format. The data need to convert to ID format, as the edges are in string format. This test case is good for all the data that you would download from Internet where data is in graph format but are in string format, such as LANL 2017, etc. that are available as benchmarks.

`Exmaple 3: Stream Analytics, Continous execution, exit at the end.`
- In memory, undirected, text input, BFS runs concurrently with data ingestion: `./graphone32 -i kron21_16/text_file/  -j 30 -v 2097152 -e 33554432 -r 1` <= r is 1 for one BFS. See concurrent analytics later. BFS runs concurrently to archiving, results in better performance as well as enables concurrent stream analytics.
- In memory, undirected, text input, BFS runs after each archiving in the archiving context: `./graphone32 -i kron21_16/text_file/  -j 31 -v 2097152 -e 33554432` As BFS run in the context of archiving, or archiving runs as a first step in the analytics (BFS), this mimicks prior streaming works. 

`Internal testcases: We have also written many internal testcases. Some of them are described here:`
 - `./graphone32 -i ~/data/kron-21/bin/ -j 1 -v 2097152 -s 1 -o ./out/` : -j1 only does logging, and runs bfs from the edge list data.
 - `./graphone32 -i ~/data/kron-21/bin/ -j 2 -v 2097152 -t2 -s1 -r 16` : -j2 does archiving at the rate of (1<<16) edges at a time. change -r flag to any number to change the archiving threshold.
 - `./graphone32 -i ~/data/kron-21/bin/ -j 3 -v 2097152 -t2 -s1 -o ./out/` : -j3 converts (1<<18) edges to adjacency list, and leaves the rest as edge list in the edge log. Useful for testing the BFS performance in a mix of adjacency list and edge list.
 -`./graphone32  -j 4 -v 2097152 -s 1 -i ./out/friend.elog -r 16`: -j 4 recovers adjacency list by using the durable edge log. -r 16 tells us that only (1<<16) edges will be converted to adjacency list at a time.
 - ignore, -j 5, 6 and 7.
 - -j9 is like -j 0 except that the data will be ingested from hard-drive directly. In previous cases, the data was first loaded in memory before, logging phase was called. Here, data will be read from disk and logging phase will be performed.
 - -j50 to -j56 are similar to -j2, expect that BFS/PR/etc will be called multiple times (10 times) to do the benchmarking. Always expects certain value of r.
 
 - -j57 to -j59 are written as simulators to estimate some internal things. I haven't verified those lately.

## Ingestion of Your Own Data
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

- Write Down a Testcase to Ingest: In the file plaingraph_test.cpp, see the huge switch-case statement, choose a number that is un-used as switch value. Let's choose 35, and write down following code. We will implement `ingestion_full()` next.
```
        case 17://text to our format
            ingestion_full<netflow_dst_t>(idir, odir) ;
            break;
```   

The function ingestion_full() have some nomenclature, you don't have to worry about that. The function is implemented like this:
```
template <class T>
void ingestion_full(const string& idir, const string& odir)
{
    plaingraph_manager_t<T> manager;
    manager.schema(_dir); \\Schema is already defined for directed single stream graph. We have three types of schema: directed, undirected and unidirected. You don't have to write it.
    manager.setup_graph_vert_nocreate(v_count); \\ saying that your plugin will be coverting string to vertex ID
    manager.prep_graph(idir, odir);
    manager.run_bfs();
    //g->store_graph_baseline(); \\ If you want to store the adjacency store data at the end.
}
```

- Executing the Testcase: Run this command: 
 `./graphone32 -i lanl_2017/text_file/ -o ./db_dir/ -j 17 -v 162315`
 
 ## --job or -j Values
 We have written many testcases for the paper as well as for your demonstration. `plain_to_edge.h` graph have a good wrapper than handles any single stream graph including plain, weighted, and or any arbitrary edge property, i.e. it will handle a property graph. It has useful functions for schema setup to know whether it is a plaingraph, weighted graph, or more general property graph. We have written BFS (many versions), PageRank, 1-Hop Query, 2-Hop Query, WCC (only streaming versions using stateless stream view) etc. See plain_to_edge.h for functions like `run_bfs`, `run_pr`, `run_1hop` etc. 

These functions when called from testcases, can do all the work for you. E.g. when you see the `test_ingestion` function in `plaingraph test.h` file, you will see how nicely they have been called to implement a testcase. So, you can replace run_bfs(), with run_pr(), and you are likely to run PageRank(). You can call them in sequence, to run many of them in sequence. For running them concurrently see next section.

So, check a j value in plaingraph_test.h, read the 5-6 lines of code, and put your favourite run_XXX() function, and you are performing the testcase you like. Therefore, in plaingraph_test.h, our focus has been to test different aspects of data management, hence you will find that many of the testcases call run_bfs() only. 

Here are the -j values (they can change when we provide more cases) :
```
       case 0: 
            test_ingestion<dst_id_t>(idir, odir); <= Normal: i.e. logging, archiving in parallel
            break;
        case 1:
            test_logging<dst_id_t>(idir, odir); <= Logging Only. To test logging speed of 1-thread
            break;
        case 2: 
            test_archive<dst_id_t>(idir, odir); <= Archiving Only: To test archiving speed when (1<< r) edges are batched.  Provide -r values in the command line. 
            break;
        case 3://leave some in the edge format
            test_mix<dst_id_t>(idir, odir); <== Want some edge to be left in the edge log, while (1<<r) edges in the adjacency list.
            break;
        case 4://recover from durable edge log
            recover_test0<dst_id_t>(idir, odir); <== recovery test
            break;
        case 5:
            recover_test<dst_id_t>(odir); <== recovery test
            break;
        case 6:
            prior_snap_test<dst_id_t>(odir); <= trying to run on an old snaphshot,
            break;
        case 7://SNB
            test_ingestion_snb<dst_id_t>(idir, odir); <== New development, ignore.
            break;
        //plain graph in text format with ID. 
        case 9: //Not for performance
            ingestion<dst_id_t>(idir, odir); <=== possibly going to be removed.
            break;
```
 
 # Concurrent Batch and/or Stream Analytcs
 The ACM TOS paper and my dissertation discusses more about concurrent anlaytics on the evolving graph. The idea here is to run many different as well as same type of analytics from the same data store. This is in contrast to prior works that can only execute on analytics at a time. Therefore, each analytics will require there own data-store to execute the analytics. GraphOne is first ever system to demostrate this capability. 
 
**Example**: Following job (-j 30) will let you run any number of concurrent streaming bfs. Pass -r value as 10 (-r 10) if you are interested in running 10 streaming BFS. We allocate random root for each BFS.
 
 ```
 case 30:
            multi_stream_bfs<dst_id_t>(idir, odir, stream_bfs, residue);
            break;
  ```
# Multi-Stream Graphs
TODO.
  
# Distributed GraphOne
Contact me for more information.

# Citatation
If you want to cite our paper, use the following bibtex entries: 
```
@inproceedings{kumar2019graphone,
  title={GraphOne: A Data Store for Real-Time Analytics on Evolving Graphs},
  author={Kumar, Pradeep and Huang, H Howie},
  booktitle={17th $\{$USENIX$\}$ Conference on File and Storage Technologies ($\{$FAST$\}$ 19)},
  pages={249--263},
  year={2019}
}

@article{10.1145/3364180,
author = {Kumar, Pradeep and Huang, H. Howie},
title = {GraphOne: A Data Store for Real-Time Analytics on Evolving Graphs},
year = {2020},
issue_date = {February 2020},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
volume = {15},
number = {4},
issn = {1553-3077},
url = {https://doi.org/10.1145/3364180},
doi = {10.1145/3364180},
journal = {ACM Trans. Storage},
month = jan,
articleno = {Article 29},
numpages = {40},
keywords = {stream analytics, unified graph data store, batch analytics, graph data management, Graph systems}
}

```

