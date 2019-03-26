# GraphOne
This repository is for following FAST'19 paper: "GraphOne: A Data Store for Real-time Analytics on Evolving Graphs"

The repository is a storage engine (i.e. Data Store) for dynamic/evolving/streaming graph data. Two main attraction of the GraphOne are Data Visibility and GraphView abstractions. They are described below. 

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
       
`Example1:`
   `./graphone32 -i ./kron21_16/edge_file/ -o ./del2/ -c 0 -j 25 -v 2097152`

This command ingests the data from binary files present in kron21_16/edge_file/ directory, where vertex count is 2097152. `c` is just another way for us to separate some categories of testcases. `del2` is the output directory where we write the data.  The files inside kron21_16/edge_file/ directory have graph data in binary edge list format.

You can generate such a graph file using https://github.com/pradeep-k/gConv/tree/master/g500_gen code.

`Example 2:`
   `./graphone32 -i /mnt/disk_huge_1/pradeepk/pradeep_graph/kron_21_16/text_file/ -o ./del2/ -c 0 -j 18 -v 2097152`

This command ingests the data from text files present in kron21_16/text_file/ directory, where vertex count is 2097152. `c` is just another way for us to separate some categories of testcases. `del2` is the output directory where we write the data. The files inside kron21_16/text_file/ directory have graph data in text edge list format. The data does not need to convert to ID format, as they are already in ID format. This test case is good for all the data that you would download from Internet such as twitter, orkut etc. that are available as benchmarks.

`Example 3:`
   


 
