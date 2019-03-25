CC=g++
EXE1=graphone32
EXE2=graphone64

CFLAGS=-O3 -g -Wall -std=gnu++11 -march=native -fopenmp -DOVER_COMMIT -DTBB -ltbb
#CFLAGS=-g -Wall -std=gnu++17 -march=native -fopenmp -DOVER_COMMIT  

SRC=main.cpp \
	cf_info.cpp\
	graph.cpp\
	sgraph.cpp\
	str.cpp\
	stringkv.cpp\
	enumkv.cpp\
	typekv.cpp\
	multi_graph.cpp\
	prop_encoder.cpp\
	nt_to_edge.cpp\
	csv_to_edge.cpp\
	io_driver.cpp\
	util.cpp\
	plaingraph_test.cpp\
	multigraph_test.cpp\
	lanl15_test.cpp\
	#rset.cpp\
	#query_triple.cpp\
	#graph_query.cpp\
	#llama_test.cpp\
	
#weight_graph.cpp\
#lite_sgraph.cpp\
#p_sgraph.cpp\
#propkv.cpp\
	propkv.h\
	mlabel.cpp\
	mlabel.h\


SRC64= lubm_test.cpp\
	ldbc_test.cpp\
	#darshan_to_edge.cpp\
	#darshan_test.cpp\

HEADER=graph.h\
	cf_info.h\
	sgraph.h\
	sgraph2.h\
	skv.h\
	prop_encoder.h\
	str.h\
	num.h\
	stringkv.h\
	numberkv.h\
	enumkv.h\
	mixkv.h\
	typekv.h\
	type.h\
	graph_base.h\
	io_driver.h\
	onegraph.h\
	onekv.h\
	mem_iterative_analytics.h\
	stream_analytics.h\
	nt_to_edge.h\
	csv_to_edge.h\
	plain_to_edge.h\
	multi_graph.h\
	util.h\
	new_type.h\
	new_func.h\
	test.h\
	str2sid.h\
	#rset.h\
	#query_node.h\
	#query_triple.h\
    #query_clause.h\
	#snap_iterative_analytics.h\
	#ext_iterative_analytics.h\
	#ext2_iterative_analytics.h\
	#darshan_to_edge.h\

#	lite_sgraph.h\
#	p_sgraph.h\

#INCLUDES= -Iantlr4/include -Iantlr4
#LIBDIRS= -Lantlr4/lib
INCLUDES=-Iinclude 
LIBDIRS=

DEPS=$(SRC) $(SRC64) $(HEADER)

all:${EXE1} ${EXE2}

${EXE1}: $(DEPS)
	$(CC) $(CFLAGS) -DB32 -DPLAIN_GRAPH $(INCLUDES) $(LIBDIRS) $(SRC)  -o ${EXE1} -laio

${EXE2}: $(DEPS)
	$(CC) $(CFLAGS) -DB64 -DPLAIN_GRAPH $(INCLUDES) $(LIBDIRS) $(SRC) ${SRC64} -o ${EXE2} -laio

clean:
	rm ${EXE1} ${EXE2} 
