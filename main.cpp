
#include <omp.h>
#include <iostream>
#include <getopt.h>
#include <stdlib.h>
#include "graph.h"

#define no_argument 0
#define required_argument 1 
#define optional_argument 2

void plain_test(vid_t v_count, const string& idir, const string& odir, int job);
void dist_test(vid_t v_count, const string& idir, const string& odir, int job);
void multigraph_test(vid_t v_count, const string& idir, const string& odir, int job);
void lubm_test(const string& typefile, const string& idir, const string& odir, int job);
void ldbc_test(const string& conf_file, const string& idir, const string& odir, int job);
void darshan_test0(const string& conf_file, const string& idir, const string& odir);

index_t residue = 0;
int THD_COUNT = 0;
vid_t _global_vcount = 0;
index_t _edge_count = 0;
int _dir = 0;//undirected
int _persist = 0;//no
int _source = 0;//text

void print_usage() 
{
    string help = "./exe options.\n";
    help += " --help -h: This message.\n";
    help += " --vcount -v: Vertex count\n";
    help += " --edgecount -e: Edge count, required only for streaming analytics as exit criteria.\n";
    help += " --idir -i: input directory\n";
    help += " --odir -o: output directory. This option will also persist the edge log.\n";
    //help += " --category -c: 0 for single stream graphs. Default: 0\n";
    help += " --job -j: job number. Default: 0\n";
    help += " --threadcount --t: Thread count. Default: Cores in your system - 1\n";
    help += " --direction -d: Direction, 0 for undirected, 1 for directed, 2 for unidirected. Default: 0(undirected)\n";
    help += " --source  -s: Data source. 0 for text files, 1 for binary files. Default: text files\n";
    help += " --residue or -r: Various meanings.\n";

    cout << help << endl;
}

graph* g;

int main(int argc, char* argv[])
{
    const struct option longopts[] =
    {
        {"vcount",    required_argument,  0, 'v'},
        {"help",      no_argument,        0, 'h'},
        {"idir",      required_argument,  0, 'i'},
        {"odir",      required_argument,  0, 'o'},
        {"category",   required_argument,  0, 'c'},
        {"job",       required_argument,  0, 'j'},
        {"residue",   required_argument,  0, 'r'},
        {"qfile",     required_argument,  0, 'q'},
        {"fileconf",  required_argument,  0, 'f'},
        {"threadcount",  required_argument,  0, 't'},
        {"edgecount",  required_argument,  0, 'e'},
        {"direction",  required_argument,  0, 'd'},
        {"source",  required_argument,  0, 's'},
        {0,			  0,				  0,  0},
    };

	int o;
	int index = 0;
	string typefile, idir, odir;
    string queryfile;
    int category = 0;
    int job = 0;
	THD_COUNT = omp_get_max_threads()-1;// - 3;
    
    //int i = 0;
    //while (i < 100000) { usleep(10); ++i; }
    g = new graph; 
	while ((o = getopt_long(argc, argv, "i:c:j:o:q:t:f:r:v:e:d:s:h", longopts, &index)) != -1) {
		switch(o) {
			case 'v':
				#ifdef B64
                sscanf(optarg, "%ld", &_global_vcount);
				#elif B32
                sscanf(optarg, "%d", &_global_vcount);
				#endif
				cout << "Global vcount = " << _global_vcount << endl;
				break;
			case 'h':
				print_usage();
                return 0;
                break;
			case 'i':
				idir = optarg;
				cout << "input dir = " << idir << endl;
				break;
            case 'c':
                category = atoi(optarg);
				break;
            case 'j':
                job = atoi(optarg);
				break;
			case 'o':
				odir = optarg;
                _persist = 1;
				cout << "output dir = " << odir << endl;
				break;
            case 'q':
                queryfile = optarg;
                break;
            case 't':
                //Thread thing
                THD_COUNT = atoi(optarg);
                break;
            case 'f':
                typefile = optarg;
                break;
            case 'e':
                sscanf(optarg, "%ld", &_edge_count);
                break;
            case 'd':
                sscanf(optarg, "%d", &_dir);
                break;
            case 's':
                sscanf(optarg, "%d", &_source);
                break;
            case 'r':
                sscanf(optarg, "%ld", &residue);
                cout << "residue (multi-purpose) value) = " << residue << endl;
                break;
			default:
                cout << "invalid input " << endl;
                print_usage();
                return 1;
		}
	}
    cout << "Threads Count = " << THD_COUNT << endl;
    g->set_odir(odir);
    switch (category) {
        case 0:
        plain_test(_global_vcount, idir, odir, job);
            break;
        case 1:
        multigraph_test(_global_vcount, idir, odir, job);
            break;
#ifdef B64
        case 2:
        lubm_test(typefile, idir, odir, job);
            break;
        case 3:
            ldbc_test(typefile, idir, odir, job);
            break;
        /*
        case 5:
            darshan_test0(typefile, idir, odir);
            break;
        */
#endif
        default:
            break;
    }
    return 0;
}
