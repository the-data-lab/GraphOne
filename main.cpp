
#include <omp.h>
#include <iostream>
#include <getopt.h>
#include <stdlib.h>
#include "graph.h"

#define no_argument 0
#define required_argument 1 
#define optional_argument 2

void plain_test(vid_t v_count, const string& idir, const string& odir, int job);
void multigraph_test(vid_t v_count, const string& idir, const string& odir, int job);
void lubm_test(const string& typefile, const string& idir, const string& odir, int job);
void ldbc_test(const string& conf_file, const string& idir, const string& odir, int job);
void darshan_test0(const string& conf_file, const string& idir, const string& odir);
//using namespace std;

graph* g;
int THD_COUNT = 0;

void ontology_lubm();
void fill_lubm_inference_type();

extern index_t residue;
index_t _edge_count = 0;

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
		{0,			  0,				  0,  0},
	};

	int o;
	int index = 0;
	string typefile, idir, odir;
    string queryfile;
    int convert = -1;
	vid_t v_count = 0;
    int job = 0;

    //Thread thing
	THD_COUNT = omp_get_max_threads()-1;// - 3;

    g = new graph; 
	while ((o = getopt_long(argc, argv, "i:c:j:o:q:t:f:r:v:e:h", longopts, &index)) != -1) {
		switch(o) {
			case 'v':
				#ifdef B64
                sscanf(optarg, "%ld", &v_count);
				#elif B32
                sscanf(optarg, "%d", &v_count);
				#endif
				cout << "v_count = " << v_count << endl;
				break;
			case 'h':
				cout << "Help coming soon" << endl;
				break;
			case 'i':
				idir = optarg;
				cout << "input dir = " << idir << endl;
				break;
            case 'c':
                convert = atoi(optarg);
				break;
            case 'j':
                job = atoi(optarg);
				break;
			case 'o':
				odir = optarg;
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
            case 'r':
                sscanf(optarg, "%ld", &residue);
                cout << "residue (multi-purpose) value) = " << residue << endl;
                break;
			default:
                cout << "invalid input " << endl;
                return 1;
		}
	}
	cout << "Total thds = " << THD_COUNT << endl;
    g->set_odir(odir);
    switch (convert) {
        case 0:
        plain_test(v_count, idir, odir, job);
            break;
        case 1:
        multigraph_test(v_count, idir, odir, job);
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
