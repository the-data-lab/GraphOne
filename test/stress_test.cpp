#include <cassert>
#include <future>
#include <iostream>
#include <omp.h>
#include <random>
#include <thread>
#include <syscall.h>
#include <unistd.h>
#include <vector>

// GraphOne includes
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Woverloaded-virtual"
#include "graph.h"
#include "graph_view.h"
#include "sgraph.h"
#include "stringkv.h"
#include "typekv.h"
#pragma GCC diagnostic pop

using namespace std;

// globals
graph* g;
int THD_COUNT = (omp_get_max_threads() -1);
volatile bool g_keep_going = true; // flag to stop all workers

struct Edge {
    uint64_t m_source;
    uint64_t m_destination;
};

namespace std {
template<> struct hash<::Edge>{ // hash function for ::Edge
private:
    // Adapted from the General Purpose Hash Function Algorithms Library
    // Author: Arash Partow - 2002
    // URL: http://www.partow.net
    // URL: http://www.partow.net/programming/hashfunctions/index.html
    // MIT License
    unsigned int APHash(uint64_t value) const {
       unsigned int hash = 0xAAAAAAAA;
       unsigned int i    = 0;
       char* str = (char*) &value;

       for (i = 0; i < sizeof(value); ++str, ++i) {
          hash ^= ((i & 1) == 0) ? (  (hash <<  7) ^ (*str) * (hash >> 3)) :
                                   (~((hash << 11) + ((*str) ^ (hash >> 5))));
       }

       return hash;
    }

public:
    size_t operator()(const ::Edge& e) const {
        return APHash((e.m_source << 32) + (e.m_destination & 0xFFFFFFFF));
    }
};
} // namespace std

static std::vector<Edge> generate_edge_stream(uint64_t num_vertices);
static void add_vertex(uint64_t vertex_id);
static void add_edge(uint64_t source, uint64_t destination, double weight);
static void remove_edge(uint64_t source, uint64_t destination);
static void update_edge(bool is_insert, uint64_t source, uint64_t destination, double weight = 0.0);
template<typename T>
static std::vector<T> permute(const std::vector<T>& array, uint64_t seed = 0);
static std::vector<uint64_t> generate_partitions(uint64_t num_edges, uint64_t num_threads);
static void worker_main(Edge* edges, uint64_t edges_sz);
static void validate(uint64_t num_vertices);

[[maybe_unused]]
static std::mutex g_cout_lock;
#define LOG(message) { auto thread_id = syscall(SYS_gettid); std::scoped_lock<std::mutex> lock{ g_cout_lock }; std::cout << "[Thread #" << thread_id << "] " << message << endl; }

int main(int argc, char* argv[]){
    const uint64_t num_threads = 16; // thread::hardware_concurrency(); // =8
    const uint64_t num_vertices = 33; //1ull << 14;

    uint64_t seed = random_device{}(); // random seed
    cout << "Random seed: " << seed << "\n";

    g = new graph;

    g->cf_info = new cfinfo_t*[2];

    // metadata (vertex dictionary)
    auto metadata = new typekv_t(); // vertex types or classes
    g->add_columnfamily(metadata); // cf_info[0] <- typekv_t

    // create the weighted edges
    auto weighted_edges = new p_ugraph_t(); // undirected
    weighted_edges->flag1 = weighted_edges->flag2 =1; // `1' is the vertex type
    g->add_columnfamily(weighted_edges); // weighted edges

    metadata->manual_setup(/* total number of vertices */ 128 * 1024 * 1024, /* create the vertices */ false);
    g->prep_graph_baseline();
    g->create_threads(/* archiving */ true, /* logging */ false);

    {
        cout << "Inserting " << num_vertices << " vertices ..." << endl;
        vector<uint64_t> vertices; vertices.reserve(num_vertices);
        for(uint64_t i = 1; i <= num_vertices; i++){ vertices.push_back(i); }
        vertices = permute(vertices, seed++);
        for(uint64_t i = 0; i < vertices.size(); i++){ add_vertex(vertices[i]); }
    }

    cout << "Generating the stream of edges ... " << endl;
    auto stream = generate_edge_stream(num_vertices);
    stream = permute(stream, seed++);
    uint64_t num_edges = stream.size();
    
    auto partitions = generate_partitions(num_edges, num_threads);
    assert(partitions.size() == num_threads);

    //---
    int count = 0;
    do {
		cout << "Updating " << num_edges << " edges with " << num_threads << " threads ... " << endl;
		uint64_t partition_start = 0;
		vector<thread> workers;
		for(uint64_t i = 0; i < num_threads; i++){
			if(partitions[i] == 0) break;
			uint64_t partition_end = partition_start + partitions[i];
			workers.emplace_back(worker_main, stream.data() + partition_start, partition_end - partition_start);
			partition_start = partition_end; // next iteration
		}

		// let it run for a bit
		LOG("ZzZ ... ");
		this_thread::sleep_for(0.1s);

		LOG("Waiting for all threads to terminate ...");
		g_keep_going = false;
		for(auto& w: workers) w.join();
		workers.clear();

		cout << "Build ..." << endl;
		g->waitfor_archive();

		//-----
    	cout << "Validate ..." << endl;
    	validate(num_vertices);
		weighted_edges->compress_graph_baseline();
		++count;
    } while (count <= 400);
    

    // It's enough it does not end with a seg fault...
    cout << "Done" << endl;

    return 0;
}

static
void worker_main(Edge* edges, uint64_t edges_sz){
    uniform_int_distribution<uint64_t> distribution(1, 800000);
    mt19937_64 rand {std::random_device{}()};


    while(g_keep_going){
		// first insert all edges in the list
		for(uint64_t i = 0; i < edges_sz; i++){
			double weight = distribution(rand);
			add_edge(edges[i].m_source, edges[i].m_destination, weight);
		}
        uint64_t delete_index = 0;
        // then remove them
        for( /* nop */ ; delete_index < edges_sz; delete_index++){
            remove_edge(edges[delete_index].m_source, edges[delete_index].m_destination);
        }

		/*
        // insert them again
        for(uint64_t insert_index = 0; insert_index < delete_index; insert_index++){
            double weight = distribution(rand);
            add_edge(edges[insert_index].m_source, edges[insert_index].m_destination, weight);
        }*/
    }
}

static
void dump_vertex(uint64_t source_name, lite_edge_t* neighbours, uint64_t neighbours_sz){
    auto labels = g->get_typekv();

    string str_source_name = to_string(source_name);
    uint64_t source_id = labels->get_sid(str_source_name.c_str());

    cout << "[DUMP] Vertex: " << source_name << ", id: " << source_id << ", degree: " << neighbours_sz << "\n";
    for(uint64_t i = 0; i < neighbours_sz; i++){
        uint64_t destination_name = get_sid(neighbours[i]);
        string str_destination_name = to_string(destination_name);
        uint64_t destination_id = labels->get_sid(str_destination_name.c_str());

        cout << "[" << i << "] destination: " << destination_name << ", id: " << destination_id << ", weight: " << neighbours[i].second.value_double << "\n";
    }
}

static
void validate(uint64_t num_vertices){
    auto labels = g->get_typekv();
    auto* view = create_static_view((pgraph_t<lite_edge_t>*) g->get_sgraph(1), SIMPLE_MASK); // global
    lite_edge_t* neighbours = new lite_edge_t[num_vertices];


    for(uint64_t source_name = 1; source_name <= num_vertices; source_name++){
        string str_source_name = to_string(source_name);
        uint64_t source_id = labels->get_sid(str_source_name.c_str());

        uint64_t degree = view->get_degree_out(source_id);
        assert(degree < num_vertices);
        view->get_nebrs_out(source_id, neighbours);

        // replace the internal ids with the vertex names
        for(uint64_t i = 0; i < degree; i++){
            uint64_t destination_id = get_sid(neighbours[i].first);
            string str_destination_name = g->get_typekv()->get_vertex_name(destination_id);
            set_sid(neighbours[i], stoull(str_destination_name));
        }

        // sort them by vertex name
        std::sort(neighbours, neighbours + degree, [](const lite_edge_t& l1, const lite_edge_t& l2){
            return get_sid(l1) < get_sid(l2);
        });

        // finally check all edges are present
        uint64_t expected = 2*source_name;
        for(uint64_t i = 0; i < degree; i++){
            uint64_t destination_name = get_sid(neighbours[i]);
            if(destination_name < source_name){
                if(source_name % destination_name != 0){
                    string str_destination_name = to_string(destination_name);
                    uint64_t destination_id = labels->get_sid(str_destination_name.c_str());
                    dump_vertex(source_name, neighbours, degree);
                    LOG("VALIDATION ERROR (<). Index: " << i << ", Source vertex: " << source_name << " [id: " << source_id << "]. Destination vertex: " << destination_name << " [id: " << destination_id << "]. MOD FAILURE");
                    throw "Validation error <";
                }
            } else if (destination_name == source_name){
                dump_vertex(source_name, neighbours, degree);
                LOG("VALIDATION ERROR (<). Index: " << i << ", Source vertex: " << source_name << " [id: " << source_id << "]. SAME VERTEX ID")
		//throw "Validation error ==";
            } else { // neighbours[i].first > vertex_name
                if(destination_name != expected){
                    string str_destination_name = to_string(destination_name);
                    uint64_t destination_id = labels->get_sid(str_destination_name.c_str());
                    dump_vertex(source_name, neighbours, degree);
                    LOG("VALIDATION ERROR (<). Index: " << i << ", Source vertex: " << source_name << " [id: " << source_id << "]. Destination vertex: " << destination_name << " [id: " << destination_id << "]. Expected: " << expected);
                    //throw "Validation error >";
                }
                expected += source_name;
            }
        }
        if(expected < num_vertices){
            //throw "Validation error (expected)";
        }
    }

    delete[] neighbours;
    delete_static_view(view);
}

static
void add_vertex(uint64_t vertex_id){
    string str_vertex_id = to_string(vertex_id);
    g->type_update(str_vertex_id);
}

static
void add_edge(uint64_t source, uint64_t destination, double weight){
    update_edge(true, source, destination, weight);
}

static
void remove_edge(uint64_t source, uint64_t destination) {
    update_edge(false, source, destination);
}

static
void update_edge(bool is_insert, uint64_t source, uint64_t destination, double weight){
    auto labels = g->get_typekv();

    string str_source = to_string(source);
    sid_t v0 = labels->get_sid(str_source.c_str());

    string str_destination = to_string(destination);
    sid_t v1 = labels->get_sid(str_destination.c_str());

//    LOG((is_insert ? "INSERT" : "REMOVE") << " " << source << " -> " << destination << " [" << v0 << " -> " << v1 << ", weight = " << weight << "]");

    weight_edge_t edge;
    set_src(edge, is_insert ? v0 : DEL_SID(v0));
    set_dst(edge, v1);
    edge.dst_id.second.value_double = weight;
    status_t status = eOK;

    //while( true ){
        status = ((p_ugraph_t*) g->get_sgraph(1))->batch_edge(edge);
        if(!((status == eOK || status == eEndBatch))) assert(0);
     //   this_thread::sleep_for(3ms);
    //}
    //assert(status == 0);
}

static
std::vector<Edge> generate_edge_stream(uint64_t num_vertices){
    vector<Edge> edges;
    for(uint64_t i = 1; i < num_vertices; i++){
        for(uint64_t j = 2 * i; j <= num_vertices; j+=i){
            edges.push_back(Edge{i, j});
        }
    }
    return edges;
}

static
std::vector<uint64_t> generate_partitions(uint64_t num_edges, uint64_t num_threads){
    vector<uint64_t> partitions;

    uint64_t total = 0;
    uint64_t this_size = 4;
    while(total < num_edges){
        if(total + this_size > num_edges) this_size = num_edges - total;

        partitions.push_back(this_size);

        total += this_size;
        this_size *= 2;
    }

    if(partitions.size() < num_threads){ // split the last partition
        uint64_t need = 1 + num_threads - partitions.size();
        uint64_t partition_sz = partitions.back() / need;
        uint64_t partition_mod = partitions.back() % need;
        partitions.pop_back();

        for(uint64_t i = 0; i < need; i++){
            partitions.push_back(partition_sz + (i < partition_mod));
        }
    } else { // collapse the last partitions
        uint64_t last_partition_sz = 0;
        while(partitions.size() > num_threads){
            last_partition_sz += partitions.back();
            partitions.pop_back();
        }
        partitions.back() += last_partition_sz;
    }

    return partitions;
}

// extracted from libcommon
namespace {

template<typename T>
struct Bucket{
    using random_generator_t = mt19937_64;

    const int m_bucket_no;
    random_generator_t m_random_generator;
    vector<T>** m_chunks;
    const int m_chunks_size;
    uint64_t m_permutation_sz; // the size of the local permutation
    T* m_permutation;

    Bucket(int id, uint64_t seed, uint64_t no_chucks) :
            m_bucket_no(id), m_random_generator(seed + m_bucket_no), m_chunks(nullptr), m_chunks_size(no_chucks),
            m_permutation_sz(0), m_permutation(nullptr){
        // initialise the chunks
        m_chunks = new vector<T>*[m_chunks_size];
        for(int i = 0; i < m_chunks_size; i++){
            m_chunks[i] = new vector<T>();
        }
    }

    ~Bucket(){
        if(m_chunks != nullptr){
            for(int i = 0; i < m_chunks_size; i++){
                delete m_chunks[i]; m_chunks[i] = nullptr;
            }
        }
        delete[] m_chunks; m_chunks = nullptr;

        // DO NOT DELETE m_permutation, THE CLASS DOES NOT OWN THIS PTR
        m_permutation = nullptr;
    }
};

} // anonymous namespace

template<typename T>
static void do_permute(T* array, uint64_t array_sz, uint64_t no_buckets, uint64_t seed){
    if(no_buckets > array_sz) no_buckets = array_sz;

    // initialise the buckets
    vector< unique_ptr< Bucket<T> > > buckets;
//    size_t bytes_per_element = compute_bytes_per_elements(size); // the original implementation used an array with variable-length data
    for(uint64_t i = 0; i < no_buckets; i++){
        buckets.emplace_back(new Bucket<T>(i, seed + i, no_buckets /*, bytes_per_element*/));
    }

    { // exchange the elements in the buckets
        auto create_partition = [&buckets](int bucket_id, uint64_t range_start /* inclusive */ , uint64_t range_end /* exclusive */){
            assert(bucket_id < (int) buckets.size());

            // first gather all buckets
            vector< vector<T>* > stores;
            for(size_t i = 0; i < buckets.size(); i++){
                Bucket<T>* b = buckets[i].get();
                stores.push_back( b->m_chunks[bucket_id] );
            }

            // current state
            Bucket<T>* bucket = buckets[bucket_id].get();
            uniform_int_distribution<size_t> distribution{0, buckets.size() -1};
            for(uint64_t i = range_start; i < range_end; i++){
                size_t target_bucket = distribution(bucket->m_random_generator);
                stores[target_bucket]->push_back(i);
            }
        };

        std::vector<future<void>> tasks;
        size_t range_start = 0, range_step = array_sz / no_buckets, range_end = range_step;
        uint64_t range_mod = array_sz % no_buckets;
        for(uint64_t i = 0; i < no_buckets; i++){
            if(i < range_mod) range_end++;
            tasks.push_back( async(launch::async, create_partition, i, range_start, range_end) );
            range_start = range_end;
            range_end += range_step;
        }
        // wait for all tasks to finish
        for(auto& t: tasks) t.get();
    }

    { // compute [sequentially] the size of each local permutation
        uint64_t offset = 0;
        for (uint64_t bucket_id = 0, sz = buckets.size(); bucket_id < sz; bucket_id++) {
            Bucket<T>* bucket = buckets[bucket_id].get();
            uint64_t size = 0;
            assert((size_t) bucket->m_chunks_size == buckets.size());
            for(int i = 0; i < bucket->m_chunks_size; i++){ size += bucket->m_chunks[i]->size(); }

            bucket->m_permutation = array + offset;
            bucket->m_permutation_sz = size;
            offset += size;
        }
    }

    { // perform a local permutation
        auto local_permutation = [&buckets/*, bytes_per_element*/](int bucket_id){
            // store all the values in a single array
            Bucket<T>* bucket = buckets[bucket_id].get();
            T* __restrict permutation = bucket->m_permutation; //new CByteArray(bytes_per_element, capacity);
            size_t index = 0;
            for(int i = 0; i < bucket->m_chunks_size; i++){
                vector<T>* vector = bucket->m_chunks[i];
                for(size_t j = 0, sz = vector->size(); j < sz; j++){
                    permutation[index++] = vector->at(j);
                }
            }

            // deallocate the chunks to save some memory
            for(int i = 0; i < bucket->m_chunks_size; i++){
                delete bucket->m_chunks[i]; bucket->m_chunks[i] = nullptr;
            }
            delete[] bucket->m_chunks; bucket->m_chunks = nullptr;

            // perform a local permutation
            if(bucket->m_permutation_sz >= 2){
                T* __restrict permutation = bucket->m_permutation;
                for(size_t i = 0; i < bucket->m_permutation_sz -2; i++){
                    size_t j = uniform_int_distribution<size_t>{i, bucket->m_permutation_sz -1}(bucket->m_random_generator);
                    std::swap(permutation[i], permutation[j]);  // swap A[i] with A[j]
                }
            }
        };

        std::vector<future<void>> tasks;
        for(size_t i = 0; i < no_buckets; i++){
            tasks.push_back( async(launch::async, local_permutation, i) );
        }
        // wait for all tasks to finish
        for(auto& t: tasks) t.get();
    }

    // there is no need to explicitly concatenate all the results together as before (CByteArray::merge)
    // done
}

static
unique_ptr<uint64_t[]> random_permutation(uint64_t N, uint64_t seed/* 0 => automatically generate a seed */){
    if(seed == 0) seed = std::random_device{}();
    unique_ptr<uint64_t[]> result{ new uint64_t[N] };
    do_permute<uint64_t>(result.get(), N, /* no buckets = */ max(4u, thread::hardware_concurrency()) * 8, seed);
    return result;
}

template<typename T>
static void permute_par_copy(const T& from, T& to, uint64_t array_sz, uint64_t* __restrict permutation){
    auto copy = [&from, &to, &permutation](uint64_t start /* incl */, uint64_t end /* excl */){
        for(uint64_t i = start; i < end; i++){
            to[i] = from [ permutation[ i ] ];
        }
    };

    uint64_t no_tasks = thread::hardware_concurrency() * 8;
    uint64_t partition_sz = array_sz / no_tasks;
    uint64_t partition_mod = array_sz % no_tasks;

    uint64_t start = 0;
    std::vector<future<void>> tasks;
    for(uint64_t i = 0; i < no_tasks; i++){
        uint64_t end = start + partition_sz + (i < partition_mod);
        tasks.push_back( std::async(launch::async, copy, start, end) );

        start = end; // next iteration
    }

    // wait for all tasks to complete
    for(auto& t: tasks) t.get();
}


template<typename T>
static unique_ptr<T[]> permute(T* array, uint64_t array_sz, uint64_t seed /* 0 => automatically generate a seed */){
    auto permutation = random_permutation(array_sz, seed);
    unique_ptr<uint64_t[]> result { new T[array_sz] };
    permute_par_copy(array, result.get(), array_sz, permutation.get());
    return result;
}

template<typename T>
static vector<T> permute(const std::vector<T>& array, uint64_t seed /* 0 => automatically generate a seed */){
    auto permutation = random_permutation(array.size(), seed);
    vector<T> result ( array.size() ); // do not use the curly brackets { } for the ctor, as it's going to interpret it as an init list
    permute_par_copy(array, result, array.size(), permutation.get());
    return result;
}
