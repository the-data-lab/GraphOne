#pragma once

#include "type.h"
#include "btree.h"


#define inplace_knode 4

class labeled_edges_t {
private:
	degree_t degree;//Will contain flag, if required
	
	//make it cacheline friendly.
	// will be used in case of one key 
	// 
	key_t key; 
	union {
        btree_t* edges_tree;//many PO
        leaf_node_t* edges;//upto 32 PO
        value_t* edge; //just one PO
		value_t  value[2];
    };

public:
    //allocate spaces now so that bulk insert can go fast.
    status_t initial_setup (degree_t a_degree) {return 0;};
    
	inline status_t 
    initial_insert(key_t key, value_t value)
    { return 0; }
    
    inline status_t 
    initial_insert(pair_t pair)
    { return 0 ; }
    
    inline value_t 
    search(key_t key) {return 0;}

    inline status_t
    insert(key_t key, value_t value)
    { return 0;}

    
    inline status_t
    remove(key_t key, value_t value)
    { return 0;}

    inline status_t
    remove(pair_t pair)
    { return 0;}
};

//in-edges list and out-edges list.
typedef struct __dedges_t {
    labeled_edges_t out_edges;
    labeled_edges_t in_edges;
} d_labeled_edges_t;


/*

//arranging only keys, not key-value.
class unlabeled_edges_t {
private:
	kbtree_t  btree;

public: 
    //allocate spaces now so that bulk insert can go fast.
    status_t initial_setup (int a_degree);
    
	inline status_t 
    initial_insert(key_t value)
    { return 0; }
    
    inline value_t 
    search(key_t key) {return 0;}

    status_t insert(key_t value);

    
    inline status_t
    remove(key_t value)
    { return 0;}

    inline status_t
    remove(pair_t pair)
    { return 0;}
} ;
*/

typedef kbtree_t unlabeled_edges_t;

//in-edges list and out-edges list.
struct d_unlabeled_edges_t {
    unlabeled_edges_t out_edges;
    unlabeled_edges_t in_edges;
} ;
