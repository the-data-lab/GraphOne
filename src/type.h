#pragma once
#include <stdint.h>
#include <limits.h>
#include <string>
#include <time.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <map>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "bitmap.h"

using std::map;
using std::cout;
using std::endl;
using std::string;
using std::swap;
using std::pair;

#ifdef B64
typedef uint16_t propid_t;
typedef uint64_t vid_t;
typedef uint64_t sid_t;
typedef uint64_t eid_t;
typedef uint64_t index_t;
typedef uint32_t tid_t;
typedef uint64_t sflag_t;
typedef uint16_t qid_t;
typedef uint64_t snapid_t ;
//typedef uint16_t rdegree_t; //relative degree
typedef int32_t degree_t;
#elif B32
typedef uint8_t propid_t;
typedef uint32_t vid_t;
typedef uint32_t sid_t;
typedef uint32_t eid_t;
typedef uint64_t index_t;
typedef uint8_t tid_t;
typedef uint64_t sflag_t;
typedef uint16_t qid_t;
typedef uint64_t snapid_t ;
//typedef uint16_t rdegree_t; //relative degree
typedef int32_t degree_t;
#endif

typedef uint16_t vflag_t;


#define HUB_COUNT  8192

#ifndef PLAIN_GRAPH 
#ifdef B64
#define VBIT 40
#define VMASK 0xffffffffff
#define THIGH_MASK 0x7FFFFF0000000000
#define DEL_MASK   0x8000000000000000
#define SID_MASK   0x7FFFFFFFFFFFFFFF
#elif B32
#define VBIT 28
#define VMASK 0xfffffff
#define THIGH_MASK 0x70000000
#define DEL_MASK   0x80000000
#define SID_MASK   0x7FFFFFFF
#endif
#else
#ifdef B64
#define VBIT 40
#define VMASK 0xffffffffff
#define THIGH_MASK 0x7FFFFF0000000000
#define DEL_MASK   0x8000000000000000
#define SID_MASK   0x7FFFFFFFFFFFFFFF
#elif B32
#define VBIT 30
#define VMASK 0x3fffffff
#define THIGH_MASK 0x40000000
#define DEL_MASK   0x80000000
#define SID_MASK   0x7FFFFFFF
#endif
#endif

#define CL_MASK    0xFFFFFFFFFFFFFFC0
#define PAGE_MASK  0xFFFFFFFFFFFFFC00

#ifdef OVER_COMMIT
#define TO_CACHELINE(x) ((x+63) & CL_MASK)
#define TO_PAGESIZE(x) ((x+4095) & PAGE_MASK)
#else 
#define TO_CACHELINE(x) (x)
#define TO_PAGESIZE(x) (x)
#endif

#define TO_TID(sid)  ((sid & THIGH_MASK) >> VBIT)
#define TO_VID(sid)  (sid & VMASK)
#define TO_SID(sid)  (sid & SID_MASK)
#define TO_SUPER(tid) (((sid_t)(tid)) << VBIT)
#define TO_THIGH(sid) (sid & THIGH_MASK)
#define DEL_SID(sid) (sid | DEL_MASK)
#define IS_DEL(sid) (sid & DEL_MASK)
#define UNDEL_SID(sid) (sid & SID_MASK)

#define TID_TO_SFLAG(tid) (1L << tid)
#define WORD_COUNT(count) ((count + 63) >> 6)

extern propid_t INVALID_PID;
extern tid_t    INVALID_TID;
extern sid_t    INVALID_SID;
extern degree_t INVALID_DEGREE;
//#define INVALID_PID 0xFFFF
//#define INVALID_TID 0xFFFFFFFF
//#define INVALID_SID 0xFFFFFFFFFFFFFFFF

#define NO_QID 0xFFFF

extern double  bu_factor;
extern int32_t MAX_BCOUNT; //256
extern uint64_t MAX_ECOUNT; //1000000
extern uint64_t MAX_PECOUNT;//670000

extern index_t  BATCH_SIZE;//
extern index_t  BATCH_MASK;//
extern index_t  BLOG_SHIFT;//
//extern index_t  BLOG_MASK;//
extern index_t  DELTA_SIZE;
extern index_t  SNAP_COUNT;

extern index_t  W_SIZE;//Durable edge log offset
extern index_t  DVT_SIZE;
extern index_t  DURABLE_SIZE;//

extern index_t  OFF_COUNT;
extern int      THD_COUNT;
extern index_t  LOCAL_VUNIT_COUNT;
extern index_t  LOCAL_DELTA_SIZE;

void free_buf(void* buf);
void* alloc_buf();

off_t fsize(const string& fname);
off_t fsize(int fd);

enum direction_t {
    eout = 0, 
    ein
};

enum status_t {
    eOK = 0,
    eInvalidPID,
    eInvalidVID,
    eQueryFail,
    eEndBatch,
    eOOM,
    eDelete,
    eNoWork,
    eNotValid,
    eUnknown        
};

typedef union __univeral_type {
    uint8_t  value_8b;
    uint16_t value16b;
    tid_t    value_tid;
    vid_t    value_vid;
    sid_t    value_sid;
    float    value_float;
    sid_t    value_offset;
    sid_t    value;

#ifdef B32   
    char     value_string[4];
    sid_t    value_charp;
#else     
    char     value_string[8];
    //int64_t  value_64b;
    //eid_t    value_eid;
    time_t   value_time;
    //char*    value_charp;
    double   value_double;
#endif
}univ_t;

typedef uint16_t word_t;
struct snb_t {
    word_t src;
    word_t dst;
};

union dst_id_t {
    sid_t sid;
    snb_t snb; 
};

//First can be nebr sid, while the second could be edge id/property
template <class T>
class dst_weight_t {
 public:
    dst_id_t first;
    T        second;
};

template <class T>
class  edgeT_t {
 public:
    sid_t src_id;
    T     dst_id;
};

#include "new_type.h"

//Feel free to name the derived types, but not required.
typedef edgeT_t<dst_id_t> edge_t;

//deprecated
typedef dst_weight_t<univ_t> lite_edge_t;
typedef edgeT_t<lite_edge_t> ledge_t;

//These are new typedefs
typedef dst_weight_t<univ_t> weight_sid_t;
typedef edgeT_t<weight_sid_t> weight_edge_t;

// Functions on edgeT_t
inline sid_t get_dst(edge_t* edge) {
    return edge->dst_id.sid;
}
inline sid_t get_dst(edge_t& edge) {
    return edge.dst_id.sid;
}
inline void set_dst(edge_t* edge, sid_t dst_id) {
    edge->dst_id.sid = dst_id;
}
inline void set_dst(edge_t& edge, sid_t dst_id) {
    edge.dst_id.sid = dst_id;
}
inline void set_dst(edge_t* edge, snb_t dst_id) {
    edge->dst_id.snb = dst_id;
}
inline void set_weight(edge_t* edge, dst_id_t dst_id) {
}

template <class T>
inline sid_t get_src(edgeT_t<T>* edge) {
    return edge->src_id;
}
template <class T>
inline sid_t get_src(edgeT_t<T>& edge) {
    return edge.src_id;
}
template <class T>
inline void set_src(edgeT_t<T>* edge, sid_t src_id) {
    edge->src_id = src_id;
}
template <class T>
inline void set_src(edgeT_t<T>& edge, sid_t src_id) {
    edge.src_id = src_id;
}

template <class T>
inline sid_t get_dst(edgeT_t<T>* edge) { 
    return edge->dst_id.first.sid;
}
template <class T>
inline sid_t get_dst(edgeT_t<T>& edge) { 
    return edge.dst_id.first.sid;
}
template <class T>
inline snb_t get_snb(edgeT_t<T>* edge) { 
    return edge->dst_id.first.snb;
}
template <class T>
inline void set_dst(edgeT_t<T>* edge, sid_t dst_id) {
    edge->dst_id.first.sid = dst_id;
}
template <class T>
inline void set_dst(edgeT_t<T>& edge, sid_t dst_id) {
    edge.dst_id.first.sid = dst_id;
}
template <class T>
inline void set_weight_int(edgeT_t<T>* edge, int weight) {
    edge->dst_id.second.value = weight;
}
template <class T>
inline void set_weight_int(edgeT_t<T>& edge, int weight) {
    edge.dst_id.second.value = weight;
}
template <class T>
inline int get_weight_int(edgeT_t<T>* edge) {
   return edge->dst_id.second.value;
}
template <class T>
inline int get_weight_int(edgeT_t<T>& edge) {
    return edge.dst_id.second.value;
}
template <class T>
inline void set_weight(edgeT_t<T>* edge, T dst_id) {
    edge->dst_id.second = dst_id.second;
}

////function on dst_weight_t
template <class T> sid_t get_sid(T& dst);
template <class T> void set_sid(T& edge, sid_t sid1);
template <class T> void set_weight(T& edge, T& dst);
//template <class T> sid_t get_nebr(T* adj, vid_t k); 

template <class T>
inline sid_t get_sid(T& dst)
{
    return dst.first.sid;
}

template <class T>
inline snb_t get_snb(T& dst)
{
    return dst.first.snb;
}

template <class T>
inline void set_sid(T& edge, sid_t sid1)
{
    edge.first.sid = sid1;
}
template <class T>
inline int get_weight_int(T& dst) {
   return dst.second.value;
}

//Specialized functions for plain graphs, no weights
template <>
inline void set_sid<dst_id_t>(dst_id_t& sid , sid_t sid1) {
    sid.sid = sid1;
}

template <class T>
inline void set_snb(T& edge, snb_t snb1)
{
    edge.first.snb = snb1;
}

template <>
inline void set_snb<dst_id_t>(dst_id_t& snb, snb_t snb1)
{
    snb.snb = snb1;
}

template<>
inline sid_t get_sid<dst_id_t>(dst_id_t& sid)
{
    return sid.sid;
}

template<>
inline snb_t get_snb<dst_id_t>(dst_id_t& snb)
{
    return snb.snb;
}

//template<dst_id_t>
inline void set_weight(dst_id_t& sid , sid_t& dst)
{
    return;
}

//template<>
inline void set_weight(dst_id_t& sid , dst_id_t& dst)
{
    return;
}

template <class T>
inline void set_weight(T& edge, T& dst)
{
    edge.second = dst.second;
}

/*
template <>
inline sid_t get_nebr<sid_t>(sid_t* adj, vid_t k) {
    return adj[k];
}*/


class snapshot_t {
 public:
    snapid_t snap_id;
    index_t marker;
    index_t durable_marker;
    snapshot_t* next;
};

class disk_snapshot_t {
 public:
    snapid_t snap_id;
    index_t  marker;
    index_t  durable_marker;
};


class pedge_t {
 public:
    propid_t pid;
    sid_t src_id;
    univ_t dst_id;
};

/*
template <class T>
class disk_kvT_t {
    public:
    vid_t    vid;
    T       dst;
};*/



//property name value pair
struct prop_pair_t {
    string name;
    string value;
};


template <class T>
class delentry_t {
 public:
    degree_t pos;
    T dst_id;
};


extern snapid_t get_snapid();
extern vid_t get_vcount(tid_t tid);

typedef struct __econf_t {
    string filename;
    string predicate;
    string src_type;
    string dst_type;
    string edge_prop;
} econf_t; 

typedef struct __vconf_t {
    string filename;
    string predicate;
} vconf_t; 

template <class T>
class pgraph_t;

template<class T>
class gview_t;

template<class T>
class wstream_t;

template<class T>
class stream_t;

template<class T>
struct callback {
      typedef void(*sfunc)(gview_t<T>*);
      typedef void(*wfunc)(wstream_t<T>*);
      typedef void(*func)(gview_t<T>*);

      typedef index_t (*parse_fn_t)(const string&, const string&, pgraph_t<T>*);
      typedef index_t (*parse_fn2_t)(const char*, pgraph_t<T>*, index_t);
};

