#pragma once

#include "type.h"

#ifdef TBB
#define TBB_PREVIEW_LOCAL_OBSERVER 1
#define TBB_PREVIEW_TASK_ARENA 1

#include "tbb/concurrent_hash_map.h"
#include "tbb/blocked_range.h"
#include "tbb/parallel_for.h"
#include <string>

using namespace tbb;
struct MyHashCompare {
    static size_t hash( const string& x ) {
        size_t h = 0;
        for( const char* s = x.c_str(); *s; ++s )
            h = (h*17)^*s;
        return h;
    }
    //! True if strings are equal
    static bool equal( const string& x, const string& y ) {
        return x==y;
    }
};
// A concurrent hash table that maps strings to ints.
typedef concurrent_hash_map<string, sid_t, MyHashCompare> safemap_t;

class str2intmap{
    public:
        safemap_t safe_map;

        inline str2intmap(){}

        inline void insert(std::string rnd_string, sid_t rnd_index) {
            safemap_t::accessor a;
            safe_map.insert(a, rnd_string);
            a->second = rnd_index;
        }

        inline void erase(std::string rnd_string) {
            //size_t erased_elements = 
            safe_map.erase(rnd_string);
        }

        inline void update(std::string rnd_string, sid_t rnd_index) {
        }

        inline sid_t find(std::string rnd_string)
        {
            safemap_t::const_accessor a;
            if(safe_map.find(a, rnd_string)) {
                return a->second;
            }
            return INVALID_SID;
        }


        // Capacity functions
        inline size_t size(){ return safe_map.size(); }
        inline bool empty(){ return safe_map.empty(); }
        inline size_t max_size(){ return safe_map.max_size(); }
        inline void clear(){ safe_map.clear(); }
};

#else

#include "bib/multithreaded_container.h"

class str2intmap{
    public:
        sf::contfree_safe_ptr< std::map<std::string, sid_t> > safe_map;

        inline str2intmap(){}

        void emplace(std::string rnd_string, sid_t rnd_index){
            safe_map->emplace(rnd_string, rnd_index);
        }

        inline void insert(std::string rnd_string, sid_t rnd_index){
            safe_map->emplace(rnd_string, rnd_index);
        }

        inline void erase(std::string rnd_string){
            //size_t erased_elements = 
            safe_map->erase(rnd_string);
        }

        inline void update(std::string rnd_string, sid_t rnd_index){
            auto x_safe_map = xlock_safe_ptr(safe_map);
            auto it = x_safe_map->find(rnd_string);
            if (it != x_safe_map->cend()){
                it->second += rnd_index;
            }
        }

        inline sid_t find(std::string rnd_string)
        {
            //std::string key = std::to_string(rnd_string);
            auto s_safe_map = slock_safe_ptr(safe_map);
            auto it = s_safe_map->find(rnd_string);
            if(it != s_safe_map->cend()){
                volatile sid_t sid = it->second;
                return sid;
            }
            return INVALID_SID;
        }


        // Capacity functions
        inline size_t size(){ return safe_map->size(); }
        inline bool empty(){ return safe_map->empty(); }
        inline size_t max_size(){ return safe_map->max_size(); }
        inline void clear(){ safe_map->clear(); }
};

#endif
