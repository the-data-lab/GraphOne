#pragma once

#include <map>
#include <string.h>
#include "type.h"
using  std::map;

class disk_strkv_t {
    public:
    vid_t vid;
    sid_t offset;
};

//storing strings as edge weights
class str_t {
private:
    char*      log_beg;  //memory log pointer
    index_t    log_count;//size of memory log
    index_t    log_head; // current log write position
    index_t    log_tail; //current log cleaning position
    index_t    log_wpos; //Write this pointer for write persistency
    int         etf;

public:
    inline str_t() {
        log_beg = 0;
        log_count = 0;
        log_head = 0;
        log_tail = 0;
        log_wpos = 0;
        etf = -1;
    }

    inline void setup(index_t size) {
        log_count = size;
        if (posix_memalign((void**)&log_beg, 2097152, log_count*sizeof(char))) {
            log_beg = (char*)calloc(sizeof(char), log_count);
            //perror("posix memalign edge log");
        }
        log_head = 1;
        log_beg[0] = 0;
        log_tail = 0;
        log_wpos = 0;
    }

    inline void file_open(const char* etfile, bool trunc) {
		if (trunc) {
            etf = open(etfile, O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
        } else {
	        etf = open(etfile, O_RDWR|O_CREAT, S_IRWXU);
        }
    }
    inline void handle_read() {
        off_t size = fsize(etf);
        if (size == -1L) { assert(0); }
    
        if (size != 0) {
        index_t edge_count = size/sizeof(char);
        read(etf, log_beg, sizeof(char)*edge_count);
        log_wpos = edge_count;
        log_head = edge_count;
        }
    }
    inline status_t handle_write() {
        if(log_head > 1) {
            index_t wpos = log_wpos;
            log_wpos = log_head;
            write(etf, log_beg+wpos, log_wpos-wpos);
            //fwrite(log_beg+wpos, sizeof(char), log_head-wpos, etf);
            return eOK;
        }
        return eNoWork;
    }
    inline char* alloc_str(index_t size, index_t& offset) {
        char* ptr = log_beg + log_head;
        offset = log_head;
        log_head += size;
        assert(log_head < log_count);
        return ptr;
    }
    
    inline index_t copy_str(const char* value) {
        index_t size = strlen(value);
        if (0 == size) {
            return 0;
        }
        char* ptr = log_beg + log_head;
        index_t offset = log_head;
        log_head += strlen(value) +1;
        assert(log_head < log_count);
        memcpy(ptr, value, strlen(value)+1);
        return offset;
    }
    inline char* get_ptr(index_t offset) {
        return log_beg + offset;
    }
};
