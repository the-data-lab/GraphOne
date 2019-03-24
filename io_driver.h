#pragma once

#include "type.h"

#define ALIGN_MASK 0xFFFFFFFFFFFE00
#define TO_RESIDUE(x) (x & 0x1FF)
#define UPPER_ALIGN(x) (((x) + 511) & ALIGN_MASK)
#define LOWER_ALIGN(x) ((x) & ALIGN_MASK)

#define IO_THDS 1
#define AIO_MAXIO 32768
#define IO_MAX    32768
#define AIO_BATCHIO 256
#define BUF_SIZE  (1L<< 29L)

class meta_t {
    public:
    vid_t vid;
    index_t offset;
};

class segment {
  public:
      char* buf;
      meta_t* meta;
      int     meta_count;
      int     ctx_count;
      io_context_t  ctx;
      struct  io_event* events;
      struct  iocb** cb_list;
      int     etf;
      int     busy;
};

typedef struct __aio_meta {
    struct io_event* events;
    struct iocb** cb_list;
    io_context_t  ctx;
    int busy;
} aio_meta_t;

class io_driver {
public:
    size_t seq_read_aio(segment* seg, ext_vunit_t* ext_vunits);
    size_t random_read_aio(segment* seg, ext_vunit_t* ext_vunits);
    int wait_aio_completion(segment* seg);
    template <class T>
    int prep_random_read_aio(vid_t& last_read, vid_t v_count, 
                             uint8_t* status, uint8_t level, size_t to_read, 
                             segment* seg, ext_vunit_t* ext_vunit);

    template <class T>
    int prep_seq_read_aio(vid_t& last_read, vid_t v_count, size_t to_read,
                          segment* seg, ext_vunit_t* ext_vunit);
private:
    //aio_meta_t* aio_meta;

public:
    io_driver();
};

template<class T>
int io_driver::prep_seq_read_aio(vid_t& last_read, vid_t v_count, size_t to_read,
                             segment* seg, ext_vunit_t* ext_vunit) 
{
    index_t disk_offset = 0;
    index_t sz_to_read = BUF_SIZE;
    meta_t* meta = seg->meta;
    int k = 0;
	int ctx_count = 0;
	index_t	size = (1<<20);
	index_t local_start = 0;

    
    index_t local_size = 0;
    index_t total_count = 0;
    index_t offset;
    index_t total_size = 0;

    for (vid_t vid = last_read; vid < v_count; ++vid) {
        total_count = ext_vunit[vid].count + ext_vunit[vid].del_count;
        if (total_count == 0) continue;
        
        offset = ext_vunit[vid].offset;
        if (k == 0) {
            total_size = TO_RESIDUE(offset);
			disk_offset = offset - total_size;
            //cout << "Offset 1 = " << offset - total_size << endl;
        }

        local_size = total_count*sizeof(T) + sizeof(durable_adjlist_t<T>);

        if (total_size + local_size > to_read) {
            //disk_offset =  ext_vunit[seg->meta[0].vid].offset - (seg->meta[0].offset);
            //io_prep_pread(seg->cb_list[0], seg->etf, seg->buf, sz_to_read, disk_offset);
			
			sz_to_read = UPPER_ALIGN(total_size);
			ctx_count = (sz_to_read >> 20) + (0 != (sz_to_read & 0xFFFFF));
			for (int i = 0; i < ctx_count; ++i) {
				local_size = UPPER_ALIGN(std::min(size, sz_to_read));

				io_prep_pread(seg->cb_list[i], seg->etf, seg->buf + local_start,
							  local_size, disk_offset + local_start);
				//disk_offset += local_size;
				local_start += local_size; 
				sz_to_read  -= local_size;
			}
            
			seg->ctx_count = ctx_count;
            seg->meta_count = k;
            last_read = vid;
            return k;
        }

        meta[k].vid = vid;
        meta[k].offset = total_size;
        total_size += local_size;
        ++k;
    }
    
    //disk_offset =  ext_vunit[seg->meta[0].vid].offset - (seg->meta[0].offset);
    //io_prep_pread(seg->cb_list[0], seg->etf, seg->buf, sz_to_read, disk_offset);
	sz_to_read = UPPER_ALIGN(total_size);
	ctx_count = (sz_to_read >> 20) + (0 != (sz_to_read & 0xFFFFF));
	for (int i = 0; i < ctx_count; ++i) {
		local_size = UPPER_ALIGN(std::min(size, sz_to_read));
		io_prep_pread(seg->cb_list[i], seg->etf, seg->buf + local_start,
					  local_size, disk_offset + local_start);
		//disk_offset += local_size;
		local_start += local_size; 
		sz_to_read  -= local_size;
	}
    
	seg->ctx_count = ctx_count;
    seg->meta_count = k;
    last_read = v_count;
    return k;
}

template<class T>
int io_driver::prep_random_read_aio(vid_t& last_read, vid_t v_count, 
                             uint8_t* status, uint8_t level, size_t to_read, 
                             segment* seg, ext_vunit_t* ext_vunit)
{
    index_t disk_offset = 0;
    index_t sz_to_read = BUF_SIZE;
    meta_t* meta = seg->meta;
    int k = 0;
    int ctx_count = 0;
    
    index_t total_count = 0;
    index_t offset;
    index_t super_size = 0;
    index_t local_size = 0;
    index_t total_size = 0;
    bool started = false;
	index_t cont = 0;

    for (vid_t vid = last_read; vid < v_count; ++vid) {
        total_count = ext_vunit[vid].count + ext_vunit[vid].del_count;
        if (total_count == 0) continue;
        
        offset = ext_vunit[vid].offset;
		local_size = total_count*sizeof(T) + sizeof(durable_adjlist_t<T>);
        
		if(status[vid] != level) { 
			cont +=  local_size;
			if (cont > 1024) {
				if(started)  {
					started = false;
					sz_to_read = UPPER_ALIGN(total_size);
					io_prep_pread(seg->cb_list[ctx_count], seg->etf, seg->buf + super_size, 
								  sz_to_read, disk_offset);
				
					super_size += sz_to_read;
					total_size = 0;
					++ctx_count;
					cont = 0;
				}
			}
			continue;
        }
		
        if (started == false) {
            started = true;
            total_size = TO_RESIDUE(offset);
            disk_offset = offset - total_size;
			cont = 0;
            //super_size += total_size;
		}

        total_size += cont;
	    cont = 0;	

        if ((super_size + total_size + local_size > to_read)
            || (ctx_count == AIO_MAXIO - 1) ) {
            sz_to_read = UPPER_ALIGN(total_size);
            io_prep_pread(seg->cb_list[ctx_count], seg->etf, seg->buf + super_size, 
                          sz_to_read, disk_offset);
            
            super_size += sz_to_read;
            total_size = 0;
            ++ctx_count;
            seg->ctx_count = ctx_count;
            seg->meta_count = k;
            last_read = vid;
            return k;
        }

        meta[k].vid = vid;
        meta[k].offset = super_size + total_size;
        total_size += local_size;
        ++k;
    }
   
    if (started) { 
        //disk_offset =  ext_vunit[seg->meta[0].vid].offset - (seg->meta[0].offset);
        sz_to_read = UPPER_ALIGN(total_size);
        io_prep_pread(seg->cb_list[ctx_count], seg->etf, seg->buf + super_size, 
                      sz_to_read, disk_offset);
        
        super_size += sz_to_read;
        total_size = 0;
        ++ctx_count;
    }

    seg->ctx_count = ctx_count;
    seg->meta_count = k;
    last_read = v_count;
    return k;

}
