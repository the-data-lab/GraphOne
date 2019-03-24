#include <assert.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <omp.h>
#include "type.h"
#include "graph.h"
#include "graph_base.h"
#include "io_driver.h"

#ifdef B64
propid_t INVALID_PID = 0xFFFF;
tid_t    INVALID_TID  = 0xFFFFFFFF;
sid_t    INVALID_SID  = 0xFFFFFFFFFFFFFFFF;
#else 
propid_t INVALID_PID = 0xFF;
tid_t    INVALID_TID  = 0xFF;
sid_t    INVALID_SID  = 0xFFFFFFFF;
#endif

degree_t INVALID_DEGREE = 0xFFFFFFFF;

size_t io_driver::seq_read_aio(segment* seg, ext_vunit_t* ext_vunits)
{
    int ctx_count = seg->ctx_count;
    if (0 == ctx_count) return 0;
    int ret = 0;
    //index_t sz_to_read = BUF_SIZE;
    //index_t disk_offset =  ext_vunits[seg->meta[0].vid].offset - (seg->meta[0].offset);
    //io_prep_pread(seg->cb_list[0], seg->etf, seg->buf, sz_to_read, disk_offset);
    //cout << "Offset = " << disk_offset << endl;
    ret = io_submit(seg->ctx, ctx_count, seg->cb_list);

        if (ret != ctx_count) {
            cout << ret << endl;
            perror("io_submit");
            assert(0);
        }
    seg->busy = ctx_count;

    return 0;
}


size_t io_driver::random_read_aio(segment* seg, ext_vunit_t* ext_vunits)
{
    int ctx_count = seg->ctx_count;
    if (0 == ctx_count) return 0;
    int ret = 0;
    int local_ctx_count = 0;
    int total_ctx_count = 0;
    //index_t sz_to_read = BUF_SIZE;
    //index_t disk_offset =  ext_vunits[seg->meta[0].vid].offset - (seg->meta[0].offset);
    //io_prep_pread(seg->cb_list[0], seg->etf, seg->buf, sz_to_read, disk_offset);
    //cout << "Offset = " << disk_offset << endl;
    
    while (ctx_count != 0) {
        local_ctx_count = std::min(ctx_count, AIO_BATCHIO);
        ret = io_submit(seg->ctx, local_ctx_count, seg->cb_list + total_ctx_count);

        if (ret != local_ctx_count) {
            cout << ret << endl;
            perror("io_submit");
            assert(0);
        }
        total_ctx_count += local_ctx_count;
        seg->busy += local_ctx_count;
        ctx_count -= local_ctx_count;

        if (seg->busy == IO_MAX) {
            ret = io_getevents(seg->ctx, AIO_BATCHIO, AIO_BATCHIO, 
                           seg->events, 0);
            if (ret != AIO_BATCHIO) {
                cout << ret << endl;
                perror("io_getevents");
                assert(0);
            }
            seg->busy -= AIO_BATCHIO;
        } 
    }
    return 0;
}

int io_driver::wait_aio_completion(segment* seg)
{
    int ret = 0;
    if (seg->busy) {
        ret = io_getevents(seg->ctx, seg->busy, 
                           seg->busy, seg->events, 0);

        if (ret != seg->busy) {
            cout << seg->busy << " " << ret << endl;
            perror(" io_getevents");
        }
    }

    seg->busy = 0;
    return 0;
}

io_driver::io_driver()
{
    /*
    aio_meta = new aio_meta_t[IO_THDS];
    
    for (int j = 0; j < IO_THDS; ++j) {
        aio_meta[j].events = new struct io_event [AIO_MAXIO];
        aio_meta[j].cb_list = new struct iocb*[AIO_MAXIO];
        aio_meta[j].ctx = 0;
        for(index_t i = 0; i < AIO_MAXIO; ++i) {	
            aio_meta[j].cb_list[i] = new struct iocb;
        }
        if(io_setup(AIO_MAXIO, &aio_meta[j].ctx) < 0) {
            cout << AIO_MAXIO << endl;
            perror("io_setup");
            assert(0);
        }
        aio_meta[j].busy = 0;
    }
    */
}

