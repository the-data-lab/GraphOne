#pragma once

#ifdef _MPI

#include "type.h"

extern MPI_Datatype mpi_type_vid;
extern int _numtasks, _rank;
extern int _numlogs;
extern MPI_Comm   _analytics_comm;
extern int _analytics_rank;
extern MPI_Comm  _row_comm, _col_comm;
extern int _col_rank, _row_rank;

#define BUF_TX_SZ (1<<21)


void create_analytics_comm();
void create_2d_comm();

status_t unpack_meta(char* buf, int buf_size, int& position, uint64_t& flag, 
             uint64_t& archive_marker, uint64_t& changed_v, uint64_t& changed_e);

status_t pack_meta(char* buf, int buf_size, int& position,  uint64_t flag, 
                   uint64_t archive_marker, uint64_t changed_v, uint64_t changed_e);

#endif
