#pragma once

#include "type.h"

extern MPI_Datatype mpi_type_vid;

#define BUF_TX_SZ (1<<21)


void create_analytics_comm();
void create_2d_comm();

status_t unpack_meta(char* buf, int buf_size, int& position, uint64_t& flag, 
             uint64_t& archive_marker, uint64_t& changed_v, uint64_t& changed_e);

status_t pack_meta(char* buf, int buf_size, int& position,  uint64_t flag, 
                   uint64_t archive_marker, uint64_t changed_v, uint64_t changed_e);
