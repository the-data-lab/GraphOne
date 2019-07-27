#pragma once

#include "type.h"

void create_1d_comm();
void create_2d_comm();
void create_2d_comm1();

status_t unpack_meta(char* buf, int buf_size, int& position, uint64_t& flag, 
             uint64_t& archive_marker, uint64_t& changed_v, uint64_t& changed_e);

status_t pack_meta(char* buf, int buf_size, int& position,  uint64_t flag, 
                   uint64_t archive_marker, uint64_t changed_v, uint64_t changed_e);
