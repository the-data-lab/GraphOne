#include "communicator.h"
#include "graph.h"

#ifdef B32    
    MPI_Datatype mpi_type_vid = MPI_UINT32_T;
#elif B64 
    MPI_Datatype mpi_type_vid = MPI_UINT32_T;
#endif

void create_2d_comm()
{
    // extract the original group handle
    int group_count = _numtasks - 1;
    int *ranks1 = (int*)malloc(sizeof(int)*group_count);

    for (int i = 0; i < group_count; ++i) {
        ranks1[i] = i+1;
    }
    MPI_Group  orig_group, analytics_group;
    
    MPI_Comm_group(MPI_COMM_WORLD, &orig_group);
    MPI_Group_incl(orig_group, group_count, ranks1, &analytics_group);

    //create new communicator 
    MPI_Comm_create(MPI_COMM_WORLD, analytics_group, &_analytics_comm);
}

void create_2d_comm1()
{
    int row_id, col_id;
    // extract the original group handle
    if (0 == _rank) {
        row_id = MPI_UNDEFINED;
        col_id = MPI_UNDEFINED;
    } else {
        row_id = (_rank - 1) / _part_count;
        col_id = (_rank - 1) % _part_count;
    }

    MPI_Comm_split(MPI_COMM_WORLD, row_id, _rank, &_row_comm);
    MPI_Comm_split(MPI_COMM_WORLD, col_id, _rank, &_col_comm);
    if (_rank !=0) {
        MPI_Comm_rank(_row_comm, &_row_rank);
        MPI_Comm_rank(_col_comm, &_col_rank);
    }
}

void create_1d_comm()
{
    // extract the original group handle
    int group_count = _numtasks - 1;
    int *ranks1 = (int*)malloc(sizeof(int)*group_count);

    for (int i = 0; i < group_count; ++i) {
        ranks1[i] = i+1;
    }
    MPI_Group  orig_group, analytics_group;
    
    MPI_Comm_group(MPI_COMM_WORLD, &orig_group);
    //if (_rank > 0) { }
    MPI_Group_incl(orig_group, group_count, ranks1, &analytics_group);

    //create new communicator 
    MPI_Comm_create(MPI_COMM_WORLD, analytics_group, &_analytics_comm);
}

status_t pack_meta(char* buf, int buf_size, int& position,  uint64_t flag, 
                   uint64_t archive_marker, uint64_t changed_v, uint64_t changed_e)
{
    //Header of the package
    uint64_t endian = 0x0123456789ABCDEF;//endian
    
    MPI_Pack(&endian, 1, MPI_UINT64_T, buf, buf_size, &position, MPI_COMM_WORLD);
    MPI_Pack(&archive_marker, 1, MPI_UINT64_T, buf, buf_size, &position, MPI_COMM_WORLD);
    MPI_Pack(&flag, 1, MPI_UINT64_T, buf, buf_size, &position, MPI_COMM_WORLD);
    MPI_Pack(&changed_v, 1, MPI_UINT64_T, buf, buf_size, &position, MPI_COMM_WORLD);
    MPI_Pack(&changed_e, 1, MPI_UINT64_T, buf, buf_size, &position, MPI_COMM_WORLD);
    return eOK;
}

status_t unpack_meta(char* buf, int buf_size, int& position, uint64_t& flag, 
                   uint64_t& archive_marker, uint64_t& changed_v, uint64_t& changed_e)
{
    //Header of the package
    uint64_t endian; 
    MPI_Unpack(buf, buf_size, &position, &endian, 1, MPI_UINT64_T, MPI_COMM_WORLD);
    assert(endian == 0x0123456789ABCDEF);
    MPI_Unpack(buf, buf_size, &position, &archive_marker, 1, MPI_UINT64_T, MPI_COMM_WORLD);
    MPI_Unpack(buf, buf_size, &position, &flag, 1, MPI_UINT64_T, MPI_COMM_WORLD);
    
    MPI_Unpack(buf, buf_size, &position, &changed_v, 1, MPI_UINT64_T, MPI_COMM_WORLD);
    MPI_Unpack(buf, buf_size, &position, &changed_e, 1, MPI_UINT64_T, MPI_COMM_WORLD);
    
    return eOK;
}
