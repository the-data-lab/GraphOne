
#pragma once
/**************** SKV ******************/
template <class T>
void onekv_t<T>::setup(tid_t t)
{
        tid = t;
        vid_t max_vcount = g->get_type_scount(tid);
        kv = (T*)malloc(sizeof(T)*max_vcount);
        memset(kv, INVALID_SID, sizeof(T)*max_vcount);
}

template <class T>
void onekv_t<T>::file_open(const string& filename, bool trunc)
{
    string  vtfile = filename + ".kv";
    if(trunc) {
        //vtf = fopen(vtfile.c_str(), "wb");
		vtf = open(vtfile.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
        assert(vtf != 0);
    } else {
        //vtf = fopen(vtfile.c_str(), "r+b");
		vtf = open(vtfile.c_str(), O_RDWR|O_CREAT, S_IRWXU);
        assert(vtf != 0);
    }
}

template <class T>
void onekv_t<T>::handle_write(bool clean /* = false*/)
{
    //Make a copy
    vid_t v_count = g->get_type_vcount(tid);
    index_t count = v_count*sizeof(T);
    //Write the file
    //fwrite(dvt, sizeof(disk_kvT_t<T>), count, vtf);
    //write(vtf, kv, sizeof(T)*v_count);
    while (count != 0) {
       count -= write(vtf, kv, count);
    }
}

template <class T>
void onekv_t<T>::read_vtable()
{
    off_t size = fsize(vtf);
    if (size == -1L) {
        assert(0);
    }
    vid_t count = (size/sizeof(T));

    //read in batches
    while (count !=0) {
        //vid_t read_count = fread(dvt, sizeof(disk_kvT_t<T>), dvt_max_count, vtf);
        vid_t read_count = read(vtf, kv, sizeof(T)*count);
        read_count /= sizeof(T);
        count -= read_count;
    }
}
