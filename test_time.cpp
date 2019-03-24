#include <time.h>
#include <string.h>
#include <iostream>
#include <stdio.h>
#include <string>


using namespace std;

union univ_t {
    time_t value_time;
    long value_long;
    double value_double;
    char   value_string[8];
};

void 
encode(const char* str_time, time_t& value)
{
    struct tm x;
    memset(&x, 0, sizeof(struct tm));
    strptime(str_time, "%Y-%m-%dT%H:%M:%S", &x);
    value = timegm(&x);
    return;
}

void
print(time_t value)
{
    struct tm* px;
    struct tm x;
    char buf[255];
    
    memset(&x, 0, sizeof(struct tm));
    px = &x;
    px = gmtime_r(&value, px);
    strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", px);

    cout << buf << endl;
} 

typedef long sid_t;
template <class T>
class  edgeT_t {
 public:
    sid_t src_id;
    T     dst_id;
};

//First can be nebr sid, while the second could be edge id/property
class lite_edge_t {
 public:
    sid_t first;
    univ_t second;
};

typedef edgeT_t<sid_t> edge_t;
typedef edgeT_t<lite_edge_t> ledge_t;


int main()
{
    //string str = "2012-08-21T01:24:21.123+0000";
    string str = "2013";
    time_t time = 0;
    encode(str.c_str(), time);
    print(time);

    univ_t univ;
    memcpy(univ.value_string, "abcdefg", 8);
    cout << sizeof(univ) << endl;
    cout << univ.value_string << endl;
    cout << "sizeof(long)= " << sizeof(long) << endl;
    cout << "sizeof(edge_t) = " << sizeof(edge_t) << endl;
    cout << "sizeof(ledge_t) = " << sizeof(ledge_t) << endl;
    /*
    univ.value_double = 10.123;
    FILE* f = fopen("uni_test.bin", "wb");
    fwrite(&univ, sizeof(univ), 1,  f);
    fclose(f);

    univ_t univ2;
    f = fopen("uni_test.bin", "rb");
    fread(&univ2, sizeof(univ), 1,  f);
    */

    return 0;
}
