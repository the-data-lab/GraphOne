#include <iostream>
#include <string>
#include <string.h>
#include <cstdio>
#include "prop_encoder.h"
#include "cf_info.h"

using std::cout;
using std::endl;

status_t 
time_encoder_t::encode(const char* str_time, univ_t& value, cfinfo_t* cf_info)
{
    struct tm x;
    memset(&x, 0, sizeof(struct tm));
    strptime(str_time, "%Y-%m-%dT%H:%M:%S", &x);
    //XXX value.value_time = timegm(&x);
    return eOK;
}

void
time_encoder_t::print(univ_t value)
{
    struct tm* px;
    struct tm x;
    char buf[255];
    
    memset(&x, 0, sizeof(struct tm));
    px = &x;
    //XXX px = gmtime_r(&value.value_time, px);
    strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", px);

    cout << buf << endl;
} 

status_t 
int64_encoder_t::encode(const char* str, univ_t& value, cfinfo_t* cf_info)
{
#ifdef B64
    sscanf(str, "%ld", &value.value);
#else
    sscanf(str, "%d", &value.value);
#endif
    return eOK;
}

void
int64_encoder_t::print(univ_t value)
{
    cout << value.value << endl;
}

status_t 
str_encoder_t::encode(const char* str, univ_t& value, cfinfo_t* cf_info)
{
    value.value = cf_info->copy_str(str);
    return eOK;
}

void
str_encoder_t::print(univ_t value)
{
    assert(0);
    //cout << value.value_string << endl;
}

status_t 
double_encoder_t::encode(const char* str, univ_t& value, cfinfo_t* cf_info)
{
    //sscanf(str, "%lf", &value.value_double);
    return eOK;
}

void
double_encoder_t::print(univ_t value)
{
    //cout << value.value_double << endl;
}
