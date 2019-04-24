#pragma once

#include "type.h"

class cfinfo_t;

class prop_encoder_t {
    public:
    virtual status_t encode(const char* str_time, univ_t& value, cfinfo_t* cf_info) = 0;
    inline virtual void print(univ_t value) = 0;
    inline virtual ~prop_encoder_t(){}

};

class time_encoder_t : public prop_encoder_t {
    public:
    status_t encode(const char* str_time, univ_t& value, cfinfo_t* cf_info);
    void print(univ_t value);
    inline virtual ~time_encoder_t(){}

    //Add filters like <, > etc
};

class int64_encoder_t : public prop_encoder_t {
 public:
    status_t encode(const char* str, univ_t& value, cfinfo_t* cf_info);
    void print(univ_t value);
    inline virtual ~int64_encoder_t(){}
};

class str_encoder_t : public prop_encoder_t {
 public:
    status_t encode(const char* str, univ_t& value, cfinfo_t* cf_info);
    void print(univ_t value);
    inline virtual ~str_encoder_t(){}
}; 

class double_encoder_t : public prop_encoder_t {
 public:
    status_t encode(const char* str, univ_t& value, cfinfo_t* cf_info);
    void print(univ_t value);
}; 
