#ifndef LOG_H
#define LOG_H

#include <stdio.h>                                                          
#include <time.h>
#include <sys/time.h>

#define LOG(format, ...) ({\
    struct timeval _tv;\
    gettimeofday(&_tv, NULL);\
    struct tm _now_time;\
    time_t _time_seconds = _tv.tv_sec;\
    localtime_r(&_time_seconds, &_now_time);\
    char _buf[128] = {0};\
    size_t _len = strftime(_buf, 128, "%Y-%m-%d %H:%M:%S", &_now_time);\
    snprintf(_buf+_len, 128 - _len, ".%06ld %s:%d: %s", _tv.tv_usec, __FILE__, __LINE__, __FUNCTION__);\
    printf("%s " format "\n", _buf, ##__VA_ARGS__);\
})


#endif //LOG_H
