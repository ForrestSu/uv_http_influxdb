#ifndef _RECORDTIME_H
#define _RECORDTIME_H

#if ((defined _WIN32) || (defined _WIN64))
#include <time.h>
class RecordTimeMs
{
public:
    RecordTimeMs():startTime(0){
    }
    void start()
    {
       startTime = clock();
    }
    double stop()
    {
       double milliseconds = (double)(clock() - startTime);
       return milliseconds;
    }
    ~RecordTimeMs(){}
private:
    clock_t startTime;
};
#else
#include<sys/time.h>
class RecordTimeMs
{
public:
    RecordTimeMs(){ }
    void start()
    {
       gettimeofday(&startTime,NULL);
    }
    double stop()
    {
        gettimeofday(&endTime,NULL);
        double milliseconds = ((double)1000.0)*(endTime.tv_sec - startTime.tv_sec) + (endTime.tv_usec - startTime.tv_usec)/1000.0;
        return milliseconds;
    }
    ~RecordTimeMs(){}
private:

private:
    struct timeval startTime;
    struct timeval endTime;
};
#endif // _WIN32

#endif // _RECORDTIME_H

/** usage:
    RecordTimeMs times;
    times.start();
    //do something
    double timeCostMs = times.stop();
*/

