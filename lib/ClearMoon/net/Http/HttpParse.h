#ifndef CLEARMOON_NET_HTTP_HTTPPARSE_H
#define CLEARMOON_NET_HTTP_HTTPPARSE_H

#include "Buffer.h"
#include "Http/HttpRequest.h"
#include "Http/HttpResponse.h"
#include <cstddef>
namespace clearmoon 
{
namespace net 
{

class HttpParse
{
public:
    bool HttpRequestParse(Buffer* buffer, HttpRequest& request);

    void reset();
private: 
    enum LineStatus
    {
        LINE_OK,
        LINE_BAD,
        LINE_OPEN
    };

    enum MainStatus
    {
        STATUS_REQUEST_LINE,
        STATUS_HEADERS,
        STATUS_BODY,
        STATUS_DONE
    };

    LineStatus lineStatus_;
    MainStatus mainStatus_;

    bool parseLine(const char* data, size_t len);

    bool parseRequestLine();
    bool parseRequestHeader();
    bool parseRequestBody();
};

}
}

#endif