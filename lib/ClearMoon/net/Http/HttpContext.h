#ifndef CLEARMOON_NET_HTTP_HTTPCONTEXT_H
#define CLEARMOON_NET_HTTP_HTTPCONTEXT_H

#include "HttpRequest.h"
#include "HttpResponse.h"
#include "../Buffer.h"

namespace clearmoon 
{
namespace net
{

class HttpContext
{
public:
    enum HttpRequestParseState
    {
        kExpectRequestLine,
        kExpectHeaders,
        kExpectBody,
        kGotAll
    };

    HttpContext() : state_(kExpectRequestLine) {}
    ~HttpContext() = default;

    // 返回 true 表示解析完成（kGotAll），false 表示需要更多数据
    bool parse(Buffer* buf, HttpRequest* request, HttpResponse* response);

    bool gotAll() const { return state_ == kGotAll; }

    void reset()
    {
        state_ = kExpectRequestLine;
    }

private:
    bool parseRequestLine(const std::string& line, HttpRequest* request);

    HttpMethod stringToMethod(const std::string& method) const
    {
        if (method == "GET")        return HttpMethod::kGet;
        if (method == "POST")       return HttpMethod::kPost;
        if (method == "HEAD")       return HttpMethod::kHead;
        if (method == "PUT")        return HttpMethod::kPut;
        if (method == "DELETE")     return HttpMethod::kDelete;
        if (method == "OPTIONS")    return HttpMethod::kOptions;
        if (method == "PATCH")      return HttpMethod::kPatch;
        return HttpMethod::kInvalid;
    }

    HttpRequestParseState state_;
};

}
}

#endif