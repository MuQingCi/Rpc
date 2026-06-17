#include "HttpContext.h"

using namespace clearmoon;
using namespace clearmoon::net;

bool HttpContext::parse(Buffer* buf, HttpRequest* request, HttpResponse* response)
{
    bool more = true;

    while (more)
    {
        switch (state_)
        {
        case kExpectRequestLine:
        {
            const char* crlf = buf->findCRLF();
            if (crlf == nullptr)
            {
                more = false;  // 数据不够，等待更多
                break;
            }

            size_t lineLen = crlf - buf->peek();
            std::string line(buf->peek(), lineLen);
            buf->retrieve(lineLen + 2); // skip \r\n

            if (!parseRequestLine(line, request))
            {
                // 请求行解析失败，返回 400
                response->setStatus(HttpStatusCode::k400BadRequest);
                response->setBody("Bad Request\r\n");
                response->setContentType("text/plain");
                response->setCloseConnection(true);
                return true;
            }

            state_ = kExpectHeaders;
            break;
        }

        case kExpectHeaders:
        {
            const char* crlf = buf->findCRLF();
            if (crlf == nullptr)
            {
                more = false;  // 数据不够
                break;
            }

            size_t lineLen = crlf - buf->peek();
            std::string line(buf->peek(), lineLen);
            buf->retrieve(lineLen + 2); // skip \r\n

            if (line.empty())
            {
                // 空行，头部结束 若方法为GET/PUT/PATCH则默认有消息体
                state_ = (request->getMethod() == HttpMethod::kPost ||
                            request->getMethod() == HttpMethod::kPut ||
                            request->getMethod() == HttpMethod::kPatch)
                                ? kExpectBody
                                : kGotAll;
                break;
            }

            // 解析 header: "Key: Value"
            auto colon = line.find(':');
            if (colon != std::string::npos)
            {
                std::string key = line.substr(0, colon);
                std::string value = line.substr(colon + 1);
                // trim leading spaces
                size_t start = value.find_first_not_of(" \t");
                if (start != std::string::npos)
                    value = value.substr(start);
                request->setHeader(key, value);
            }
            break;
        }

        case kExpectBody:
        {
            // 从 Content-Length 获取 body 长度
            std::string contentLenStr = request->getHeader("Content-Length");
            if (!contentLenStr.empty())
            {
                size_t contentLen = static_cast<size_t>(std::stoul(contentLenStr));
                if (buf->readableBytes() >= contentLen)
                {
                    request->setBody(buf->readAsString(contentLen));
                    state_ = kGotAll;
                }
                else
                {
                    more = false;  // body 数据未到齐
                }
            }
            else
            {
                // 没有 Content-Length，认为 body 为空
                state_ = kGotAll;
            }
            break;
        }

        case kGotAll:
            more = false;
            break;
        }
    }

    return state_ == kGotAll;
}

bool HttpContext::parseRequestLine(const std::string& line, HttpRequest* request)
{
    // 格式: METHOD PATH HTTP/1.1
    // 或:    METHOD PATH?query HTTP/1.1

    // 找到第一个空格前的部分 = method
    size_t space1 = line.find(' ');
    if (space1 == std::string::npos)
        return false;

    std::string methodStr = line.substr(0, space1);
    HttpMethod method = stringToMethod(methodStr);
    if (method == HttpMethod::kInvalid)
        return false;
    request->setMethod(method);

    // 找到第二个空格 = url 结束
    size_t space2 = line.rfind(' ');
    if (space2 == std::string::npos || space2 <= space1 + 1)
        return false;

    std::string url = line.substr(space1 + 1, space2 - space1 - 1);

    // 解析 URL 中的 path 和 query
    size_t qmark = url.find('?');
    if (qmark != std::string::npos)
    {
        request->setPath(url.substr(0, qmark));
        request->setQuery(url.substr(qmark + 1));
    }
    else
    {
        request->setPath(url);
    }

    // 解析 HTTP 版本
    std::string versionStr = line.substr(space2 + 1);
    if (versionStr == "HTTP/1.1")
        request->setVersion(HttpVersion::kHttp11);
    else if (versionStr == "HTTP/1.0")
        request->setVersion(HttpVersion::kHttp10);
    else
        return false;

    return true;
}
