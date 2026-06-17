#ifndef CLEARMOON_NET_HTTP_HTTPREQUEST_H
#define CLEARMOON_NET_HTTP_HTTPREQUEST_H

#include "../../base/noncopy.h"

#include <map>
#include <string>

namespace clearmoon 
{
namespace net 
{

enum class HttpMethod
{
    kInvalid,
    kGet,
    kPost,
    kHead,
    kPut,
    kDelete,
    kOptions,
    kPatch
};

enum class HttpVersion
{
    kUnknown,
    kHttp10,
    kHttp11
};

class HttpRequest : public noncopyable
{
public:
    HttpRequest() = default;
    ~HttpRequest() = default;

    /**
     * @brief Set/Get the Method/Path/Query/Version object
     * 
     * @param method 
     */
    void setMethod(HttpMethod method) { method_ = method; }
    HttpMethod getMethod() const { return method_; }

    void setPath(const std::string& path) { path_ = path; }
    const std::string& getPath() const { return path_; }

    void setQuery(const std::string& query) { query_ = query; }
    const std::string& getQuery() const { return query_; }

    void setVersion(HttpVersion version) { version_ = version; }
    HttpVersion getVersion() const { return version_; }

    void setBody(const std::string& body) { body_ = body; }
    const std::string& getBody() const { return body_; }
    void appendBody(const std::string& chunk) { body_ += chunk; }

    void setHeader(const std::string& key, const std::string& value)
    {
        headers_[key] = value;
    }
    std::string getHeader(const std::string& key) const
    {
        auto it = headers_.find(key);
        return it != headers_.end() ? it->second : "";
    }
    const std::map<std::string, std::string>& getHeaders() const
    {
        return headers_;
    }

    void reset()
    {
        method_ = HttpMethod::kInvalid;
        path_.clear();
        query_.clear();
        version_ = HttpVersion::kUnknown;
        body_.clear();
        headers_.clear();
    }

    // 工具函数：将方法名转为字符串
    static const char* methodToString(HttpMethod method)
    {
        switch (method)
        {
        case HttpMethod::kGet:     return "GET";
        case HttpMethod::kPost:    return "POST";
        case HttpMethod::kHead:    return "HEAD";
        case HttpMethod::kPut:     return "PUT";
        case HttpMethod::kDelete:  return "DELETE";
        case HttpMethod::kOptions: return "OPTIONS";
        case HttpMethod::kPatch:   return "PATCH";
        default:                   return "INVALID";
        }
    }

private:
    HttpMethod method_ = HttpMethod::kInvalid;
    std::string path_;
    std::string query_;
    HttpVersion version_ = HttpVersion::kUnknown;
    std::string body_;
    std::map<std::string, std::string> headers_;
};

}
}

#endif