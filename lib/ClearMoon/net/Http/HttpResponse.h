#ifndef CLEARMOON_NET_HTTP_HTTPRESPONSE_H
#define CLEARMOON_NET_HTTP_HTTPRESPONSE_H

#include "../Buffer.h"

#include <cstring>
#include <map>
#include <string>
#include <sys/stat.h>

namespace clearmoon 
{
namespace net 
{

enum class HttpStatusCode
{
    kUnknown,
    k200Ok = 200,
    k301MovedPermanently = 301,
    k302Found = 302,
    k400BadRequest = 400,
    k403Forbidden = 403,
    k404NotFound = 404,
    k405MethodNotAllowed = 405,
    k408RequestTimeout = 408,
    k413PayloadTooLarge = 413,
    k414UriTooLong = 414,
    k500InternalServerError = 500,
    k501NotImplemented = 501,
    k502BadGateway = 502,
    k503ServiceUnavailable = 503
};

class HttpResponse
{
public:
    HttpResponse() = default;
    ~HttpResponse() = default;

    void setStatus(HttpStatusCode code) { statusCode_ = code; }
    HttpStatusCode getStatus() const { return statusCode_; }

    void setBody(const std::string& body) { body_ = body; }
    const std::string& getBody() const { return body_; }

    void setContentType(const std::string& contentType)
    {
        setHeader("Content-Type", contentType);
    }

    void setHeader(const std::string& key, const std::string& value)
    {
        headers_[key] = value;
    }
    std::string getHeader(const std::string& key) const
    {
        auto it = headers_.find(key);
        return it != headers_.end() ? it->second : "";
    }

    void setCloseConnection(bool on) { closeConnection_ = on; }
    bool getCloseConnection() const { return closeConnection_; }

    // ========== 文件模式接口 ==========
    /**
     * @brief 设置响应为文件模式，使用 sendfile 零拷贝发送文件
     * @param path 文件路径
     * @param contentType MIME 类型，如 "video/mp4"
     * @return true 文件存在且成功设置，false 文件不存在
     */
    bool setFilePath(const std::string& path, const std::string& contentType);

    bool isFileMode() const { return fileMode_; }
    const std::string& getFilePath() const { return filePath_; }
    off_t getFileSize() const { return fileSize_; }

    /**
     * @brief 仅序列化响应头（不含 body），用于文件模式下先发头部再发文件
     */
    void serializeHeaders(Buffer* output) const;

    /**
     * @brief 将完整响应序列化写入 Buffer（普通模式，body 在内存中）
     */
    void appendToBuffer(Buffer* output) const
    {
        // 状态行: HTTP/1.1 200 OK\r\n
        char buf[64];
        int n = snprintf(buf, sizeof buf, "HTTP/1.1 %d ", static_cast<int>(statusCode_));
        output->append(buf, static_cast<size_t>(n));
        const char* reason = statusCodeToString(statusCode_);
        output->append(reason, strlen(reason));
        output->append("\r\n", 2);

        // 如果没有设置 Content-Length 且未设置 Transfer-Encoding，自动计算
        if (headers_.find("Content-Length") == headers_.end() &&
            headers_.find("Transfer-Encoding") == headers_.end())
        {
            char lenBuf[32];
            n = snprintf(lenBuf, sizeof lenBuf, "%zu", body_.size());
            setHeaderInternal("Content-Length", std::string(lenBuf, n));
        }

        // 头部
        for (const auto& [key, value] : headers_)
        {
            output->append(key.data(), key.size());
            output->append(": ", 2);
            output->append(value.data(), value.size());
            output->append("\r\n", 2);
        }

        // 空行
        output->append("\r\n", 2);

        // 响应体
        output->append(body_.data(), body_.size());
    }

    void reset()
    {
        statusCode_ = HttpStatusCode::kUnknown;
        body_.clear();
        headers_.clear();
        closeConnection_ = false;
        fileMode_ = false;
        filePath_.clear();
        fileSize_ = 0;
    }

    // 将状态码转为文本描述
    static const char* statusCodeToString(HttpStatusCode code)
    {
        switch (code)
        {
        case HttpStatusCode::k200Ok:                return "OK";
        case HttpStatusCode::k301MovedPermanently:  return "Moved Permanently";
        case HttpStatusCode::k302Found:             return "Found";
        case HttpStatusCode::k400BadRequest:        return "Bad Request";
        case HttpStatusCode::k403Forbidden:         return "Forbidden";
        case HttpStatusCode::k404NotFound:          return "Not Found";
        case HttpStatusCode::k405MethodNotAllowed:  return "Method Not Allowed";
        case HttpStatusCode::k408RequestTimeout:    return "Request Timeout";
        case HttpStatusCode::k413PayloadTooLarge:   return "Payload Too Large";
        case HttpStatusCode::k414UriTooLong:        return "URI Too Long";
        case HttpStatusCode::k500InternalServerError: return "Internal Server Error";
        case HttpStatusCode::k501NotImplemented:    return "Not Implemented";
        case HttpStatusCode::k502BadGateway:        return "Bad Gateway";
        case HttpStatusCode::k503ServiceUnavailable: return "Service Unavailable";
        default:                                    return "Unknown";
        }
    }

private:
    // 内部辅助：直接插入 header
    void setHeaderInternal(const std::string& key, const std::string& value) const
    {
        const_cast<HttpResponse*>(this)->headers_[key] = value;
    }

    HttpStatusCode statusCode_ = HttpStatusCode::kUnknown;
    std::string body_;
    mutable std::map<std::string, std::string> headers_;
    bool closeConnection_ = false;

    // 文件模式
    bool fileMode_ = false;
    std::string filePath_;
    off_t fileSize_ = 0;
};

// ========== 文件模式方法实现（inline 方式，避免新增 .cc 文件） ==========
inline bool HttpResponse::setFilePath(const std::string& path, const std::string& contentType)
{
    struct stat st;
    if (::stat(path.c_str(), &st) < 0)
    {
        return false;
    }

    fileMode_ = true;
    filePath_ = path;
    fileSize_ = st.st_size;

    setHeader("Content-Type", contentType);
    setHeader("Content-Length", std::to_string(fileSize_));
    setHeader("Accept-Ranges", "bytes");

    return true;
}

inline void HttpResponse::serializeHeaders(Buffer* output) const
{
    // 状态行: HTTP/1.1 200 OK\r\n
    char buf[64];
    int n = snprintf(buf, sizeof buf, "HTTP/1.1 %d ", static_cast<int>(statusCode_));
    output->append(buf, static_cast<size_t>(n));
    const char* reason = statusCodeToString(statusCode_);
    output->append(reason, strlen(reason));
    output->append("\r\n", 2);

    // 头部
    for (const auto& [key, value] : headers_)
    {
        output->append(key.data(), key.size());
        output->append(": ", 2);
        output->append(value.data(), value.size());
        output->append("\r\n", 2);
    }

    // 空行
    output->append("\r\n", 2);
}

}
}

#endif