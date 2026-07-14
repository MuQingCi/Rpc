#ifndef CLEARMOON_RPC_EXCEPTIONS_H
#define CLEARMOON_RPC_EXCEPTIONS_H

#include <cstdint>
#include <stdexcept>
#include <string>

///=============================================================================
/// @brief RPC调用超时异常
///=============================================================================
class RpcTimeoutException : public std::runtime_error
{
public:
    explicit RpcTimeoutException(uint64_t seq, uint32_t timeoutMs)
        : std::runtime_error("RPC call timeout, seq: " + std::to_string(seq)
                             + ", timeout: " + std::to_string(timeoutMs) + "ms")
        , seq_(seq)
        , timeoutMs_(timeoutMs)
    {}
    uint64_t seq()      const noexcept { return seq_; }
    uint32_t timeoutMs() const noexcept { return timeoutMs_; }
private:
    uint64_t seq_;
    uint32_t timeoutMs_;
};

///=============================================================================
/// @brief RPC连接异常（网络断开 / 重连失败）
///=============================================================================
class RpcConnectionException : public std::runtime_error
{
public:
    explicit RpcConnectionException(const std::string& msg)
        : std::runtime_error("RPC connection error: " + msg)
    {}
};

#endif // CLEARMOON_RPC_EXCEPTIONS_H