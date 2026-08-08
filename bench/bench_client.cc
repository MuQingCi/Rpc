// =============================================================================
// bench_client.cc — CMRPC 网络/RPC 压测器（独立新增文件，不改任何现有源码）
//
// 复用现有 Client 库的静态单地址模式，测量端到端 RPC 性能：
//   吞吐(QPS)、延迟分位数(P50/P90/P99/P999)、错误率、线宽吞吐估算
//
// 用法:
//   bench_client [--host=IP] [--port=N] [--conns=N] [--threads=N]
//                [--duration=S] [--warmup=S] [--payload=B]
//                [--method=echo|add] [--timeout-ms=M] [--filters]
//
// 说明:
//   * 每个 worker 线程串行发起调用(协程), 并发度 = min(threads, conns)
//   * 每条调用持有 Borrowed 连接直至响应返回(BUSY->IDLE), 因此
//     单连接在途请求=1, 连接池大小即并发上限
//   * --filters 开启与 Rpc_Async_Client_Test 相同的过滤器链(限流rate调高避免压测被限流)
// =============================================================================

#include "Client/Client.h"
#include "Client/ClientConfig.h"
#include "Client/Rpc_exceptions.h"
#include "ToolFunc.h"
#include "net/EventLoop.h"
#include "net/EventLoopThread.h"
#include "net/InetAddress.h"
#include "net/Log/Logger.h"
#include "Message.pb.h"

#include "Filter/TraceFilter.h"
#include "Filter/MetricsFilter.h"
#include "Filter/RateLimitFilter.h"
#include "Filter/CircuitBreakerFilter.h"
#include "Filter/TokenBucket.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <memory>
#include <map>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace cmlib = clearmoon::net;

struct Options
{
    std::string host    = "127.0.0.1";
    uint16_t    port    = 12345;
    size_t      conns   = 64;    // 连接池大小(并发上限)
    size_t      threads = 64;    // worker 线程数
    int         duration = 10;   // 测量时长(秒)
    int         warmup   = 2;    // 预热时长(秒), 结果丢弃
    size_t      payload  = 16;   // Echo 请求 msg 字节数
    std::string method   = "echo";
    int         timeoutMs = 5000;
    bool        filters  = false;
    bool        graceful = false;   // 默认打印结果后快速退出(绕过 ConnectionPool 析构死锁缺陷);
                                   // --graceful 时走完整清理路径(可复现该缺陷)
};

static Options parseArgs(int argc, char** argv)
{
    Options o;
    for (int i = 1; i < argc; ++i)
    {
        std::string arg = argv[i];
        auto has = [&](const char* k) { return arg.rfind(k, 0) == 0; };
        auto val = [&](const char* k) { return arg.substr(strlen(k)); };
        if (has("--host="))        o.host      = val("--host=");
        else if (has("--port="))   o.port      = static_cast<uint16_t>(std::stoi(val("--port=")));
        else if (has("--conns="))  o.conns     = std::stoull(val("--conns="));
        else if (has("--threads="))o.threads   = std::stoull(val("--threads="));
        else if (has("--duration="))o.duration = std::stoi(val("--duration="));
        else if (has("--warmup=")) o.warmup    = std::stoi(val("--warmup="));
        else if (has("--payload="))o.payload   = std::stoull(val("--payload="));
        else if (has("--method=")) o.method    = val("--method=");
        else if (has("--timeout-ms=")) o.timeoutMs = std::stoi(val("--timeout-ms="));
        else if (arg == "--filters") o.filters = true;
        else if (arg == "--graceful") o.graceful = true;
        else if (arg == "-h" || arg == "--help")
        {
            std::printf("用法: bench_client [--host=IP] [--port=N] [--conns=N] [--threads=N]\n"
                        "        [--duration=S] [--warmup=S] [--payload=B]\n"
                        "        [--method=echo|add] [--timeout-ms=M] [--filters]\n");
            std::exit(0);
        }
        else
        {
            std::fprintf(stderr, "未知参数: %s\n", arg.c_str());
            std::exit(1);
        }
    }
    if (o.threads > o.conns)
    {
        std::fprintf(stderr, "[warn] threads(%zu) > conns(%zu), 超过连接池并发上限会抛异常, 已将 threads 降为 conns\n",
                     o.threads, o.conns);
        o.threads = o.conns;
    }
    return o;
}

static std::atomic<bool> g_running{true};
static std::atomic<size_t> g_finished{0};

struct WorkerStats
{
    std::vector<uint64_t> latUs;  // 成功调用的端到端延迟(us)
    std::atomic<uint64_t> ok{0};
    std::atomic<uint64_t> err{0};
    std::atomic<uint64_t> errAcquire{0};   // acquire 超时/无健康连接
    std::atomic<uint64_t> errTimeout{0};   // RpcTimeoutException
    std::atomic<uint64_t> errConn{0};      // RpcConnectionException(非 acquire)
    std::atomic<uint64_t> errRemote{0};    // RpcRemoteException
    std::atomic<uint64_t> errOther{0};     // 其它异常
    std::map<int32_t, uint64_t> remoteCodes;  // RpcRemoteException 错误码分布(受 mtx 保护)
    std::mutex            mtx;    // 保护 latUs(最终化时可能仍有 worker 在写)
};

static void worker(std::shared_ptr<RPCClient> client, const Options& o, WorkerStats& st)
{
    const uint32_t methodId = (o.method == "add")
                                  ? static_cast<uint32_t>(MethodID::Add)
                                  : static_cast<uint32_t>(MethodID::Echo);

    CLRPC::EchoRequest ereq;
    CLRPC::AddRequest  areq;
    if (methodId == static_cast<uint32_t>(MethodID::Echo))
        ereq.set_msg(std::string(o.payload, 'x'));
    else
    {
        areq.set_a(1);
        areq.set_b(2);
    }

    const auto timeout = std::chrono::milliseconds(o.timeoutMs);
    std::vector<uint64_t>& lat = st.latUs;
    lat.reserve(1u << 20);

    while (g_running.load(std::memory_order_relaxed))
    {
        auto t0 = std::chrono::steady_clock::now();
        bool ok = false;
        try
        {
            if (methodId == static_cast<uint32_t>(MethodID::Echo))
            {
                auto resp = client->CallAsync<CLRPC::EchoRequest, CLRPC::EchoResponse>(ereq, timeout, methodId).get();
                ok = resp.code() == 0;
            }
            else
            {
                auto resp = client->CallAsync<CLRPC::AddRequest, CLRPC::AddResponse>(areq, timeout, methodId).get();
                ok = true;
            }
        }
        catch (const RpcConnectionException& e)
        {
            // acquire 超时/无健康连接 与 发送时连接不可用 区分不开, 统一计数;
            // 通过字符串信息粗略区分
            std::string what = e.what();
            if (what.find("acquire") != std::string::npos || what.find("Healthy") != std::string::npos)
                st.errAcquire.fetch_add(1, std::memory_order_relaxed);
            else
                st.errConn.fetch_add(1, std::memory_order_relaxed);
            ok = false;
        }
        catch (const RpcTimeoutException&)
        {
            st.errTimeout.fetch_add(1, std::memory_order_relaxed);
            ok = false;
        }
        catch (const RpcRemoteException& e)
        {
            st.errRemote.fetch_add(1, std::memory_order_relaxed);
            {
                std::lock_guard<std::mutex> lk(st.mtx);
                st.remoteCodes[e.code()]++;
            }
            ok = false;
        }
        catch (const std::exception&)
        {
            st.errOther.fetch_add(1, std::memory_order_relaxed);
            ok = false;   // 超时 / 连接错误 / 远端错误
        }
        auto us = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - t0).count());
        if (ok)
        {
            std::lock_guard<std::mutex> lk(st.mtx);
            lat.push_back(us);
            st.ok.fetch_add(1, std::memory_order_relaxed);
        }
        else
            st.err.fetch_add(1, std::memory_order_relaxed);
    }
    g_finished.fetch_add(1, std::memory_order_relaxed);
}

static double percentile(const std::vector<uint64_t>& v, double p)
{
    if (v.empty()) return 0.0;
    size_t idx = static_cast<size_t>(p * (v.size() - 1));
    return static_cast<double>(v[idx]);
}


int main(int argc, char** argv)
{
    Options o = parseArgs(argc, argv);

    // 静音网络库日志, 减少压测期间的日志开销与噪声
    clearmoon::net::Logger::set_GlobalLevel(clearmoon::net::LogLevel::ERRNOR);
    // 忽略 SIGPIPE: 框架未在 send 时使用 MSG_NOSIGNAL, 向已关闭连接写数据会触发
    // SIGPIPE; 压测器必须忽略之以免进程被信号杀死(该问题同时是框架的稳健性缺陷,
    // 会在报告中单列)。
    std::signal(SIGPIPE, SIG_IGN);

    std::printf("== CMRPC bench ==\n");
    std::printf("  server     : %s:%u\n", o.host.c_str(), o.port);
    std::printf("  conns      : %zu\n", o.conns);
    std::printf("  threads    : %zu\n", o.threads);
    std::printf("  payload    : %zu B\n", o.payload);
    std::printf("  method     : %s\n", o.method.c_str());
    std::printf("  warmup/dur : %ds / %ds\n", o.warmup, o.duration);
    std::printf("  timeout    : %d ms\n", o.timeoutMs);
    std::printf("  filters    : %s\n", o.filters ? "on" : "off");

    // 事件循环线程
    cmlib::EventLoopThread loopThread;
    cmlib::EventLoop* loop = loopThread.start();

    // 静态单地址客户端(不经过 etcd 服务发现, 测量纯 RPC 路径)
    ClientConfig cfg;
    cfg.connPerServer       = o.conns;
    cfg.strategy            = LoadBalanceStrategy::RoundRobin;
    cfg.healthCheckInterval = std::chrono::seconds(5);
    cfg.callTimeout         = std::chrono::milliseconds(o.timeoutMs);

    auto client = std::make_shared<RPCClient>(loop, cmlib::InetAddress(o.host, o.port, false), cfg);

    if (o.filters)
    {
        // 与 Rpc_Async_Client_Test 相同的过滤器链, 但限流调高以避免压测被限流
        auto bucket = std::make_shared<TokenBucket>(1000000000.0, 2000000000u);
        client->addFilter(std::make_shared<TraceFilter>());
        client->addFilter(std::make_shared<MetricsFilter>());
        client->addFilter(std::make_shared<RateLimitFilter>(bucket));
        client->addFilter(std::make_shared<CircuitBreakerFilter>(60, std::chrono::seconds(10)));
    }

    // 等待 TCP 连接建立
    std::printf("  waiting for %zu connections...\n", o.conns);
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 启动 worker
    std::vector<std::thread> pool;
    std::vector<WorkerStats> stats(o.threads);
    pool.reserve(o.threads);
    for (size_t i = 0; i < o.threads; ++i)
        pool.emplace_back(worker, client, o, std::ref(stats[i]));

    // 预热(丢弃结果)
    std::this_thread::sleep_for(std::chrono::seconds(o.warmup));
    for (auto& st : stats)
    {
        std::lock_guard<std::mutex> lk(st.mtx);
        st.latUs.clear();
        st.ok.store(0, std::memory_order_relaxed);
        st.err.store(0, std::memory_order_relaxed);
    }

    // 进入测量窗口
    auto tStart = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::seconds(o.duration));
    g_running.store(false, std::memory_order_relaxed);
    auto tEnd = std::chrono::steady_clock::now();

    // 看门狗: 正常情况 RpcAwaiter 超时(timeoutMs)会唤醒卡住的 worker;
    // 若框架缺陷导致 worker 永久挂起, 等待 timeoutMs+3s 后仍按已累计数据出数,
    // 最后 _exit 快速退出(不 join, 绕过 ConnectionPool 析构死锁)。
    const auto grace = std::chrono::seconds(o.timeoutMs / 1000 + 3);
    auto wd = std::chrono::steady_clock::now() + grace;
    while (g_finished.load(std::memory_order_relaxed) < o.threads &&
           std::chrono::steady_clock::now() < wd)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    const double wallSec = std::chrono::duration<double>(tEnd - tStart).count();

    // 汇总(worker 可能仍在运行, 用锁与原子读取)
    uint64_t ok = 0, err = 0;
    std::vector<uint64_t> allLat;
    allLat.reserve(o.threads * (1u << 20));
    for (auto& st : stats)
    {
        std::lock_guard<std::mutex> lk(st.mtx);
        ok += st.ok.load(std::memory_order_relaxed);
        err += st.err.load(std::memory_order_relaxed);
        allLat.insert(allLat.end(), st.latUs.begin(), st.latUs.end());
    }
    std::sort(allLat.begin(), allLat.end());

    const double qps = ok / wallSec;
    const double avgUs = allLat.empty()
                             ? 0.0
                             : std::accumulate(allLat.begin(), allLat.end(), 0.0) / allLat.size();

    // 汇总错误分类
    uint64_t errAcq = 0, errTo = 0, errC = 0, errR = 0, errO = 0;
    for (auto& st : stats)
    {
        errAcq += st.errAcquire.load(std::memory_order_relaxed);
        errTo  += st.errTimeout.load(std::memory_order_relaxed);
        errC   += st.errConn.load(std::memory_order_relaxed);
        errR   += st.errRemote.load(std::memory_order_relaxed);
        errO   += st.errOther.load(std::memory_order_relaxed);
    }

    std::printf("\n== results (measured %.1fs, %llu successful calls) ==\n",
                wallSec, static_cast<unsigned long long>(ok));
    std::printf("  QPS        : %.0f req/s\n", qps);
    std::printf("  errors     : %llu (%.2f%%)\n",
                static_cast<unsigned long long>(err),
                100.0 * err / static_cast<double>(ok + err));
    std::printf("    - acquire : %llu\n    - timeout : %llu\n    - conn    : %llu\n    - remote  : %llu\n    - other   : %llu\n",
                static_cast<unsigned long long>(errAcq),
                static_cast<unsigned long long>(errTo),
                static_cast<unsigned long long>(errC),
                static_cast<unsigned long long>(errR),
                static_cast<unsigned long long>(errO));

    // 远端错误码分布
    {
        std::map<int32_t, uint64_t> codes;
        for (auto& st : stats)
        {
            std::lock_guard<std::mutex> lk(st.mtx);
            for (auto& kv : st.remoteCodes) codes[kv.first] += kv.second;
        }
        std::printf("    - remote codes: ");
        if (codes.empty()) std::printf("(none)\n");
        else
        {
            bool first = true;
            for (auto& kv : codes)
            {
                if (!first) std::printf(", ");
                std::printf("%d=%llu", kv.first, static_cast<unsigned long long>(kv.second));
                first = false;
            }
            std::printf("\n");
        }
    }
    std::printf("  latency avg: %.0f us\n", avgUs);
    if (!allLat.empty())
        std::printf("  latency    : min=%.0f p50=%.0f p90=%.0f p99=%.0f p999=%.0f max=%.0f us\n",
                    static_cast<double>(allLat.front()),
                    percentile(allLat, 0.50), percentile(allLat, 0.90),
                    percentile(allLat, 0.99), percentile(allLat, 0.999),
                    static_cast<double>(allLat.back()));

    // 线宽字节估算: Header(8B) + RPC_Meta(32B) + proto 体
    const size_t reqBytes  = 8 + 32 + (o.method == "add" ? 8 : 8 + o.payload + 2);
    const size_t respBytes = 8 + 32 + (o.method == "add" ? 8 : 8 + o.payload + 13);
    const double mbs = qps * (reqBytes + respBytes) / (1024.0 * 1024.0);
    std::printf("  wire bytes : ~%zu B req / ~%zu B resp, 双向吞吐≈%.2f MB/s\n",
                reqBytes, respBytes, mbs);

    // 清理: 快速退出(_exit 不跑析构, 绕过 ConnectionPool 析构对已断开连接的
    // closeCv_.wait 死锁缺陷; 统计已完成, 无需等待 worker join)。
    // --graceful 时走完整清理路径以复现/观察该缺陷。
    fflush(stdout);
    if (o.graceful)
    {
        for (auto& t : pool) if (t.joinable()) t.join();
        client.reset();
        loop->quit();
        loopThread.join();
        return err == 0 ? 0 : 2;
    }
    _exit(err == 0 ? 0 : 2);
}

