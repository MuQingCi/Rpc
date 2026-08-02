#对一些接口的约定

Rpc_server.h中的getLocalIp()返回的是网卡的 IP,故建议监听地址改为"0.0.0.0"
```cpp
//读取本地地址环境变量，若有效则直接返回环境变量所指地址
const char* env = getenv("MY_RPC_LOCAL_IP");

if (env && env[0] != '\0') 
{
    return env;   // 环境变量有效，直接返回
}

//获取网卡IP等非环回地址
struct ifaddrs* ifAddrStruct = nullptr;
if(getifaddrs(&ifAddrStruct) == 0)
{
    for(struct ifaddrs* ifa = ifAddrStruct; ifa != nullptr; ifa = ifa->ifa_next)
    {
        //如果地址为空或者不为Ipv4则跳过
        if(!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET)
            continue;
        auto* addr_in = reinterpret_cast<struct sockaddr_in*>(ifa->ifa_addr);
        //排除环回地址
        if(addr_in->sin_addr.s_addr == htonl(INADDR_LOOPBACK))
            continue;

            char ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &addr_in->sin_addr, ip, sizeof(ip));
            freeifaddrs(ifAddrStruct);

            return std::string(ip);
    }
    freeifaddrs(ifAddrStruct);
}


```