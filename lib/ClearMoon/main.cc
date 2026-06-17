#include "EventLoop.h"
#include "net/TcpServer.h"
#include <cstddef>
#include <cstring>
#include <netinet/in.h>

using namespace clearmoon;
using namespace clearmoon::net;

// int main()
// {
//     EventLoop* loop;
//     sockaddr_in addr{0};
//     size_t len = sizeof(struct sockaddr_in);

//     ::memset(&addr, 0, len);
//     addr.sin_addr = 
//     TcpServer(loop)
// }
