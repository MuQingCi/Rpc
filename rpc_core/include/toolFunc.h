#ifndef CLEARMOON_TOOLFUNC_H
#define CLEARMOON_TOOLFUNC_H

#include "message.pb.h"
#include "net/Buffer.h"

#include <cstdint>
#include <google/protobuf/message.h>

#define RPC_MAGIC_NUMBER 0xC1EA 

using namespace clearmoon;
using namespace clearmoon::net;

//8Bytes
#pragma pack(push,1)
struct Header
{
    uint16_t Magic;
    uint8_t Flags;  //0请求/1回应/2心跳 /3ACK
    uint8_t Version;
    uint32_t TotalLength;
};
#pragma pack(pop)

//32Bytes
#pragma pack(push, 1)
struct RPC_Meta
{
    uint64_t seq;
    uint32_t method_id; //0-Echo/1-Add
    uint32_t timeout;
    int32_t err_code;
    uint8_t reserver[12];
};
#pragma pack(pop)

enum MethodID : uint32_t
{
    Echo = 0,
    Add = 1,
};

template<typename Resquest>
uint32_t getMethodId();

template<>
inline uint32_t getMethodId<CLRPC::EchoRequest>(){ return static_cast<uint32_t>(MethodID::Echo);};

template<>
inline uint32_t getMethodId<CLRPC::AddRequest>(){ return static_cast<uint32_t>(MethodID::Add);};


//编码函数(负责把有关信息和消息数据填入Buffer头部)
void encode(Buffer* buff, uint8_t flags, uint8_t version, const RPC_Meta& meta, const google::protobuf::Message& msg);

//解码函数(负责把有关信息和消息数据从Buffer中提取出来)
bool decode(Buffer* buff, Header& header, RPC_Meta& meta,std::string& body);


#endif
