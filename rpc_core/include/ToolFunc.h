#ifndef CLEARMOON_TOOLFUNC_H
#define CLEARMOON_TOOLFUNC_H

#include "Message.pb.h"
#include "net/Buffer.h"

#include <cstdint>
#include <google/protobuf/message.h>

#define RPC_MAGIC_NUMBER 0xC1EA 

namespace cmlib = clearmoon::net;

const uint8_t kVersion = 1;

enum class RpcErrorCode : int32_t {
    NoError         = 0,

    //编解码类
    SerializeFailed = 1,   // 序列化失败
    MagicError      = 2,   // 魔数错误（decode 时发现）
    LengthError     = 3,   // 长度非法
    IncompletePacket= 4,   // 半包（一般不会直接通知对端）

    //服务端类
    TaskPoolFull     = 5,   // 任务线程池已满
    MethodNotFound   = 6,   // 未找到请求方法
    ParseError       = 7,   // 请求体反序列化失败
    InternalError    = 8,    // 业务处理内部异常
    UnknownError    = 99
};

enum Flag : uint8_t
{
    kRequest    = 0,
    kResponse   = 1,
    kHealthTick = 2,
    kAck        = 3
};


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
    uint8_t retransmit; //设置为1时表示启动传输层的超时重传
    uint8_t reserver[11];
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
// void encode(cmlib::Buffer* buff, uint8_t flags, uint8_t version, const RPC_Meta& meta, const google::protobuf::Message& msg);
//--------------
void encode(cmlib::Buffer* buff, Flag flag, uint8_t version, const RPC_Meta& meta,const google::protobuf::Message* msg);

//解码函数(负责把有关信息和消息数据从Buffer中提取出来)
bool decode(cmlib::Buffer* buff, Header& header, RPC_Meta& meta,std::string& body);


#endif
