#include "../include/ToolFunc.h"
#include "net/Log/Logger.h"
#include "net/Endian.h"

#include <cstdint>

void encode(Buffer* buff, uint8_t flags, uint8_t version, const RPC_Meta& meta, const google::protobuf::Message& msg)
{
    std::string body;
    if (!msg.SerializeToString(&body)) 
    {
        buff->retrieveAll();
        return;
    }

    //计算报文总大小
    uint32_t header_len = static_cast<uint32_t>(sizeof(Header));
    uint32_t meta_len = static_cast<uint32_t>(sizeof(RPC_Meta));
    uint32_t body_len = static_cast<uint32_t>(body.size());
    uint32_t total_len = header_len + meta_len + body_len;

    //发送时无需使用Header

    //Meta
    RPC_Meta netMeta{};
    netMeta.seq = host64ToNet(meta.seq);
    netMeta.method_id = host32ToNet(meta.method_id);
    netMeta.timeout = host32ToNet(meta.timeout);
    netMeta.err_code = host32ToNet(static_cast<uint32_t>(meta.err_code));

    buff->append(reinterpret_cast<const char*>(&netMeta), sizeof(RPC_Meta));

    //填充业务数据
    buff->append(body.data(), body.size());

    //填充头部 按 Magic-Flags-Vertsion-TotalLength逆序填入
    //字节序转换问题在Buffer内部已得到解决
    buff->prependInt32(total_len);
    buff->prependInt8(version);
    buff->prependInt8(flags);
    buff->prependInt16(RPC_MAGIC_NUMBER);
}

bool decode(Buffer* buff, Header& header, RPC_Meta& meta,std::string& body)
{
    DecodeError err = DecodeError::NoError;

    const uint32_t minLength = static_cast<uint32_t>(sizeof(Header) + sizeof(RPC_Meta));
    if(buff->readableBytes() < minLength) 
    {
        buff->readAllAsString();
        return false;
    }
    //读取头部 按Header-Meta-body的顺序依次读出
    //Header
    header.Magic = buff->readUint16();
    if(header.Magic != RPC_MAGIC_NUMBER) 
    {
        LOG_WARNING <<"接收到一个非法数据包(魔数检验错误) !";
        err = DecodeError::MagicError;
    }

    header.Flags = buff->readUint8();
    header.Version = buff->readUint8();

    header.TotalLength = buff->readUint32();

    if(header.TotalLength < minLength || header.TotalLength > buff->readableBytes() + sizeof(Header)) 
    {
        err = DecodeError::LengthError;
        LOG_WARNING<< "接收到的Buffer数据小于最小长度或接收不完全";
        return false;
    }
    
    //Meta
    meta.seq = buff->readUint64();
    meta.method_id = buff->readUint32();
    meta.timeout = buff->readUint32();
    meta.err_code = static_cast<int32_t>(buff->readUint32());

    if(buff->readableBytes() < 12) return false;

    buff->read(reinterpret_cast<char*>(meta.reserver), 12);

    uint32_t body_Len = header.TotalLength - minLength;
    if(buff->readableBytes() < body_Len) return false;

    body = buff->readAsString(body_Len);
    return true;
}
