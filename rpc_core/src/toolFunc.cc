#include "../include/toolFunc.h"

void encode(Buffer* buff, uint16_t id, uint16_t status, google::protobuf::Message& msg)
{
    std::string body;
    if (!msg.SerializeToString(&body)) return;
    uint32_t total_len = static_cast<uint32_t>(body.size());

    buff->append(body.data(), body.size());

    //填充头部 按 Id-Status-Length逆序填入
    buff->prependInt32(total_len);

    buff->prependInt16(status);

    buff->prependInt16(id);
}

bool decode(Buffer* buff, Header& header, std::string& body)
{
    uint32_t minLength = static_cast<uint32_t>(sizeof(uint32_t) + sizeof(uint16_t) * 2);
    if(buff->readableBytes() < minLength) return false;
    
    //读取头部 按ID-Status-TotalLen的顺序依次读出
    header.id = buff->readUint16();
    header.status = buff->readUint16();
    header.totalLen = buff->readUint32();

    if(buff->readableBytes() < header.totalLen) return false;

    body = buff->readAsString(header.totalLen);
    return true;
}
