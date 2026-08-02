#include "../include/ToolFunc.h"
#include "net/Buffer.h"
#include "net/Log/Logger.h"
#include "net/Endian.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>

/**
 * @brief 只窥探Buffer中的数据，不修改其中数据
 * 
 * @param buff 
 * @return size_t 
 */
static size_t findMagicInBuffer(cmlib::Buffer* buff)
{
    if(buff == nullptr) return static_cast<size_t>(-1);

    //将魔数转换成大端字节序并分别保留其高位/低位字节以使用search寻找相同子序列
    uint16_t magicNet = cmlib::host16ToNet(RPC_MAGIC_NUMBER);
    char magicBytes[2];
    magicBytes[0] = static_cast<char>((magicNet >>8) & 0xFF);
    magicBytes[1] = static_cast<char>(magicNet & 0xFF);

    const char* start = buff->peek();
    const char* end = start + buff->readableBytes();
    const char* pos = std::search(start,end,magicBytes,magicBytes + 2);

    if(pos == end)
        return static_cast<size_t>(-1);
    return static_cast<size_t>(pos - start);
}



void encode(cmlib::Buffer* buff, Flag flag, uint8_t version, const RPC_Meta& meta,const google::protobuf::Message* msg)
{
    std::string body;
    uint32_t body_len = 0;

    RPC_Meta outMeta = meta;

    if(msg != nullptr)
    {   
        if (!msg->SerializeToString(&body)) 
        {
            // buff->retrieveAll();
            outMeta.err_code = static_cast<int32_t>(RpcErrorCode::SerializeFailed);
        }else {
            body_len = static_cast<uint32_t>(body.size());
            outMeta.err_code = static_cast<int32_t>(RpcErrorCode::NoError);
        }
    }

    //计算报文总大小
    uint32_t header_len = static_cast<uint32_t>(sizeof(Header));
    uint32_t meta_len = static_cast<uint32_t>(sizeof(RPC_Meta));
    uint32_t total_len = header_len + meta_len + body_len;

    //发送时无需使用Header

    //字节序转换
    //Meta
    RPC_Meta netMeta{};
    netMeta.seq        = cmlib::host64ToNet(outMeta.seq);
    netMeta.method_id  = cmlib::host32ToNet(outMeta.method_id);
    netMeta.timeout    = cmlib::host32ToNet(outMeta.timeout);
    netMeta.err_code   = cmlib::host32ToNet(static_cast<int32_t>(outMeta.err_code));
    netMeta.retransmit = outMeta.retransmit;
    netMeta.traceID    = cmlib::host64ToNet(outMeta.traceID);
    std::memcpy(netMeta.reserver, outMeta.reserver, sizeof(outMeta.reserver));

    buff->append(reinterpret_cast<const char*>(&netMeta), sizeof(RPC_Meta));

    //填充业务数据
    if(body_len > 0)
        buff->append(body.data(), body.size());

    //填充头部 按 Magic-Flags-Vertsion-TotalLength逆序填入
    //字节序转换问题在Buffer内部已得到解决
    uint8_t flags = static_cast<uint8_t>(flag);
    buff->prependInt32(total_len);
    buff->prependInt8(version);
    buff->prependInt8(flags);
    buff->prependInt16(RPC_MAGIC_NUMBER);
}

bool decode(cmlib::Buffer* buff, Header& header, RPC_Meta& meta,std::string& body)
{
    if(buff == nullptr) return false;

    const size_t kHeaderLen = sizeof(Header);
    const size_t kMetaLen   = sizeof(RPC_Meta);
    const size_t kMinLen    = kHeaderLen + kMetaLen;

    constexpr size_t kMaxMessageSize = 10 * 1024 * 1024;//10MB

    while (buff->readableBytes() >= kHeaderLen) {
        uint16_t magicNet;
        std::memcpy(&magicNet, buff->peek(),sizeof(magicNet));
        uint16_t magic = cmlib::netToHost16(magicNet);

        if(magic != RPC_MAGIC_NUMBER)
        {
            //当前魔数错误，在Buffer中寻找下一个正确的魔数
            size_t offset = findMagicInBuffer(buff);
            if(offset == static_cast<size_t>(-1))
            {
                //并不存在一个正确的魔数,把所有数据包全部丢弃
                LOG_INFO<< "Discard "<< buff->readableBytes() <<" Bytes of data";
                buff->retrieveAll();
                return false;
            }
            else {
                //将第一个正确的魔数之前的数据包全部丢弃
                buff->retrieve(offset);
                LOG_INFO<< "Discard "<< offset <<" Bytes of data";
                continue;
            }
        }//if(magic != RPC_MAGIC_NUMBER)

        uint32_t totalLenNet;
        std::memcpy(&totalLenNet, buff->peek() + offsetof(Header, TotalLength), sizeof(totalLenNet));
        uint32_t totalLen = cmlib::netToHost32(totalLenNet);

        //长度不合法，先读取一字节数据以防假魔数
        if(totalLen < kMinLen || totalLen > kMaxMessageSize)
        {
            buff->retrieve(1);
            LOG_INFO<< "Discard "<< 1 <<" Bytes of data";
            continue;
        }

        if(totalLen > buff->readableBytes()){
            //数据包不完全，等下次读取
            return false;
        }

        header.Magic       = buff->readUint16();
        header.Flags       = buff->readUint8();
        header.Version     = buff->readUint8();
        header.TotalLength = buff->readUint32();
        
        //Meta
        meta.seq           = buff->readUint64();
        meta.method_id     = buff->readUint32();
        meta.timeout       = buff->readUint32();
        meta.err_code      = static_cast<int32_t>(buff->readUint32());
        meta.retransmit    = buff->readUint8();
        meta.traceID       = buff->readUint64();

        buff->read(reinterpret_cast<char*>(meta.reserver), sizeof(meta.reserver));

        uint32_t body_Len = totalLen - kMinLen;
        body = buff->readAsString(body_Len);

        return true;
    }
    return false;
}
// bool decode(cmlib::Buffer* buff, Header& header, RPC_Meta& meta,std::string& body)
// {
//     // DecodeError err = DecodeError::NoError;

//     const uint32_t minLength = static_cast<uint32_t>(sizeof(Header) + sizeof(RPC_Meta));
//     if(buff->readableBytes() < minLength) 
//     {
//         buff->readAllAsString();
//         return false;
//     }


//     //读取头部 按Header-Meta-body的顺序依次读出
//     //Header
//     header.Magic = buff->readUint16();
//     if(header.Magic != RPC_MAGIC_NUMBER) 
//     {
//         LOG_WARNING <<"接收到一个非法数据包(魔数检验错误) !";
//         // err = DecodeError::MagicError;
//     }

//     header.Flags = buff->readUint8();
//     header.Version = buff->readUint8();

//     header.TotalLength = buff->readUint32();

//     if(header.TotalLength < minLength || header.TotalLength > buff->readableBytes() + sizeof(Header)) 
//     {
//         // err = DecodeError::LengthError;
//         LOG_WARNING<< "接收到的Buffer数据小于最小长度或接收不完全";
//         return false;
//     }
    
//     //Meta
//     meta.seq = buff->readUint64();
//     meta.method_id = buff->readUint32();
//     meta.timeout = buff->readUint32();
//     meta.err_code = static_cast<int32_t>(buff->readUint32());

//     if(buff->readableBytes() < 12) return false;

//     buff->read(reinterpret_cast<char*>(meta.reserver), 12);

//     uint32_t body_Len = header.TotalLength - minLength;
//     if(buff->readableBytes() < body_Len) return false;

//     body = buff->readAsString(body_Len);
//     return true;
// }
