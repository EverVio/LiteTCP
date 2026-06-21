#pragma once

#include <cstdint>
#include <cstring>
#include <iostream>
#include <vector>

// TCP 标志位常量定义，包括 SYN 同步、ACK 确认、FIN 结束和无标志位。
constexpr uint8_t TCP_FLAG_SYN = 0x08;	 // 同步标志，用于连接建立。
constexpr uint8_t TCP_FLAG_ACK = 0x04;	 // 确认标志，表明确认号有效。
constexpr uint8_t TCP_FLAG_FIN = 0x02;	 // 结束标志，用于释放连接。
constexpr uint8_t TCP_FLAG_NONE = 0x00;	 // 无任何标志位。

// TCP 状态机状态定义，对应标准 TCP 状态迁移图的各个阶段。
enum class TcpState {
	CLOSED = 0,		  // 关闭状态，代表没有活跃的连接。
	LISTEN = 1,		  // 监听状态，等待来自远端的 TCP 连接请求。
	SYN_SENT = 2,	  // 已发送 SYN，正在等待对端的 SYN 和 ACK 确认。
	SYN_RECV = 3,	  // 已收发 SYN 并发送了 SYN+ACK，等待对端的 ACK 确认。
	ESTABLISHED = 4,  // 连接已建立，可以进行双向正常数据交互。
	FIN_WAIT_1 = 5,	  // 主动关闭端发送了 FIN 包，等待对端的 ACK 确认或 FIN 请求。
	FIN_WAIT_2 = 6,	  // 主动关闭端收到对端对 FIN 的 ACK 确认，正在等待对端的 FIN 请求。
	CLOSE_WAIT = 7,	  // 被动关闭端收到对端 FIN 并回复 ACK 确认，等待本地程序调用 close。
	CLOSING = 8,	  // 双方同时执行关闭，已收对端 FIN 且己方 FIN 尚未被确认。
	LAST_ACK = 9,	  // 被动关闭端发送了 FIN 包，正在等待对端最后的 ACK 确认。
	TIME_WAIT = 10	  // 主动关闭端等待足够时间（2MSL）以确保对端收到了最后的 ACK。
};

// 辅助函数：将给定的 TcpState 状态枚举值转换为对应的英文大写字符串，用于日志记录与调试。
inline std::string state_to_string(TcpState state) {
	switch (state) {
		case TcpState::CLOSED:
			return "CLOSED";
		case TcpState::LISTEN:
			return "LISTEN";
		case TcpState::SYN_SENT:
			return "SYN_SENT";
		case TcpState::SYN_RECV:
			return "SYN_RECV";
		case TcpState::ESTABLISHED:
			return "ESTABLISHED";
		case TcpState::FIN_WAIT_1:
			return "FIN_WAIT_1";
		case TcpState::FIN_WAIT_2:
			return "FIN_WAIT_2";
		case TcpState::CLOSE_WAIT:
			return "CLOSE_WAIT";
		case TcpState::CLOSING:
			return "CLOSING";
		case TcpState::LAST_ACK:
			return "LAST_ACK";
		case TcpState::TIME_WAIT:
			return "TIME_WAIT";
		default:
			return "UNKNOWN";
	}
}

#pragma pack(push, 1)
// 紧凑打包的 LiteTCP 报文头结构体，固定大小为 20 字节。
struct TcpHeader {
	uint16_t source_port;		 // 源端口号。
	uint16_t destination_port;	 // 目的端口号。
	uint32_t seq_num;			 // 序列号，代表当前包中数据载荷第一个字节的序号。
	uint32_t ack_num;			 // 确认号，代表期望收到对端的下一个字节的序列号。
	uint8_t hlen;				 // 头部长度，固定为 20 字节。
	uint16_t plen;				 // 数据包总长度，包含头部和载荷。
	uint8_t flags;				 // 控制标志位组合（SYN, ACK, FIN等）。
	uint16_t advertised_window;	 // 通告接收窗口大小，用于接收端向发送端反馈流量控制状态。
	uint16_t checksum;			 // 16位 Internet 校验和。
};
#pragma pack(pop)

// LiteTCP 数据包类，整合了包头与数据载荷，并提供序列化、反序列化以及校验和计算等辅助逻辑。
class TcpPacket {
public:
	TcpHeader header;			// 报文头结构体实例。
	std::vector<uint8_t> data;	// 数据载荷字节数组。

	// 默认构造函数，将头部结构体置零并初始化 hlen 与 plen。
	TcpPacket();

	// 常用构造函数，通过端口、序号、控制标志、窗口大小和载荷数据直接构造完整的 TCP 包。
	TcpPacket(uint16_t src_port, uint16_t dst_port, uint32_t seq, uint32_t ack,
			  uint8_t flags, uint16_t adv_win, const std::vector<uint8_t>& payload = {});

	// 将当前数据包序列化为字节数组，在拷贝头部时会自动计算校验和填入。
	std::vector<uint8_t> serialize() const;

	// 从二进制缓冲区中反序列化提取数据包，长度不足或校验和不匹配时返回 false 并丢弃。
	static bool deserialize(const uint8_t* buffer, size_t length, TcpPacket& packet);

	// 计算标准 16 位 Internet 校验和，接收头部和载荷指针及其长度作为输入。
	static uint16_t calculate_checksum(const TcpHeader& hdr, const uint8_t* data_ptr, size_t data_len);
};
