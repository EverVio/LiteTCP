#pragma once

#include <litetcp/TcpPacket.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>

// 虚拟 IP 地址与端口结构体，用于在用户态标识套接字端点。
struct LiteSockAddr {
	uint32_t ip;	// 32 位 IPv4 地址，使用网络字节序。
	uint16_t port;	// 16 位端口号，使用网络字节序。
};

// 已发送但尚未确认的包结构，用于实现超时重传机制。
struct SentPacket {
	uint32_t seq;									  // 包的起始序列号。
	std::vector<uint8_t> data;						  // 序列化后的完整报文数据。
	int len;										  // 载荷数据的长度，SYN/FIN 占用 1 字节逻辑序列号。
	int retransmit_count;							  // 该包被重传的次数。
	std::chrono::steady_clock::time_point send_time;  // 发送或最近重传的时间戳。
};

// 环形缓冲区类，实现高性能 O(1) 数据读写，避免频繁的内存碎片。
class RingBuffer {
private:
	std::vector<uint8_t> buffer;  // 底层环形缓冲区数组。
	size_t head = 0;			  // 读取指针。
	size_t tail = 0;			  // 写入指针。
	size_t count = 0;			  // 当前已缓存的字节数。
	size_t capacity = 0;		  // 缓冲区的总容量上限。

public:
	// 构造函数，根据指定容量初始化缓冲区。
	RingBuffer(size_t cap = 0) : capacity(cap), buffer(cap), head(0), tail(0), count(0) {}

	// 重新设置环形缓冲区的大小，并清空历史读写指针。
	void resize(size_t cap) {
		capacity = cap;
		buffer.assign(cap, 0);
		head = 0;
		tail = 0;
		count = 0;
	}

	// 获取当前已缓存的数据大小（字节数）。
	size_t size() const {
		return count;
	}

	// 获取当前缓冲区的剩余空闲容量。
	size_t free_space() const {
		return capacity - count;
	}

	// 检查当前缓冲区是否为空。
	bool empty() const {
		return count == 0;
	}

	// 向缓冲区写入数据，返回实际成功写入的字节数。
	size_t write(const uint8_t* data, size_t len) {
		size_t written = std::min(len, capacity - count);
		if (written == 0)
			return 0;

		size_t first_part = std::min(written, capacity - tail);
		std::copy(data, data + first_part, buffer.begin() + tail);

		if (written > first_part) {
			size_t second_part = written - first_part;
			std::copy(data + first_part, data + written, buffer.begin());
		}

		tail = (tail + written) % capacity;
		count += written;
		return written;
	}

	// 从缓冲区中读取数据并消费它，返回实际成功读取的字节数。
	size_t read(uint8_t* dest, size_t len) {
		size_t to_read = std::min(len, count);
		if (to_read == 0)
			return 0;

		size_t first_part = std::min(to_read, capacity - head);
		std::copy(buffer.begin() + head, buffer.begin() + head + first_part, dest);

		if (to_read > first_part) {
			size_t second_part = to_read - first_part;
			std::copy(buffer.begin(), buffer.begin() + second_part, dest + first_part);
		}

		head = (head + to_read) % capacity;
		count -= to_read;
		return to_read;
	}

	// 在指定的偏移量处直接写入数据而不改变读写指针，主要用于乱序数据包直接原地落盘。
	void write_at(size_t offset, const uint8_t* data, size_t len) {
		// 防范无符号数溢出漏洞，并确保写入范围严格限制在空闲区间内。
		if (offset + len < offset || offset + len > capacity - count) {
			return;
		}

		size_t write_pos = (tail + offset) % capacity;
		size_t first_part = std::min(len, capacity - write_pos);
		std::copy(data, data + first_part, buffer.begin() + write_pos);

		if (len > first_part) {
			size_t second_part = len - first_part;
			std::copy(data + first_part, data + len, buffer.begin());
		}
	}

	// 推进写入指针并更新已缓存字节数，用于在乱序包被连续确认后合并转正。
	void advance_tail(size_t len) {
		if (count + len > capacity)
			return;
		tail = (tail + len) % capacity;
		count += len;
	}
};

// LiteTCP 套接字类，实现面向连接、支持拥塞控制与流量控制的可靠传输协议。
class TcpSocket {
private:
	std::mutex socket_mutex;		   // 互斥锁，保护套接字内部所有共享状态。
	std::condition_variable recv_cv;   // 接收数据的同步条件变量。
	std::condition_variable conn_cv;   // 连接建立的同步条件变量。
	std::condition_variable send_cv;   // 发送窗口空余的同步条件变量。
	std::condition_variable close_cv;  // 关闭套接字的同步条件变量。

	RingBuffer recv_buf;			// 接收缓冲区，使用环形队列减少内存移动开销。
	std::vector<uint8_t> send_buf;	// 发送缓冲区，暂存应用层调用 send 发送的数据。

	static constexpr size_t MAX_OOO_INTERVALS = 128;								 // 最大允许维护的乱序数据区间上限。
	std::array<std::pair<uint32_t, uint32_t>, MAX_OOO_INTERVALS> ooo_intervals;	 // 静态分配的乱序区间映射表。
	size_t ooo_count = 0;														 // 当前活跃的乱序区间个数。

	bool has_ooo_fin = false;													 // 是否存在乱序 FIN 报文。
	uint32_t ooo_fin_seq = 0;													 // 乱序 FIN 报文的逻辑序列号。
	uint32_t ooo_fin_ack = 0;													 // 乱序 FIN 报文对应的 ACK 号。

	// 将新的乱序区间插入表并尝试与前后重叠区间合并。
	void add_interval(uint32_t start, uint32_t end);
	void do_process_fin(uint32_t fin_seq, uint32_t ack);

	// 生命周期及数据处理辅助函数 (用于重构 handle_packet)
	void handle_handshake_packet(const TcpPacket& packet);
	void handle_established_or_close_packet(const TcpPacket& packet);
	void process_ack(const TcpPacket& packet, size_t payload_len);
	void process_payload(const TcpPacket& packet, size_t payload_len);
	void process_fin(const TcpPacket& packet);
	void process_state_cleanup(const TcpPacket& packet);

	TcpState state;			   // TCP 当前所处的状态机状态。
	LiteSockAddr local_addr;   // 本地绑定的网络地址端口。
	LiteSockAddr remote_addr;  // 对端建立连接的远端网络地址端口。

	// TCP 滑动窗口核心变量。
	uint32_t snd_una;	 // 已发送但尚未收到确认的最小序列号。
	uint32_t snd_nxt;	 // 下一个待发送的新数据的起始序列号。
	uint32_t rcv_nxt;	 // 期望从对端接收的下一个字节的序列号。
	uint32_t peer_rwnd;	 // 对端通告的剩余接收窗口大小。

	// 拥塞控制模块状态变量，基于 TCP Reno 实现。
	double cwnd;				 // 拥塞窗口大小（以字节为单位）。
	uint32_t ssthresh;			 // 慢启动阈值。
	int dup_ack_count;			 // 冗余 ACK 的连续计数。
	uint32_t last_ack_received;	 // 上一次收到的确认号。
	int congestion_state;		 // 当前拥塞状态（0为慢启动，1为拥塞避免，2为快速恢复）。
	uint32_t recover;			 // 记录进入快速恢复时的最高发送序列号。
	uint32_t rto_recover;		 // 记录进入 RTO 超时重传时的最高发送序列号。
	int send_waiting_threads;	 // 当前因发送窗口满而阻塞等待的线程数量，用于驱动零窗口探测机制。

	// 往返时间（RTT）估计与超时重传间隔（RTO）计算参数。
	double estimated_rtt;									// 平滑往返时间（秒）。
	double dev_rtt;											// 往返时间偏差均值（秒）。
	double rto;												// 当前超时重传阈值（秒）。
	bool rto_pending;										// RTO 超时重传且尚未收到新确认的挂起状态标志。
	std::chrono::steady_clock::time_point rto_timer_start;	// RTO 重传定时器的起始/重启绝对时间点。

	// 服务端监听套接字专用的半连接/全连接队列。
	std::queue<TcpSocket*> completed_queue;	 // 已完成三次握手的子套接字指针队列。
	TcpSocket* parent = nullptr;			 // 指向父监听套接字的指针。

	uint64_t active_event_id;  // 当前活跃的定时器事件 ID，用于惰性删除无效的定时器。

	std::vector<SentPacket> sent_packets;  // 已发送但未收到确认的包队列，用于重传。

	std::ofstream csv_log;							   // 数据吞吐日志文件输出流。
	std::chrono::steady_clock::time_point start_time;  // 套接字启动的绝对时间戳。

	// 写入当前连接的滑动窗口与拥塞控制指标到 CSV 文件。
	void write_log(const std::string& event = "state_change");

	// 计算当前的通告窗口大小，取决于接收缓冲区的剩余空闲空间。
	uint16_t get_advertised_window() const;

	friend class TimerManager;

	// 重新计算并调度最紧迫的超时事件（RTO、零窗口探测或 TIME_WAIT）。
	void update_timer_locked();

	// 定时器触发时的超时处理函数。
	void handle_timeout();

	// 构造并直接通过网络引擎发送一个无载荷的控制包（如 SYN, ACK, FIN 等）。
	void send_control_packet(uint8_t flags);

	// 重传指定的发送包，并记录重传计数。
	void retransmit_packet(SentPacket& pkt);

	// 检查对端是否为零窗口，若是且有线程阻塞，则定时发送 1 字节的探测包。
	void check_and_send_zero_window_probe();

	std::chrono::steady_clock::time_point last_zero_window_probe_time;	// 上一次发送零窗口探测包的时间戳。

public:
	TcpSocket();
	~TcpSocket();

	// 绑定本地地址端口，成功返回 0。
	int bind(LiteSockAddr bind_addr);

	// 进入监听状态，成功返回 0。
	int listen();

	// 阻塞并接受新连接，成功返回新创建的套接字实例。
	TcpSocket* accept();

	// 主动向对端发起握手连接，成功返回 0，失败或超时返回 -1。
	int connect(LiteSockAddr target_addr);

	// 向对端发送指定长度的数据，受窗口控制阻塞，返回实际发送字节数。
	int send(const void* buffer, int len);

	// 从接收缓冲区读取就绪数据，阻塞至有数据或断开，返回接收字节数。
	int recv(void* buffer, int len);

	// 启动 FIN 挥手关闭连接，成功返回 0。
	int close();

	// 收到网络层报文时的入口处理回调函数。
	void handle_packet(const TcpPacket& packet);

	// 获取当前套接字的状态机状态。
	TcpState get_state() const { return state; }

	// 获取本地绑定的地址。
	LiteSockAddr get_local_addr() const { return local_addr; }

	// 获取远端连接的地址。
	LiteSockAddr get_remote_addr() const { return remote_addr; }
};
