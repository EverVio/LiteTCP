#include <arpa/inet.h>
#include <litetcp/TcpPacket.h>
#include <litetcp/TcpSocket.h>

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <random>

#include "NetworkEngine.h"
#include "TimerManager.h"

// 设定接收缓冲区最大限制以及每个 TCP 报文段的 MSS 大小（排除 20 字节首部后为 1380 字节）。
constexpr size_t MAX_RECV_BUF_SIZE = 65536;
constexpr uint32_t MSS = 1400 - sizeof(TcpHeader);

TcpSocket::TcpSocket()
	: state(TcpState::CLOSED),
	  rcv_nxt(0),
	  peer_rwnd(32 * MSS),
	  cwnd(static_cast<double>(MSS)),
	  ssthresh(32 * MSS),
	  dup_ack_count(0),
	  congestion_state(0),	// 拥塞状态默认为 0（慢启动阶段）。
	  recover(0),
	  send_waiting_threads(0),
	  estimated_rtt(0.1),
	  dev_rtt(0.05),
	  rto(0.2),
	  rto_pending(false),
	  rto_timer_start(std::chrono::steady_clock::now()),
	  active_event_id(0),
	  recv_buf(MAX_RECV_BUF_SIZE) {
	// 使用随机数作为本端连接的初始序列号（ISN），并初始化发送窗口。
	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<uint32_t> distr(1000, 1000000);
	uint32_t isn = distr(gen);
	snd_una = isn;
	snd_nxt = isn;
	last_ack_received = isn;

	std::memset(&local_addr, 0, sizeof(local_addr));
	std::memset(&remote_addr, 0, sizeof(remote_addr));
	start_time = std::chrono::steady_clock::now();
	last_zero_window_probe_time = start_time;

	// 将当前新建的套接字注册到全局定时调度器中。
	TimerManager::register_socket(this);
}

TcpSocket::~TcpSocket() {
	if (state == TcpState::LISTEN) {
		NetworkEngine::unregister_listen_socket(local_addr.port);
	} else if (state != TcpState::CLOSED) {
		NetworkEngine::unregister_established_socket(local_addr.port, remote_addr.port);
	}

	// 从定时调度器中注销，防止执行任何后续的超时事件。
	TimerManager::unregister_socket(this);

	if (csv_log.is_open()) {
		csv_log.close();
	}
}

void TcpSocket::write_log(const std::string& event) {
	if (!csv_log.is_open()) {
		// 根据本地绑定的端口号创建独立的性能分析 CSV 日志文件，默认输出至 test_results 目录。
		std::string filename = "test_results/metrics_" + std::to_string(local_addr.port) + ".csv";
		csv_log.open(filename, std::ios::app);
		if (!csv_log.is_open()) {
			csv_log.open("metrics_" + std::to_string(local_addr.port) + ".csv", std::ios::app);
		}
		if (!csv_log.is_open())
			return;

		csv_log.seekp(0, std::ios::end);
		if (csv_log.tellp() == 0) {
			// 在全新创建的文件头部写入 CSV 各列字段名称。
			csv_log << "Timestamp,TcpState,cwnd,ssthresh,rwnd,seq_num,ack_num,Event\n";
		}
	}

	auto now = std::chrono::steady_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time).count();

	// 输出当前的时间戳、状态机阶段、拥塞控制指标及滑动窗口实时数据。
	csv_log << elapsed << ","
			<< state_to_string(state) << ","
			<< static_cast<int>(cwnd) << ","
			<< ssthresh << ","
			<< peer_rwnd << ","
			<< snd_nxt << ","
			<< rcv_nxt << ","
			<< event << "\n";
	csv_log.flush();
}

int TcpSocket::bind(LiteSockAddr bind_addr) {
	std::lock_guard<std::mutex> lock(socket_mutex);
	local_addr = bind_addr;
	return 0;
}

int TcpSocket::listen() {
	std::lock_guard<std::mutex> lock(socket_mutex);
	state = TcpState::LISTEN;
	// 注册本地端口至网络接收引擎，启动对 SYN 握手请求包的路由监听。
	NetworkEngine::register_listen_socket(local_addr.port, this);
	write_log();
	return 0;
}

TcpSocket* TcpSocket::accept() {
	std::unique_lock<std::mutex> lock(socket_mutex);
	// 阻塞等待，直到全连接队列不为空（有握手成功的子连接）或者套接字已被关闭。
	recv_cv.wait(lock, [this]() {
		return !completed_queue.empty() || state == TcpState::CLOSED;
	});

	if (state == TcpState::CLOSED || completed_queue.empty()) {
		return nullptr;
	}

	TcpSocket* new_conn = completed_queue.front();
	completed_queue.pop();
	return new_conn;
}

int TcpSocket::connect(LiteSockAddr target_addr) {
	{
		std::lock_guard<std::mutex> lock(socket_mutex);
		remote_addr = target_addr;

		// 若未显式绑定本地端口，则在 30000~60000 范围内随机挑选一个端口进行临时绑定。
		if (local_addr.port == 0) {
			local_addr.ip = inet_addr("127.0.0.1");
			std::random_device rd;
			std::mt19937 gen(rd());
			std::uniform_int_distribution<> distr(30000, 60000);
			local_addr.port = distr(gen);
		}

		std::random_device rd;
		std::mt19937 gen(rd());
		std::uniform_int_distribution<uint32_t> distr(1000, 1000000);
		uint32_t isn = distr(gen);
		snd_una = isn;
		snd_nxt = isn;
		last_ack_received = isn;
		state = TcpState::SYN_SENT;

		// 提前在网络引擎中建立四元组路由，以便接收对端回复的 SYN+ACK 包。
		NetworkEngine::register_established_socket(local_addr.port, remote_addr.port, this);
		write_log("connect_start");

		// 发送第一个握手 SYN 包并启动重传定时器。
		send_control_packet(TCP_FLAG_SYN);
	}

	// 挂起调用线程以阻塞等待握手确认（设置 10 秒超时门限防止挂死）。
	std::unique_lock<std::mutex> lock(socket_mutex);
	bool success = conn_cv.wait_for(lock, std::chrono::seconds(10), [this]() {
		return state == TcpState::ESTABLISHED || state == TcpState::CLOSED;
	});

	if (success && state == TcpState::ESTABLISHED) {
		return 0;
	}

	// 三次握手超时未果，清理已占用的路由表项并将套接字重置回关闭状态。
	state = TcpState::CLOSED;
	NetworkEngine::unregister_established_socket(local_addr.port, remote_addr.port);
	write_log("connect_timeout");
	return -1;
}

int TcpSocket::send(const void* buffer, int len) {
	const uint8_t* byte_buf = reinterpret_cast<const uint8_t*>(buffer);
	int bytes_sent = 0;

	// 分块发送直到全部用户数据发送完毕。
	while (bytes_sent < len) {
		std::vector<uint8_t> serialized;

		{
			std::unique_lock<std::mutex> lock(socket_mutex);

			send_waiting_threads++;
			update_timer_locked();

			// 当拥塞窗口或接收滑动窗口已满时挂起线程，直到有确认的 ACK 带来窗口滑动的通知。
			send_cv.wait(lock, [this]() {
				uint32_t curr_wnd = std::min(static_cast<uint32_t>(cwnd), peer_rwnd);
				if (peer_rwnd > 0 && curr_wnd < MSS) {
					curr_wnd = MSS;	 // 零窗口探测防止死锁保护。
				}
				uint32_t flight_size = snd_nxt - snd_una;
				return (peer_rwnd > 0 && flight_size < curr_wnd) || state != TcpState::ESTABLISHED;
			});

			send_waiting_threads--;
			update_timer_locked();

			if (state != TcpState::ESTABLISHED) {
				return -1;
			}

			uint32_t flight_size = snd_nxt - snd_una;
			uint32_t curr_wnd = std::min(static_cast<uint32_t>(cwnd), peer_rwnd);
			if (peer_rwnd > 0 && curr_wnd < MSS) {
				curr_wnd = MSS;
			}
			uint32_t allowed_to_send = curr_wnd - flight_size;

			// 计算本次发送段的大小，受发送窗口配额、剩余长度与 MSS 三者限制。
			int chunk_size = std::min({static_cast<int>(allowed_to_send), len - bytes_sent, static_cast<int>(MSS)});
			if (chunk_size <= 0) {
				continue;
			}

			std::vector<uint8_t> payload(byte_buf + bytes_sent, byte_buf + bytes_sent + chunk_size);
			TcpPacket pkt(local_addr.port, remote_addr.port, snd_nxt, rcv_nxt, TCP_FLAG_ACK,
						  get_advertised_window(), payload);

			serialized = pkt.serialize();

			// 缓存当前的包至已发送包列表中，供超时重传使用。
			SentPacket sent_pkt;
			sent_pkt.seq = snd_nxt;
			sent_pkt.data = serialized;
			sent_pkt.len = chunk_size;
			sent_pkt.retransmit_count = 0;
			sent_pkt.send_time = std::chrono::steady_clock::now();
			
			if (sent_packets.empty()) {
				rto_timer_start = sent_pkt.send_time;
			}
			sent_packets.push_back(sent_pkt);

			// 更新已发送的序列号前缘。
			snd_nxt += chunk_size;
			bytes_sent += chunk_size;

			write_log("data_sent");
			update_timer_locked();
		}

		// 在持有锁临界区外部执行网络包发送，减轻锁冲突。
		NetworkEngine::send_packet(serialized);
	}

	return bytes_sent;
}

int TcpSocket::recv(void* buffer, int len) {
	std::unique_lock<std::mutex> lock(socket_mutex);

	// 阻塞等待直到环形接收缓冲区有可用数据，或者连接已断开、收到 FIN 进入关闭处理流程。
	recv_cv.wait(lock, [this]() {
		return !recv_buf.empty() || state == TcpState::CLOSE_WAIT ||
			   state == TcpState::LAST_ACK || state == TcpState::CLOSED;
	});

	if (recv_buf.empty()) {
		// 缓冲区为空且状态变为被动关闭或关闭，返回 0 代表正常收到对端发送的 EOF 标志。
		if (state == TcpState::CLOSE_WAIT || state == TcpState::LAST_ACK || state == TcpState::CLOSED) {
			return 0;
		}
		return -1;
	}

	size_t before_free = recv_buf.free_space();

	// 从环形接收缓冲区拷贝数据并消费。
	int read_len = recv_buf.read(reinterpret_cast<uint8_t*>(buffer), len);

	size_t after_free = recv_buf.free_space();

	// 如果读出数据使接收窗口产生显著释放，主动发送 ACK 以向发送端更新通告窗口。
	if (read_len > 0 && (state == TcpState::ESTABLISHED || state == TcpState::FIN_WAIT_1 || state == TcpState::FIN_WAIT_2)) {
		size_t cap = MAX_RECV_BUF_SIZE;
		if (before_free < 2 * MSS && (after_free - before_free >= MSS || after_free >= cap / 2)) {
			send_control_packet(TCP_FLAG_ACK);
			write_log("window_update_sent");
		}
	}

	write_log("data_recv");
	return read_len;
}

int TcpSocket::close() {
	std::unique_lock<std::mutex> lock(socket_mutex);

	// 如果发送队列中还有尚未被确认的包，必须阻塞等待它们全部收到 ACK 后再开始挥手。
	close_cv.wait(lock, [this]() {
		return sent_packets.empty() || state != TcpState::ESTABLISHED;
	});

	if (state == TcpState::ESTABLISHED) {
		// 主动关闭逻辑：迁移至 FIN_WAIT_1 并向对端发送 FIN。
		state = TcpState::FIN_WAIT_1;
		write_log("close_active_fin_1");
		send_control_packet(TCP_FLAG_FIN);

		// 阻塞直到本端发出的 FIN 确认且连接完全关闭，或者进入 TIME_WAIT 状态（最长等待 10 秒）。
		close_cv.wait_for(lock, std::chrono::seconds(10), [this]() {
			return state == TcpState::CLOSED || state == TcpState::TIME_WAIT;
		});
	} else if (state == TcpState::CLOSE_WAIT) {
		// 被动关闭逻辑：迁移至 LAST_ACK 并发送本端 FIN 包。
		state = TcpState::LAST_ACK;
		write_log("close_passive_last_ack");
		send_control_packet(TCP_FLAG_FIN);

		// 等待对端最后的 ACK 确认。
		close_cv.wait_for(lock, std::chrono::seconds(10), [this]() {
			return state == TcpState::CLOSED;
		});
	}

	return 0;
}

void TcpSocket::send_control_packet(uint8_t flags) {
	// 组装无数据载荷的纯控制报文（SYN, ACK, FIN 或其组合）。
	TcpPacket pkt(local_addr.port, remote_addr.port, snd_nxt, rcv_nxt, flags,
				  get_advertised_window(), {});

	std::vector<uint8_t> serialized = pkt.serialize();

	// SYN 或 FIN 包虽然不带载荷，但在逻辑上占用 1 个序列号，需加入发送队列用以超时重传。
	if ((flags & TCP_FLAG_SYN) || (flags & TCP_FLAG_FIN)) {
		SentPacket sent_pkt;
		sent_pkt.seq = snd_nxt;
		sent_pkt.data = serialized;
		sent_pkt.len = 1;
		sent_pkt.retransmit_count = 0;
		sent_pkt.send_time = std::chrono::steady_clock::now();
		sent_packets.push_back(sent_pkt);

		snd_nxt += 1;
	}

	NetworkEngine::send_packet(serialized);
	update_timer_locked();
}

uint16_t TcpSocket::get_advertised_window() const {
	size_t free_space = MAX_RECV_BUF_SIZE - recv_buf.size();
	return static_cast<uint16_t>(std::min(free_space, static_cast<size_t>(65535)));
}
