#include <litetcp/TcpPacket.h>
#include <litetcp/TcpSocket.h>

#include <algorithm>
#include <iostream>
#include <thread>

#include "NetworkEngine.h"
#include "TimerManager.h"

constexpr size_t MAX_RECV_BUF_SIZE = 64800;
constexpr uint32_t MSS = 1400 - sizeof(TcpHeader);

void TcpSocket::retransmit_packet(SentPacket& pkt) {
	// 记录重传时刻，累加重传次数，并直接通过收发引擎重发数据包。
	pkt.send_time = std::chrono::steady_clock::now();
	pkt.retransmit_count++;
	NetworkEngine::send_packet(pkt.data);

	rto_timer_start = pkt.send_time;
}

void TcpSocket::update_timer_locked() {
	if (state == TcpState::CLOSED) {
		active_event_id++;
		return;
	}

	auto now = std::chrono::steady_clock::now();
	std::chrono::steady_clock::time_point min_expire = std::chrono::steady_clock::time_point::max();
	bool has_timeout = false;

	// 1. TIME_WAIT 状态超时。设置 2 秒的持续时长（2 * MSL 的简化表示），到期后将彻底销毁该连接。
	if (state == TcpState::TIME_WAIT) {
		auto tw_expire = last_zero_window_probe_time + std::chrono::milliseconds(2000);
		if (tw_expire < min_expire) {
			min_expire = tw_expire;
		}
		has_timeout = true;
	} else {
		// 2. 超时重传（RTO）计时。基于发送队列中最老的包的发送时刻加上当前算得的 RTO 阈值计算。
		if (!sent_packets.empty()) {
			auto rto_duration = std::chrono::duration_cast<std::chrono::steady_clock::duration>(
				std::chrono::duration<double>(rto));
			auto rto_expire = rto_timer_start + rto_duration;
			if (rto_expire < min_expire) {
				min_expire = rto_expire;
			}
			has_timeout = true;
		}

		// 3. 零窗口探测计时。若对端通告窗口为 0 且本端有数据待发，则每隔 1 秒调度一次零窗口探测事件。
		if (peer_rwnd == 0 && send_waiting_threads > 0) {
			auto probe_expire = last_zero_window_probe_time + std::chrono::milliseconds(1000);
			if (probe_expire < min_expire) {
				min_expire = probe_expire;
			}
			has_timeout = true;
		}
	}

	// 递增 active_event_id，使堆中关于当前 socket 的历史到期事件在弹出时直接被惰性删除。
	active_event_id++;

	if (has_timeout) {
		TimerManager::add_timer(min_expire, this, active_event_id);
	}
}

void TcpSocket::handle_timeout() {
	if (state == TcpState::CLOSED) {
		return;
	}

	auto now = std::chrono::steady_clock::now();

	// 1. 处理 TIME_WAIT 状态超时释放。
	if (state == TcpState::TIME_WAIT) {
		double elapsed_tw = std::chrono::duration<double>(now - last_zero_window_probe_time).count();
		if (elapsed_tw >= 2.0) {
			state = TcpState::CLOSED;
			write_log("time_wait_timeout");
			NetworkEngine::unregister_established_socket(local_addr.port, remote_addr.port);
			close_cv.notify_all();

			update_timer_locked();
			return;
		}
	}

	// 2. 处理 RTO 超时重传。触发重传后，对重传间隔 RTO 执行指数退避（最大退避至 5 秒），同时将慢启动阈值减半并把拥塞窗口 cwnd 重置为 1 MSS 回退至慢启动状态。
	if (!sent_packets.empty()) {
		auto& oldest = sent_packets.front();
		double elapsed = std::chrono::duration<double>(now - rto_timer_start).count();
		if (elapsed >= rto) {
			// 检测并限制最大重传次数，超过次数则直接关闭连接并释放资源。
			int max_retrans = (state == TcpState::SYN_RECV || state == TcpState::SYN_SENT) ? 5 : 15;
			if (oldest.retransmit_count >= max_retrans) {
				if (state == TcpState::SYN_RECV) {
					state = TcpState::CLOSED;
					write_log("syn_recv_timeout_abandoned");
					NetworkEngine::unregister_established_socket(local_addr.port, remote_addr.port);
					std::thread([this]() { delete this; }).detach();
					return;
				} else if (state == TcpState::SYN_SENT) {
					state = TcpState::CLOSED;
					write_log("connect_failed_timeout");
					NetworkEngine::unregister_established_socket(local_addr.port, remote_addr.port);
					conn_cv.notify_all();
					return;
				} else {
					state = TcpState::CLOSED;
					write_log("transmission_timeout_closed");
					NetworkEngine::unregister_established_socket(local_addr.port, remote_addr.port);
					recv_cv.notify_all();
					send_cv.notify_all();
					close_cv.notify_all();
					return;
				}
			}

			retransmit_packet(oldest);

			rto = std::min(rto * 2.0, 5.0);
			if (!rto_pending) {
				ssthresh = std::max(static_cast<uint32_t>(cwnd / 2), 2 * MSS);
			}
			cwnd = static_cast<double>(MSS);
			congestion_state = 0;
			rto_recover = snd_nxt;
			dup_ack_count = 0;
			rto_pending = true;
			rto_timer_start = now;

			write_log("rto_timeout");
		}
	}

	// 3. 处理零窗口探测。每隔 1 秒向对端发送一个序列号为 snd_nxt - 1 的 1 字节探测包，以诱使对端回复含有最新接收窗口大小的 ACK 报文。
	if (peer_rwnd == 0 && send_waiting_threads > 0) {
		double elapsed_probe = std::chrono::duration<double>(now - last_zero_window_probe_time).count();
		if (elapsed_probe >= 1.0) {
			TcpPacket probe(local_addr.port, remote_addr.port, snd_nxt - 1, rcv_nxt, TCP_FLAG_ACK,
							get_advertised_window(), {0});
			NetworkEngine::send_packet(probe.serialize());
			last_zero_window_probe_time = now;
			write_log("zero_window_probe");
		}
	}

	// 重新评估并计算下一次定时器事件触发绝对时间点。
	update_timer_locked();
}
