#include <arpa/inet.h>
#include <litetcp/TcpPacket.h>
#include <litetcp/TcpSocket.h>

#include <algorithm>
#include <iostream>
#include <random>

#include "NetworkEngine.h"

constexpr size_t MAX_RECV_BUF_SIZE = 65536;
constexpr uint32_t MSS = 1400 - sizeof(TcpHeader);

void TcpSocket::handle_packet(const TcpPacket& packet) {
	std::lock_guard<std::mutex> lock(socket_mutex);

	switch (state) {
		case TcpState::LISTEN:
		case TcpState::SYN_SENT:
		case TcpState::SYN_RECV:
			handle_handshake_packet(packet);
			break;
		case TcpState::ESTABLISHED:
		case TcpState::FIN_WAIT_1:
		case TcpState::FIN_WAIT_2:
		case TcpState::CLOSE_WAIT:
		case TcpState::CLOSING:
		case TcpState::LAST_ACK:
			handle_established_or_close_packet(packet);
			break;
		default:
			break;
	}

	update_timer_locked();
}

void TcpSocket::handle_handshake_packet(const TcpPacket& packet) {
	uint8_t flags = packet.header.flags;
	uint32_t seq = packet.header.seq_num;
	uint32_t ack = packet.header.ack_num;

	// 1. LISTEN 状态处理。收到客户端 SYN 握手请求后，创建一个子套接字并初始化状态为 SYN_RECV，随后在网络引擎中建立四元组路由并发送 SYN+ACK。
	if (state == TcpState::LISTEN) {
		if ((flags & TCP_FLAG_SYN) && !(flags & TCP_FLAG_ACK)) {
			TcpSocket* child = new TcpSocket();
			child->parent = this;
			child->local_addr = this->local_addr;
			child->remote_addr.ip = inet_addr("127.0.0.1");
			child->remote_addr.port = packet.header.source_port;
			child->rcv_nxt = seq + 1;
			std::random_device rd;
			std::mt19937 gen(rd());
			std::uniform_int_distribution<uint32_t> distr(1000, 1000000);
			uint32_t isn = distr(gen);
			child->snd_una = isn;
			child->snd_nxt = isn;
			child->last_ack_received = isn;
			child->state = TcpState::SYN_RECV;

			NetworkEngine::register_established_socket(child->local_addr.port, child->remote_addr.port, child);
			child->write_log("accept_syn_recv");

			child->send_control_packet(TCP_FLAG_SYN | TCP_FLAG_ACK);
		}
		return;
	}

	// 2. SYN_SENT 状态处理。客户端收到服务端的 SYN+ACK 后，确认无误则清空重传包，更新发送窗口并将自身迁移至 ESTABLISHED 状态，随后发送 ACK 并唤醒 connect 等待线程。
	if (state == TcpState::SYN_SENT) {
		if ((flags & (TCP_FLAG_SYN | TCP_FLAG_ACK)) == (TCP_FLAG_SYN | TCP_FLAG_ACK)) {
			if (ack == snd_una + 1) {
				sent_packets.clear();
				snd_una = ack;
				rcv_nxt = seq + 1;
				state = TcpState::ESTABLISHED;
				write_log("connect_established");

				send_control_packet(TCP_FLAG_ACK);
				conn_cv.notify_all();
			}
		}
		return;
	}

	// 3. SYN_RECV 状态处理。服务端收到客户端对 SYN+ACK 的 ACK 确认包后，清空重传队列并将状态迁移至 ESTABLISHED，最后将子套接字推入监听套接字的全连接队列中并唤醒 accept 线程。
	if (state == TcpState::SYN_RECV) {
		if ((flags & TCP_FLAG_ACK) && !(flags & TCP_FLAG_SYN)) {
			if (ack == snd_una + 1) {
				sent_packets.clear();
				snd_una = ack;
				state = TcpState::ESTABLISHED;
				write_log("accept_established");

				if (parent) {
					std::lock_guard<std::mutex> p_lock(parent->socket_mutex);
					parent->completed_queue.push(this);
					parent->recv_cv.notify_all();
				}
			}
		}
		return;
	}
}

void TcpSocket::handle_established_or_close_packet(const TcpPacket& packet) {
	size_t payload_len = packet.data.size();

	// === 步骤 A: 处理 ACK 确认以驱动发送窗口滑动与拥塞窗口更新 ===
	process_ack(packet, payload_len);

	// === 步骤 B: 处理接收到的数据载荷，并处理乱序和连续包重组转正 ===
	process_payload(packet, payload_len);

	// === 步骤 C: 处理 FIN 挥手标志以驱动状态机进入关闭流程 ===
	process_fin(packet);

	// === 步骤 D: 纯 ACK 包驱动的挥手阶段状态迁移 ===
	process_state_cleanup(packet);
}

void TcpSocket::process_ack(const TcpPacket& packet, size_t payload_len) {
	uint8_t flags = packet.header.flags;
	uint32_t ack = packet.header.ack_num;

	if (flags & TCP_FLAG_ACK) {
		uint32_t old_rwnd = peer_rwnd;
		peer_rwnd = packet.header.advertised_window;
		if (peer_rwnd > 0 && old_rwnd == 0) {
			send_cv.notify_all();
		}

		if (ack > snd_una) {
			// 收到新的确认号，更新发送窗口左边缘。
			snd_una = ack;
			rto_pending = false;

			// 计算精确往返时间（RTT）与超时阈值（RTO），根据 Karn 算法排除包含重传的报文。
			auto now = std::chrono::steady_clock::now();
			rto_timer_start = now;
			double sample_rtt = -1.0;
			bool has_retransmitted = false;
			for (const auto& p : sent_packets) {
				if (p.seq + p.len <= ack) {
					if (p.retransmit_count > 0) {
						has_retransmitted = true;
						break;
					}
				}
			}

			if (!has_retransmitted) {
				for (const auto& p : sent_packets) {
					if (p.seq + p.len <= ack) {
						auto duration = std::chrono::duration_cast<std::chrono::microseconds>(now - p.send_time).count();
						sample_rtt = duration / 1000000.0;
						break;
					}
				}
			}

			if (sample_rtt > 0.0) {
				estimated_rtt = estimated_rtt + 0.125 * (sample_rtt - estimated_rtt);
				dev_rtt = dev_rtt + 0.25 * (std::abs(sample_rtt - estimated_rtt) - dev_rtt);
				rto = estimated_rtt + 4 * dev_rtt;
				rto = std::max(0.02, std::min(rto, 5.0));  // 保证 RTO 在 20ms 到 5s 的安全区间内。
			}

			// 清理已被确认的数据包缓存。
			sent_packets.erase(
				std::remove_if(sent_packets.begin(), sent_packets.end(),
							   [ack](const SentPacket& p) {
								   return p.seq + p.len <= ack;
							   }),
				sent_packets.end());

			// 基于 TCP Reno 算法的拥塞控制更新逻辑，分慢启动、拥塞避免和快速恢复三种情况进行 cwnd 的增减。
			if (congestion_state == 0) {
				cwnd += MSS;  // 慢启动：每收到 1 个新 ACK，拥塞窗口 cwnd 增加 1 MSS。
				if (cwnd >= ssthresh) {
					congestion_state = 1;
				}
			} else if (congestion_state == 1) {
				cwnd += (static_cast<double>(MSS) * MSS) / cwnd;  // 拥塞避免：每个 RTT 周期内，cwnd 大约增加 1 MSS。
			} else if (congestion_state == 2) {
				if (ack >= recover) {
					// 收到完全确认（Full ACK），退出快速恢复阶段，回到拥塞避免。
					congestion_state = 1;
					cwnd = static_cast<double>(ssthresh);
				} else {
					// 收到部分确认（Partial ACK），触发 NewReno 重传未确认的最老报文，并按规范扣减拥塞窗口。
					if (!sent_packets.empty()) {
						retransmit_packet(sent_packets.front());
					}

					uint32_t confirmed_bytes = ack - last_ack_received;
					cwnd = cwnd - confirmed_bytes + MSS;

					// 避免大块确认使 cwnd 变为负数或极小值。
					if (cwnd < ssthresh) {
						cwnd = ssthresh;
					}

					congestion_state = 2;
				}
			}

			dup_ack_count = 0;
			last_ack_received = ack;

			// 唤醒可能阻塞在 send 或 close 的线程。
			send_cv.notify_all();
			close_cv.notify_all();
			write_log("ack_received");
		} else if (ack == snd_una) {
			// 收到重复的确认号，仅在有未确认数据且该包是无载荷纯控制包时进行冗余 ACK 计数。
			if (!sent_packets.empty() && payload_len == 0 && !rto_pending && peer_rwnd == old_rwnd) {
				dup_ack_count++;
				write_log("dup_ack");

				// 触发快速重传与快速恢复。
				if (congestion_state == 2) {
					cwnd += MSS;  // 快速恢复阶段继续收到重复 ACK，临时膨胀 cwnd 并尝试发送新数据。
					send_cv.notify_all();
				} else {
					if (dup_ack_count == 3) {
						congestion_state = 2;
						recover = snd_nxt;	// 记录进入快速恢复时的边界序列号。
						ssthresh = std::max(static_cast<uint32_t>(cwnd / 2), 2 * MSS);
						cwnd = ssthresh + 3 * MSS;

						if (!sent_packets.empty()) {
							retransmit_packet(sent_packets.front());
						}
						write_log("fast_retransmit");
					} else if (dup_ack_count > 3) {
						cwnd += MSS;  // 快速恢复阶段继续收到重复 ACK，临时膨胀 cwnd 并尝试发送新数据。
						send_cv.notify_all();
					}
				}
			}
		}
	}
}

void TcpSocket::process_payload(const TcpPacket& packet, size_t payload_len) {
	uint32_t seq = packet.header.seq_num;

	if (payload_len > 0) {
		uint32_t start_seq = seq;
		uint32_t end_seq = seq + payload_len;

		// 去重：截断已经确认过的重叠历史数据。
		if (start_seq < rcv_nxt) {
			start_seq = rcv_nxt;
		}

		if (start_seq < end_seq) {
			uint32_t offset = start_seq - seq;
			uint32_t new_len = end_seq - start_seq;

			// 当接收缓冲区容量足够时，将去重后的数据按照相对接收指针 rcv_nxt 的偏移量原地落盘写入环形队列。
			uint32_t buffer_offset = start_seq - rcv_nxt;
			if (buffer_offset + new_len >= buffer_offset &&
				buffer_offset + new_len <= recv_buf.free_space()) {
				recv_buf.write_at(buffer_offset, packet.data.data() + offset, new_len);

				// 在乱序表记录当前落盘的区间。
				add_interval(start_seq, end_seq);

				if (start_seq > rcv_nxt) {
					write_log("data_out_of_order");
				}
			}
		}

		// 检查乱序区间队列头部是否与当前等待的接收序列号重合。若重合则表示有连续数据转正，推进接收窗口并唤醒 recv 调用。
		if (ooo_count > 0 && ooo_intervals[0].first == rcv_nxt) {
			uint32_t consecutive_len = ooo_intervals[0].second - ooo_intervals[0].first;

			recv_buf.advance_tail(consecutive_len);
			rcv_nxt = ooo_intervals[0].second;

			for (size_t i = 1; i < ooo_count; i++) {
				ooo_intervals[i - 1] = ooo_intervals[i];
			}
			ooo_count--;

			recv_cv.notify_all();
			write_log("data_ordered");
		}

		// 只要接收了载荷数据，不管是有序还是乱序，都必须回复 ACK。
		send_control_packet(TCP_FLAG_ACK);
	}
}

void TcpSocket::process_fin(const TcpPacket& packet) {
	uint8_t flags = packet.header.flags;
	uint32_t seq = packet.header.seq_num;
	uint32_t ack = packet.header.ack_num;

	if (flags & TCP_FLAG_FIN) {
		rcv_nxt = seq + 1;	// FIN 报文段逻辑上占 1 字节序列号。
		send_control_packet(TCP_FLAG_ACK);

		if (state == TcpState::ESTABLISHED) {
			state = TcpState::CLOSE_WAIT;
			write_log("state_close_wait");
			recv_cv.notify_all();
		} else if (state == TcpState::FIN_WAIT_1) {
			if (ack >= snd_nxt) {
				// 己方 FIN 已经被 ACK 且收到对方 FIN，直接进入 TIME_WAIT。
				state = TcpState::TIME_WAIT;
				write_log("state_time_wait");
				last_zero_window_probe_time = std::chrono::steady_clock::now();
			} else {
				// 收到对端 FIN 但对方还未确认己方的 FIN，进入 CLOSING。
				state = TcpState::CLOSING;
				write_log("state_closing");
			}
		} else if (state == TcpState::FIN_WAIT_2) {
			state = TcpState::TIME_WAIT;
			write_log("state_time_wait");
			last_zero_window_probe_time = std::chrono::steady_clock::now();
		}
		close_cv.notify_all();
	}
}

void TcpSocket::process_state_cleanup(const TcpPacket& packet) {
	uint8_t flags = packet.header.flags;
	uint32_t ack = packet.header.ack_num;

	if (!(flags & TCP_FLAG_FIN) && (flags & TCP_FLAG_ACK)) {
		if (state == TcpState::FIN_WAIT_1) {
			if (ack >= snd_nxt) {
				state = TcpState::FIN_WAIT_2;
				write_log("state_fin_wait_2");
				close_cv.notify_all();
			}
		} else if (state == TcpState::CLOSING) {
			if (ack >= snd_nxt) {
				state = TcpState::TIME_WAIT;
				write_log("state_time_wait");
				last_zero_window_probe_time = std::chrono::steady_clock::now();
				close_cv.notify_all();
			}
		} else if (state == TcpState::LAST_ACK) {
			if (ack >= snd_nxt) {
				state = TcpState::CLOSED;
				write_log("state_closed");
				NetworkEngine::unregister_established_socket(local_addr.port, remote_addr.port);
				close_cv.notify_all();
			}
		}
	}
}

void TcpSocket::add_interval(uint32_t start, uint32_t end) {
	if (start >= end)
		return;

	// 1. 寻找插入位置以保证区间列表严格按 start 起始号升序排列。
	size_t ins_pos = 0;
	while (ins_pos < ooo_count && ooo_intervals[ins_pos].first < start) {
		ins_pos++;
	}

	// 2. 尝试与前一个乱序区间进行合并，避免重叠。
	if (ins_pos > 0 && ooo_intervals[ins_pos - 1].second >= start) {
		ooo_intervals[ins_pos - 1].second = std::max(ooo_intervals[ins_pos - 1].second, end);
		ins_pos--;
	} else {
		// 无法与前一个区间合并则必须作为独立新条目插入。若超出限制大小则丢弃。
		if (ooo_count >= MAX_OOO_INTERVALS) {
			return;
		}
		for (size_t i = ooo_count; i > ins_pos; i--) {
			ooo_intervals[i] = ooo_intervals[i - 1];
		}
		ooo_intervals[ins_pos] = {start, end};
		ooo_count++;
	}

	// 3. 从插入位置向后遍历，消除并合并所有存在重叠的乱序区间。
	size_t next_pos = ins_pos + 1;
	while (next_pos < ooo_count && ooo_intervals[ins_pos].second >= ooo_intervals[next_pos].first) {
		ooo_intervals[ins_pos].second = std::max(ooo_intervals[ins_pos].second, ooo_intervals[next_pos].second);
		next_pos++;
	}

	// 统一向前移动数据以清理因合并被删除的多余条目。
	size_t removed_count = next_pos - (ins_pos + 1);
	if (removed_count > 0) {
		for (size_t i = ins_pos + 1; i + removed_count < ooo_count; i++) {
			ooo_intervals[i] = ooo_intervals[i + removed_count];
		}
		ooo_count -= removed_count;
	}
}
