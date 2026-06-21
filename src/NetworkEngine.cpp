#include "NetworkEngine.h"

#include <arpa/inet.h>
#include <litetcp/TcpPacket.h>
#include <litetcp/TcpSocket.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>

#include "TimerManager.h"

// 静态成员变量初始化，定义引擎套接字句柄、端口及收包线程。
int NetworkEngine::backend_udp_fd = -1;
uint16_t NetworkEngine::local_port = 0;
uint16_t NetworkEngine::dest_port = 0;
std::thread NetworkEngine::recv_thread;
std::atomic<bool> NetworkEngine::running(false);
std::mutex NetworkEngine::socks_mutex;

TcpSocket* NetworkEngine::listen_sock = nullptr;
TcpSocket* NetworkEngine::established_sock = nullptr;

bool NetworkEngine::initialize(uint16_t local_udp, uint16_t dest_udp) {
	if (running)
		return true;

	local_port = local_udp;
	dest_port = dest_udp;

	// 创建底层数据报套接字（UDP）。
	backend_udp_fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (backend_udp_fd < 0) {
		std::cerr << "[NetworkEngine] Failed to create UDP socket" << std::endl;
		return false;
	}

	// 允许快速复用本地地址端口，防止程序重启时端口处于 TIME_WAIT 导致绑定失败。
	int optval = 1;
	setsockopt(backend_udp_fd, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&optval), sizeof(optval));

	sockaddr_in local_addr{};
	std::memset(&local_addr, 0, sizeof(local_addr));
	local_addr.sin_family = AF_INET;
	local_addr.sin_port = htons(local_port);
	local_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

	// 绑定底层 UDP 端口，绑定成功后启动接收循环线程及定时器管理器。
	if (bind(backend_udp_fd, reinterpret_cast<struct sockaddr*>(&local_addr), sizeof(local_addr)) < 0) {
		std::cerr << "[NetworkEngine] Failed to bind UDP socket to port " << local_port << std::endl;
		close(backend_udp_fd);
		backend_udp_fd = -1;
		return false;
	}

	running = true;
	TimerManager::initialize();
	recv_thread = std::thread(receive_loop);
	return true;
}

void NetworkEngine::shutdown() {
	if (!running)
		return;

	running = false;
	TimerManager::shutdown();

	// 向引擎自己监听的本地 UDP 端口发送一个空的 dummy 字节包，以强行唤醒可能阻塞在 recvfrom 系统调用处的收包线程。
	if (backend_udp_fd != -1) {
		sockaddr_in self_addr{};
		std::memset(&self_addr, 0, sizeof(self_addr));
		self_addr.sin_family = AF_INET;
		self_addr.sin_port = htons(local_port);
		self_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

		char dummy = 0;
		sendto(backend_udp_fd, &dummy, 1, 0, reinterpret_cast<struct sockaddr*>(&self_addr), sizeof(self_addr));
	}

	// 等待接收线程安全退出并关闭底层套接字。
	if (recv_thread.joinable()) {
		recv_thread.join();
	}

	if (backend_udp_fd != -1) {
		close(backend_udp_fd);
		backend_udp_fd = -1;
	}
}

bool NetworkEngine::send_packet(const std::vector<uint8_t>& pkt_data) {
	if (backend_udp_fd < 0)
		return false;

	sockaddr_in send_addr{};
	std::memset(&send_addr, 0, sizeof(send_addr));
	send_addr.sin_family = AF_INET;
	send_addr.sin_port = htons(dest_port);
	send_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

	// 将用户态封装好的 LiteTCP 字节报文通过底层 UDP 发送至代理端口。
	int rst = sendto(backend_udp_fd, reinterpret_cast<const char*>(pkt_data.data()), pkt_data.size(), 0,
					 reinterpret_cast<struct sockaddr*>(&send_addr), sizeof(send_addr));
	return rst >= 0;
}

void NetworkEngine::receive_loop() {
	std::vector<uint8_t> buffer(2048);
	sockaddr_in from_addr{};
	socklen_t from_len = sizeof(from_addr);

	// 持续接收数据，提取出源与目的端口，并对反序列化成功的合法数据包进行路由匹配。
	while (running) {
		int n = recvfrom(backend_udp_fd, reinterpret_cast<char*>(buffer.data()), buffer.size(), 0,
						 reinterpret_cast<struct sockaddr*>(&from_addr), &from_len);
		if (n <= 0) {
			if (!running)
				break;
			continue;
		}

		TcpPacket packet;
		if (TcpPacket::deserialize(buffer.data(), n, packet)) {
			uint16_t local_tcp_port = packet.header.destination_port;
			uint16_t remote_tcp_port = packet.header.source_port;

			TcpSocket* target_sock = nullptr;

			{
				std::lock_guard<std::mutex> lock(socks_mutex);

				// 优先在路由表中匹配已建立连接的套接字，其次匹配处于监听状态的套接字。
				if (established_sock) {
					target_sock = established_sock;
				} else if (listen_sock) {
					target_sock = listen_sock;
				}
			}

			// 成功路由后，将数据包递交给对应的套接字逻辑处理。
			if (target_sock) {
				target_sock->handle_packet(packet);
			}
		}
	}
}

void NetworkEngine::register_listen_socket(uint16_t port, TcpSocket* sock) {
	std::lock_guard<std::mutex> lock(socks_mutex);
	listen_sock = sock;
}

void NetworkEngine::unregister_listen_socket(uint16_t port) {
	std::lock_guard<std::mutex> lock(socks_mutex);
	listen_sock = nullptr;
}

void NetworkEngine::register_established_socket(uint16_t local_port, uint16_t remote_port, TcpSocket* sock) {
	std::lock_guard<std::mutex> lock(socks_mutex);
	established_sock = sock;
}

void NetworkEngine::unregister_established_socket(uint16_t local_port, uint16_t remote_port) {
	std::lock_guard<std::mutex> lock(socks_mutex);
	established_sock = nullptr;
}
