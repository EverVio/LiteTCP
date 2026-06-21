#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

class TcpSocket;

// 底层 UDP 收发引擎，用于模拟 IP 层的数据包派发与网络中继。
class NetworkEngine {
private:
	static int backend_udp_fd;		   // 绑定的本地 UDP 套接字描述符。
	static uint16_t local_port;		   // 本地绑定的 UDP 端口号，用于接收对端发送的数据。
	static uint16_t dest_port;		   // 发送的目标 UDP 端口，即中间代理 network_proxy 的监听端口。
	static std::thread recv_thread;	   // 运行后台接收数据包事件循环的线程。
	static std::atomic<bool> running;  // 引擎运行状态的原子布尔值。
	static std::mutex socks_mutex;	   // 保护套接字路由表的互斥锁。

	static TcpSocket* listen_sock;		 // 当前进程注册的处于监听状态的套接字。
	static TcpSocket* established_sock;	 // 当前进程注册的已建立连接的套接字。

	// 后台收包线程的主循环，负责调用 recvfrom 接收 UDP 数据并解包派发给目标套接字。
	static void receive_loop();

public:
	// 初始化并启动 UDP 发收引擎，绑定本地端口 `local_udp`，设定发送目标端口 `dest_udp`，成功时返回 true。
	static bool initialize(uint16_t local_udp, uint16_t dest_udp);

	// 优雅关闭收发引擎，停止后台收包线程并关闭底层 UDP 套接字。
	static void shutdown();

	// 将序列化后的报文数据 `pkt_data` 发送给对端代理。
	static bool send_packet(const std::vector<uint8_t>& pkt_data);

	// 注册和注销当前处于监听状态的套接字路由。
	static void register_listen_socket(uint16_t port, TcpSocket* sock);
	static void unregister_listen_socket(uint16_t port);

	// 注册和注销已建立连接的套接字路由。
	static void register_established_socket(uint16_t local_port, uint16_t remote_port, TcpSocket* sock);
	static void unregister_established_socket(uint16_t local_port, uint16_t remote_port);
};
