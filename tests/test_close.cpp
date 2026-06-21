#include <litetcp/litetcp.h>

#include <chrono>
#include <thread>

#include "../src/NetworkEngine.h"
#include "common/TestHarness.hpp"

void run_server(int case_num) {
	NetworkEngine::initialize(20221, 20218);

	litetcp_t* server = litetcp_socket();
	litetcp_sock_addr bind_addr{inet_addr("127.0.0.1"), 1234};
	litetcp_bind(server, bind_addr);
	litetcp_listen(server);

	litetcp_t* conn = litetcp_accept(server);
	LITE_ASSERT_TRUE(conn != nullptr);

	if (case_num == 1) {
		// 主动关闭测试中的被动关闭端。挂起等待客户端主动发起关闭，检测到对方 FIN 之后判定连接已自动迁移进入 CLOSE_WAIT 状态，随后服务端也调用 close 发送 FIN 以完成挥手。
		std::this_thread::sleep_for(std::chrono::milliseconds(300));

		LITE_ASSERT_EQ(static_cast<int>(conn->get_state()), static_cast<int>(TcpState::CLOSE_WAIT));

		litetcp_close(conn);
	} else if (case_num == 2) {
		// 同时关闭测试。服务端接收到 READY 指令后，立刻发起关闭，以模拟通信两端同时发起 close 关闭连接的场景。
		char buf[32];
		int n = litetcp_recv(conn, buf, sizeof(buf));
		LITE_ASSERT_TRUE(n > 0);

		litetcp_close(conn);
	}

	litetcp_close(server);
	NetworkEngine::shutdown();
	std::cout << "[Server Case " << case_num << "] Close test passed!" << std::endl;
}

void run_client(int case_num) {
	NetworkEngine::initialize(20222, 20218);

	litetcp_t* client = litetcp_socket();
	litetcp_sock_addr target_addr{inet_addr("127.0.0.1"), 1234};

	int rst = litetcp_connect(client, target_addr);
	LITE_ASSERT_EQ(rst, 0);

	if (case_num == 1) {
		// 主动关闭端。客户端首先主动调用 close 关闭套接字，在挥手关闭过程中连接状态将依次经历 FIN_WAIT_1、FIN_WAIT_2 以及 TIME_WAIT 状态。
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		litetcp_close(client);
	} else if (case_num == 2) {
		// 同时关闭端。客户端发送 READY 告知服务端，随后不等待对方关闭便强行同时调用 close 触发双向同时关闭。
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		litetcp_send(client, "READY", 5);
		litetcp_close(client);
	}

	NetworkEngine::shutdown();
	std::cout << "[Client Case " << case_num << "] Close test passed!" << std::endl;
}

int main(int argc, char** argv) {
	if (argc < 3) {
		std::cerr << "Usage: " << argv[0] << " --server|--client <case_num>" << std::endl;
		return 1;
	}

	std::string role = argv[1];
	int case_num = std::stoi(argv[2]);
	if (role == "--server") {
		run_server(case_num);
		return 0;
	} else if (role == "--client") {
		run_client(case_num);
		return 0;
	} else {
		std::cerr << "Unknown role: " << role << std::endl;
		return 1;
	}
}
