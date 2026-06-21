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
	LITE_ASSERT_EQ(static_cast<int>(conn->get_state()), static_cast<int>(TcpState::ESTABLISHED));

	std::this_thread::sleep_for(std::chrono::milliseconds(500));
	litetcp_close(conn);
	litetcp_close(server);

	NetworkEngine::shutdown();
	std::cout << "[Server Case " << case_num << "] Connect test passed!" << std::endl;
}

void run_client(int case_num) {
	NetworkEngine::initialize(20222, 20218);

	if (case_num == 2) {
		// SYN 包丢失重传测试。客户端先通知代理将接下来收到的第一个 SYN 报文丢弃，用以模拟恶劣网络环境下触发客户端 RTO 超时并成功重传握手包的场景。
		std::cout << "[Client Case 2] Request proxy to drop next SYN..." << std::endl;
		notify_proxy("DROP_SYN");
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}

	litetcp_t* client = litetcp_socket();
	litetcp_sock_addr target_addr{inet_addr("127.0.0.1"), 1234};

	int rst = litetcp_connect(client, target_addr);
	LITE_ASSERT_EQ(rst, 0);
	LITE_ASSERT_EQ(static_cast<int>(client->get_state()), static_cast<int>(TcpState::ESTABLISHED));

	std::this_thread::sleep_for(std::chrono::milliseconds(200));
	litetcp_close(client);

	NetworkEngine::shutdown();
	std::cout << "[Client Case " << case_num << "] Connect test passed!" << std::endl;
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
