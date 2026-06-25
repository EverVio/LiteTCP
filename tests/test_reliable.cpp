#include <litetcp/litetcp.h>

#include <chrono>
#include <numeric>
#include <thread>
#include <vector>

#include "../src/NetworkEngine.h"
#include "common/TestHarness.hpp"

constexpr size_t DATA_SIZE_CASE2 = 5 * 1024 * 1024;	 // 默认 5MB 测试数据大小。

// 辅助函数：计算 8 位二进制累加校验和，用于在应用层对传输完毕的测试数据块进行一致性比对。
uint8_t calculate_checksum_8(const std::vector<uint8_t>& data) {
	uint32_t sum = 0;
	for (uint8_t b : data) {
		sum += b;
	}
	return static_cast<uint8_t>(sum & 0xFF);
}

void run_server(int case_num, size_t payload_size = 5 * 1024 * 1024) {
	NetworkEngine::initialize(20221, 20218);

	litetcp_t* server = litetcp_socket();
	litetcp_sock_addr bind_addr{inet_addr("127.0.0.1"), 1234};
	litetcp_bind(server, bind_addr);
	litetcp_listen(server);

	litetcp_t* conn = litetcp_accept(server);
	LITE_ASSERT_TRUE(conn != nullptr);

	if (case_num == 1) {
		// 用例 1：零窗口流量控制测试。服务端故意暂停调用 recv 接收数据 2 秒，以迫使发送端的滑动窗口完全耗尽并挂起，随后触发零窗口探测包流程。
		std::cout << "[Server Case 1] Intentionally pause receiving for 2 seconds..." << std::endl;
		std::this_thread::sleep_for(std::chrono::seconds(2));

		// 恢复正常接收，此时空闲空间释放，向发送端回复 ACK 以拓宽通告接收窗口，从而继续接收并消费完后续所有数据包。
		std::vector<uint8_t> buf(1024);
		int total_received = 0;
		while (true) {
			int n = litetcp_recv(conn, buf.data(), buf.size());
			if (n <= 0)
				break;
			total_received += n;
		}
		std::cout << "[Server Case 1] Successfully received total: " << total_received << " bytes" << std::endl;
		litetcp_close(conn);
	} else if (case_num == 2) {
		// 用例 2：复杂弱网乱序丢包重传测试。服务端接收指定大小的数据块，并定期向终端打印接收百分比进度，以便观察在高丢包低带宽的网络中继条件下的传输表现。
		std::vector<uint8_t> received;
		received.reserve(payload_size);

		std::vector<uint8_t> buf(4096);
		std::cout << "[Server Case 2] Start receiving " << (payload_size / 1024.0 / 1024.0) << "MB data under lossy network..." << std::endl;

		auto start = std::chrono::steady_clock::now();
		size_t last_report = 0;
		size_t report_interval = 512 * 1024;  // 每接收 512 KB 打印一次实时进度。
		bool blocked_7s = false;
		while (received.size() < payload_size) {
			// 检查是否需要触发 7 秒后的长阻塞（暂停调用 litetcp_recv 2 秒）
			auto now = std::chrono::steady_clock::now();
			double elapsed_sec = std::chrono::duration<double>(now - start).count();
			if (elapsed_sec >= 7.0 && !blocked_7s) {
				std::cout << "[Server Case 2] Intentionally pause receiving for 2 seconds (Zero Window)..." << std::endl;
				std::this_thread::sleep_for(std::chrono::seconds(2));
				blocked_7s = true;
			}

			int n = litetcp_recv(conn, buf.data(), buf.size());
			if (n <= 0) {
				break;
			}
			received.insert(received.end(), buf.begin(), buf.begin() + n);

			if (received.size() - last_report >= report_interval) {
				std::cout << "[Server Case 2] Progress: "
						  << received.size() / 1024 << " KB / "
						  << payload_size / 1024 << " KB ("
						  << (received.size() * 100 / payload_size) << "%)" << std::endl;
				last_report = received.size();
			}
		}
		auto end = std::chrono::steady_clock::now();
		double elapsed = std::chrono::duration<double>(end - start).count();

		std::cout << "[Server Case 2] Received size: " << received.size()
				  << " bytes in " << elapsed << " seconds ("
				  << (received.size() / 1024.0 / 1024.0) / elapsed << " MB/s)" << std::endl;

		LITE_ASSERT_EQ(received.size(), payload_size);

		// 对接收完的完整缓冲区计算校验和，并将其写入文件保存，以便主控脚本与客户端发出的原始数据一致性校验和进行比对断言。
		uint8_t check = calculate_checksum_8(received);
		std::cout << "[Server Case 2] Received Checksum: " << (int)check << std::endl;

		std::ofstream out("test_results/checksum_server.txt");
		if (!out.is_open()) {
			out.open("checksum_server.txt");
		}
		out << (int)check;
		out.close();

		litetcp_close(conn);
	}

	litetcp_close(server);
	NetworkEngine::shutdown();
	std::cout << "[Server Case " << case_num << "] Reliable test finished!" << std::endl;
}

void run_client(int case_num, double drop_rate = 0.05, int delay_ms = 10, size_t payload_size = 5 * 1024 * 1024) {
	NetworkEngine::initialize(20222, 20218);

	litetcp_t* client = litetcp_socket();
	litetcp_sock_addr target_addr{inet_addr("127.0.0.1"), 1234};

	int rst = litetcp_connect(client, target_addr);
	LITE_ASSERT_EQ(rst, 0);

	if (case_num == 1) {
		// 用例 1：零窗口探测触发。客户端以高速率连续发送 200KB 数据以完全堵塞对方接收缓冲区，随后阻塞睡眠 3 秒，期间由于窗口滑动为零将会触发零窗口探测流程。
		std::vector<uint8_t> data(1000, 0xAA);
		std::cout << "[Client Case 1] High-speed sending data to trigger zero-window..." << std::endl;
		for (int i = 0; i < 200; ++i) {
			litetcp_send(client, data.data(), data.size());
		}
		std::cout << "[Client Case 1] All data pushed to send queue. Waiting for RTO / Zero Window..." << std::endl;
		std::this_thread::sleep_for(std::chrono::seconds(3));
		litetcp_close(client);
	} else if (case_num == 2) {
		// 用例 2：丢包与网络延迟仿真下的可靠传输。客户端生成带有序列标志的模拟文件数据，配置中继代理的模拟丢包率及延迟时间，随后进行可靠发送并计算原校验和供服务端校对。
		std::cout << "[Client Case 2] Generating " << (payload_size / 1024.0 / 1024.0) << "MB test dataset..." << std::endl;
		std::vector<uint8_t> test_data(payload_size);
		for (size_t i = 0; i < test_data.size(); ++i) {
			test_data[i] = static_cast<uint8_t>(i % 256);
		}
		uint8_t check = calculate_checksum_8(test_data);
		std::cout << "[Client Case 2] Dataset Checksum: " << (int)check << std::endl;

		std::ofstream out("test_results/checksum_client.txt");
		if (!out.is_open()) {
			out.open("checksum_client.txt");
		}
		out << (int)check;
		out.close();

		// 通过 UDP 控制通道动态设置中继代理的丢包率和包延迟时间。
		notify_proxy("SET_DROP_RATE " + std::to_string(drop_rate));
		notify_proxy("SET_DELAY " + std::to_string(delay_ms));
		std::this_thread::sleep_for(std::chrono::milliseconds(200));

		std::cout << "[Client Case 2] Sending " << (payload_size / 1024.0 / 1024.0) << "MB dataset..." << std::endl;
		litetcp_send(client, test_data.data(), test_data.size());

		std::this_thread::sleep_for(std::chrono::seconds(5));

		litetcp_close(client);
	}

	NetworkEngine::shutdown();
	std::cout << "[Client Case " << case_num << "] Reliable test finished!" << std::endl;
}

int main(int argc, char** argv) {
	if (argc < 3) {
		std::cerr << "Usage: " << argv[0] << " --server|--client <case_num> [extra_params...]" << std::endl;
		std::cerr << "  For --server: [payload_size]" << std::endl;
		std::cerr << "  For --client: [drop_rate] [delay_ms] [payload_size]" << std::endl;
		return 1;
	}

	std::string role = argv[1];
	int case_num = std::stoi(argv[2]);
	if (role == "--server") {
		size_t payload_size = 5 * 1024 * 1024;
		if (argc >= 4) {
			payload_size = std::stoull(argv[3]);
		}
		run_server(case_num, payload_size);
		return 0;
	} else if (role == "--client") {
		double drop_rate = 0.05;
		int delay_ms = 10;
		size_t payload_size = 5 * 1024 * 1024;
		if (argc >= 4) {
			drop_rate = std::stod(argv[3]);
		}
		if (argc >= 5) {
			delay_ms = std::stoi(argv[4]);
		}
		if (argc >= 6) {
			payload_size = std::stoull(argv[5]);
		}
		run_client(case_num, drop_rate, delay_ms, payload_size);
		return 0;
	} else {
		std::cerr << "Unknown role: " << role << std::endl;
		return 1;
	}
}
