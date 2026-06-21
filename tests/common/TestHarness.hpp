#pragma once
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#define LITE_TEST_CASE(name) void name()

#define LITE_ASSERT_EQ(actual, expected)                                                      \
	if ((actual) != (expected)) {                                                             \
		std::cerr << "[ERROR] Assert failed at " << __FILE__ << ":" << __LINE__               \
				  << " -> Expected: " << (expected) << ", Actual: " << (actual) << std::endl; \
		std::exit(-1);                                                                        \
	}

#define LITE_ASSERT_TRUE(condition)                                             \
	if (!(condition)) {                                                         \
		std::cerr << "[ERROR] Assert failed at " << __FILE__ << ":" << __LINE__ \
				  << " -> Expected TRUE, but got FALSE" << std::endl;           \
		std::exit(-1);                                                          \
	}

// 辅助函数：通过 Linux 本地 UDP 套接字向 Python 代理发送控制指令以执行特定动作。
inline void notify_proxy(const std::string& cmd) {
	int s = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (s < 0)
		return;

	sockaddr_in dest{};
	dest.sin_family = AF_INET;
	dest.sin_port = htons(20217);
	dest.sin_addr.s_addr = inet_addr("127.0.0.1");

	sendto(s, cmd.c_str(), cmd.size(), 0, (struct sockaddr*)&dest, sizeof(dest));

	close(s);
}
