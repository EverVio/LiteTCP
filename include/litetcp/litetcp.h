#pragma once
#include <litetcp/TcpSocket.h>

// LiteTCP 套接字类型定义，与底层 TcpSocket 类一一映射。
typedef TcpSocket litetcp_t;

// LiteTCP 套接字虚拟网络地址结构定义，与 LiteSockAddr 保持一致。
typedef LiteSockAddr litetcp_sock_addr;

extern "C" {
// 创建一个新的 LiteTCP 套接字实例，成功时返回该套接字指针。
litetcp_t* litetcp_socket();

// 将套接字 `sock` 绑定到指定的本地网络地址 `bind_addr`，成功返回 0，失败返回 -1。
int litetcp_bind(litetcp_t* sock, litetcp_sock_addr bind_addr);

// 将套接字 `sock` 置于监听状态以准备接收客户端连接，成功返回 0，失败返回 -1。
int litetcp_listen(litetcp_t* sock);

// 阻塞等待并接受监听套接字 `sock` 上的客户端连接，成功返回新建立连接的子套接字指针，失败返回 nullptr。
litetcp_t* litetcp_accept(litetcp_t* sock);

// 向目标地址 `target_addr` 发起 TCP 连接请求，阻塞至握手成功返回 0 或超时失败返回 -1。
int litetcp_connect(litetcp_t* sock, litetcp_sock_addr target_addr);

// 发送缓冲区 `buffer` 中长度为 `len` 的数据，可能会因发送窗口满而阻塞，成功时返回实际发送的字节数，失败返回 -1。
int litetcp_send(litetcp_t* sock, const void* buffer, int len);

// 从套接字 `sock` 中读取最大长度为 `len` 的就绪数据到 `buffer` 中，阻塞至有数据可读返回实际长度，对端正常关闭返回 0，出错返回 -1。
int litetcp_recv(litetcp_t* sock, void* buffer, int len);

// 关闭套接字 `sock` 并释放所有相关资源，主动关闭端将在此触发 FIN 挥手流程，成功返回 0，失败返回 -1。
int litetcp_close(litetcp_t* sock);
}
