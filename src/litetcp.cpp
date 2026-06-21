#include <litetcp/litetcp.h>

litetcp_t* litetcp_socket() {
	// 实例化一个全新的面向对象 TcpSocket 实例
	return new TcpSocket();
}

int litetcp_bind(litetcp_t* sock, litetcp_sock_addr bind_addr) {
	if (!sock)
		return -1;
	// 委托底层的 TcpSocket 进行端口绑定
	return sock->bind(bind_addr);
}

int litetcp_listen(litetcp_t* sock) {
	if (!sock)
		return -1;
	// 委托底层的 TcpSocket 进入监听状态，并开启底层路由
	return sock->listen();
}

litetcp_t* litetcp_accept(litetcp_t* sock) {
	if (!sock)
		return nullptr;
	// 阻塞等待，直到从三次握手完成队列中弹出一个新的子套接字
	return sock->accept();
}

int litetcp_connect(litetcp_t* sock, litetcp_sock_addr target_addr) {
	if (!sock)
		return -1;
	// 发起主动握手流程，并阻塞等待对端 ACK 回应
	return sock->connect(target_addr);
}

int litetcp_send(litetcp_t* sock, const void* buffer, int len) {
	if (!sock)
		return -1;
	// 发送用户缓冲数据，受拥塞窗口与接收窗口流量控制限制
	return sock->send(buffer, len);
}

int litetcp_recv(litetcp_t* sock, void* buffer, int len) {
	if (!sock)
		return -1;
	// 从环形缓冲区读取就绪的数据，阻塞至有数据或者连接断开
	return sock->recv(buffer, len);
}

int litetcp_close(litetcp_t* sock) {
	if (!sock)
		return -1;
	// 释放并关闭套接字，主动发起 FIN 挥手，最后 delete 释放内存
	int ret = sock->close();
	delete sock;
	return ret;
}
