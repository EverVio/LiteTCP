#include "TimerManager.h"

#include <litetcp/TcpSocket.h>

#include <iostream>

// 初始化静态成员变量，包含全局的超时优先级队列、互斥锁、控制条件变量及后台管理线程。
std::priority_queue<TimerEvent, std::vector<TimerEvent>, std::greater<TimerEvent>> TimerManager::timer_queue;
std::mutex TimerManager::queue_mutex;
std::condition_variable TimerManager::cv;
std::thread TimerManager::worker_thread;
std::atomic<bool> TimerManager::running(false);

std::unordered_map<TcpSocket*, uint64_t> TimerManager::active_sockets;
TcpSocket* TimerManager::processing_socket = nullptr;
std::condition_variable TimerManager::processing_cv;

void TimerManager::initialize() {
	std::lock_guard<std::mutex> lock(queue_mutex);
	if (running)
		return;

	running = true;
	worker_thread = std::thread(run);
}

void TimerManager::shutdown() {
	{
		std::lock_guard<std::mutex> lock(queue_mutex);
		if (!running)
			return;
		running = false;
		cv.notify_all();
	}

	if (worker_thread.joinable()) {
		worker_thread.join();
	}

	std::lock_guard<std::mutex> lock(queue_mutex);
	// 清空堆队列，释放事件内存。
	while (!timer_queue.empty()) {
		timer_queue.pop();
	}
	active_sockets.clear();
	processing_socket = nullptr;
}

void TimerManager::register_socket(TcpSocket* socket) {
	std::lock_guard<std::mutex> lock(queue_mutex);
	active_sockets[socket] = 0;
}

void TimerManager::unregister_socket(TcpSocket* socket) {
	std::unique_lock<std::mutex> lock(queue_mutex);
	active_sockets.erase(socket);

	// 阻塞等待，直到正在处理当前 `socket` 的定时器线程完成了其超时回调，防止销毁悬空指针。
	processing_cv.wait(lock, [socket]() {
		return processing_socket != socket;
	});
}

void TimerManager::add_timer(std::chrono::steady_clock::time_point expire_time, TcpSocket* socket, uint64_t event_id) {
	std::lock_guard<std::mutex> lock(queue_mutex);

	// 只有当该套接字依然处于注册的活跃映射表中时，才允许将其推入堆调度，并更新最新事件版本 ID。
	auto it = active_sockets.find(socket);
	if (it != active_sockets.end()) {
		it->second = event_id;
		timer_queue.push({expire_time, socket, event_id});
		cv.notify_one();
	}
}

void TimerManager::run() {
	while (running) {
		std::unique_lock<std::mutex> lock(queue_mutex);
		if (timer_queue.empty()) {
			cv.wait(lock);
			continue;
		}

		auto now = std::chrono::steady_clock::now();
		auto event = timer_queue.top();

		if (event.expire_time > now) {
			// 事件未到期，驱动条件变量精确睡眠，唤醒时间点设为最早到期事件的时刻。
			cv.wait_until(lock, event.expire_time);
			continue;
		}

		timer_queue.pop();

		// 惰性删除检测：检查套接字是否已注销，或该事件版本 ID 是否已被后续更新的定时任务覆盖。
		auto it = active_sockets.find(event.socket);
		if (it == active_sockets.end() || it->second != event.event_id) {
			continue;
		}

		// 记录当前正在运行超时逻辑的套接字。
		processing_socket = event.socket;

		// 释放队列锁以缩小互斥锁范围，避免在获取 socket_mutex 时与其它并发操作造成死锁。
		lock.unlock();

		{
			// 持有套接字自身的互斥锁，安全调用套接字的超时处理器。
			std::lock_guard<std::mutex> sock_lock(processing_socket->socket_mutex);
			processing_socket->handle_timeout();
		}

		lock.lock();
		processing_socket = nullptr;
		processing_cv.notify_all();
	}
}
