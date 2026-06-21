#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

class TcpSocket;

// 定时器事件节点结构，用于静态堆队列的调度。
struct TimerEvent {
	std::chrono::steady_clock::time_point expire_time;	// 定时器触发的绝对时间戳。
	TcpSocket* socket;									// 关联的套接字实例。
	uint64_t event_id;									// 事件版本 ID，用以支持定时器的惰性删除。

	// 比较运算符重载，使 expire_time 较早的事件在最小堆中优先级最高。
	bool operator>(const TimerEvent& other) const {
		return expire_time > other.expire_time;
	}
};

// 全局静态定时器管理器，采用独立后台线程加精确时间唤醒的方式提供微秒级定时服务。
class TimerManager {
private:
	static std::priority_queue<TimerEvent, std::vector<TimerEvent>, std::greater<TimerEvent>> timer_queue;	// 超时事件最小堆。
	static std::mutex queue_mutex;																			// 保护事件队列和状态表的互斥锁。
	static std::condition_variable cv;																		// 驱动定时线程精准睡眠与唤醒的条件变量。
	static std::thread worker_thread;																		// 运行定时器主循环的后台线程。
	static std::atomic<bool> running;																		// 定时器管理器的运行状态。

	static std::unordered_map<TcpSocket*, uint64_t> active_sockets;	 // 跟踪注册的活跃套接字及其最近有效的事件 ID。
	static TcpSocket* processing_socket;							 // 当前正在执行超时回调的套接字指针，用于防止析构冲突。
	static std::condition_variable processing_cv;					 // 协调处理中套接字注销流程的条件变量。

	// 后台定时器线程的执行函数，负责在事件到期时安全调用 handle_timeout。
	static void run();

public:
	// 启动定时器线程，初始化全局状态。
	static void initialize();

	// 停止并清理定时器管理器，注销所有事件。
	static void shutdown();

	// 注册套接字，使其能调度定时器。
	static void register_socket(TcpSocket* socket);

	// 注销套接字，并阻塞等待其当前正在运行的定时器回调结束以确保线程安全。
	static void unregister_socket(TcpSocket* socket);

	// 向全局队列添加一个新的定时事件，并在需要时提前唤醒定时器线程。
	static void add_timer(std::chrono::steady_clock::time_point expire_time, TcpSocket* socket, uint64_t event_id);
};
