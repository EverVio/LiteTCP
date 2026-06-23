import socket
import threading
import random
import time
import sys
import heapq

# 全局模拟网络环境参数，包含默认丢包率、延迟时延以及 SYN 丢包触发器。
DROP_RATE = 0.0
DELAY_MS = 0
drop_next_syn = False

SERVER_PORT = 20221
CLIENT_PORT = 20222
PROXY_PORT = 20218
CONTROL_PORT = 20217


class DelayForwarder:
    """
    单线程事件循环延迟转发器，通过基于时间最小堆的优先级队列和条件变量挂起，精准调度包在指定的延迟时延后发出。
    """

    def __init__(self, send_sock):
        self.send_sock = send_sock
        self.queue = []
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.running = True
        self.counter = 0
        self.thread = threading.Thread(target=self._run)
        self.thread.daemon = True
        self.thread.start()

    def add_packet(self, data, dest_port, delay_ms):
        # 计算该数据包预期的触发到期绝对时刻，将其推入堆队列，并唤醒循环线程重新评估等待的最早时延。
        expire_time = time.time() + delay_ms / 1000.0
        with self.lock:
            self.counter += 1
            heapq.heappush(self.queue, (expire_time, self.counter, data, dest_port))
            self.cv.notify()

    def _run(self):
        # 后台线程循环，若无包挂起，若首个包未到期则精确睡眠等待；到期后弹出该包并在锁外发送。
        while self.running:
            with self.lock:
                if not self.queue:
                    self.cv.wait()
                    continue

                expire_time, _, data, dest_port = self.queue[0]
                now = time.time()

                if expire_time > now:
                    self.cv.wait(timeout=expire_time - now)
                    continue

                heapq.heappop(self.queue)

            try:
                self.send_sock.sendto(data, ("127.0.0.1", dest_port))
            except Exception:
                pass


def control_thread_func():
    # 监听 20217 控制端口，接收测试主控端下发的 DROP_SYN 握手丢包或 SET_DROP_RATE 弱网参数修改指令。
    global DROP_RATE, DELAY_MS, drop_next_syn
    control_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    control_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    control_sock.bind(("127.0.0.1", CONTROL_PORT))
    print(f"[Proxy] Control channel listening on 127.0.0.1:{CONTROL_PORT}")
    while True:
        try:
            data, addr = control_sock.recvfrom(1024)
            cmd = data.decode("utf-8").strip()
            print(f"[Proxy] Received control command: {cmd}")
            if cmd == "DROP_SYN":
                drop_next_syn = True
            elif cmd.startswith("SET_DROP_RATE"):
                parts = cmd.split()
                if len(parts) == 2:
                    DROP_RATE = float(parts[1])
                    print(f"[Proxy] DROP_RATE set to {DROP_RATE}")
            elif cmd.startswith("SET_DELAY"):
                parts = cmd.split()
                if len(parts) == 2:
                    DELAY_MS = int(parts[1])
                    print(f"[Proxy] DELAY_MS set to {DELAY_MS}")
        except Exception as e:
            print(f"[Proxy] Control channel error: {e}")
            break


def main():
    global drop_next_syn
    proxy_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    proxy_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    proxy_sock.bind(("127.0.0.1", PROXY_PORT))

    send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    forwarder = DelayForwarder(send_sock)

    t = threading.Thread(target=control_thread_func)
    t.daemon = True
    t.start()

    print(f"[Proxy] Data forwarder listening on 127.0.0.1:{PROXY_PORT}")

    # 接收来自 Server/Client 的数据，按规则进行对端端口重映射转发，并应用丢包及延迟调度。
    while True:
        try:
            data, addr = proxy_sock.recvfrom(2048)
            src_port = addr[1]

            if src_port == SERVER_PORT:
                dest_port = CLIENT_PORT
            elif src_port == CLIENT_PORT:
                dest_port = SERVER_PORT
            else:
                continue

            # 从头部第 16 字节提取 flags 判定是否是 SYN 包。
            is_syn = False
            if len(data) >= 20:
                flags = data[16]
                is_syn = (flags & 0x08) != 0

            if is_syn and drop_next_syn:
                print("[Proxy] Drop next SYN package triggered!")
                drop_next_syn = False
                continue

            # 执行随机丢包判定。
            if random.random() < DROP_RATE:
                continue

            # 延迟转发处理。
            if DELAY_MS > 0:
                forwarder.add_packet(data, dest_port, DELAY_MS)
            else:
                send_sock.sendto(data, ("127.0.0.1", dest_port))

        except Exception as e:
            print(f"[Proxy] Error in forwarder: {e}")
            break


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("[Proxy] Exit.")
