# LiteTCP: 轻量级用户态 TCP 框架

LiteTCP 是一个基于 **C++17 编程标准** 实现的轻量级用户态 TCP 框架。该框架通过运行在本地 UDP 媒介之上，简化模拟了经典 TCP 协议的核心要素，包括三次握手与四次挥手连接管理、基于滑动窗口的流量控制、重传计时器管理，以及对 TCP Reno 拥塞控制快速恢复机制的基本逻辑模拟。

项目设计为一个轻量级 TCP 框架原型，可用于网络协议学习与白盒机制仿真，内建了一套基于 POSIX 套接字的网络中继代理与自动化多进程白盒测试套件，能够演示弱网环境（如随机丢包、网络延迟等）下的滑动窗口更新及超时重传动态过程。

---

## 1. 主要机制模拟

* **连接管理基本状态机**：模拟了经典 TCP 的三次握手连接建立与四次挥手连接释放（包括主动关闭、被动关闭以及双方同时发起关闭的状态变迁）。利用随机初始序列号及 RTO 超时重传机制对控制报文的丢失进行兜底。
* **可靠传输与滑动窗口**：模拟了简化的发送与接收滑动窗口机制，支持累积确认以及基于乱序区间的重组落盘；集成了 16 位 Internet 网络校验和算法对模拟损毁报文进行自动丢弃。
* **基本流量控制**：接收端根据环形缓冲区剩余空闲空间动态计算接收窗口 `rwnd` 并向对端通告；发送端自适应限制在途字节数，并在 `rwnd == 0` 时启动周期性的零窗口探测。
* **拥塞控制模拟 (TCP Reno)**：在用户态模拟了由慢启动、拥塞避免、快速重传与快速恢复构成的控制环路。快速恢复阶段在收到推进确认号的新 ACK 后立即退出并恢复窗口至阈值。

---

## 2. 核心技术选型与演示

为了在单机多进程环境下清晰展示网络控制逻辑，框架在实现上采用了以下结构设计：

* **基于 RAII 的同步控制**：使用标准 C++11 的 `std::mutex` 与 `std::condition_variable` 进行临界区保护与线程阻塞同步，利用挂起/唤醒模拟阻塞式套接字 API 语义。
* **全局单线程计时与惰性删除**：使用单一后台线程加 `std::priority_queue` 最小堆进行定时事件的统一调度，避开高成本的逐套接字轮询。通过递增 `event_id` 的版本比对实现对失效定时事件的惰性删除，精简了事件的修改开销。
* **单线程弱网仿真代理**：配套提供了一套轻量级的单线程中继脚本（[network_proxy.py](tools/network_proxy.py)），基于事件循环与条件变量模拟精确的时延和丢包行为，防止了“每包一线程”的高并发线程创建与上下文切换消耗。
* **内存管理与安全性设计**：使用静态大小的 `std::array` 区间表来维护乱序数据分片，在乱序重组生命周期中避免动态分配堆内存；对环形接收缓冲区进行了无符号数溢出与边界拦截防护。

---

## 3. 项目目录结构

```
/LiteTCP
├── CMakeLists.txt                 # 统一的项目构建配置文件
├── /include                       # 头文件目录
├── /include/litetcp               # 项目同名子目录，防止外部集成时重名冲突
│   ├── TcpPacket.h                # 报文结构定义、序列化与校验和计算
│   ├── TcpSocket.h                # Socket状态机、滑动窗口、缓冲区管理
│   └── litetcp.h                  # C-Style 阻塞式用户 API 封装层
├── /src                           # 源代码与私有头文件目录
│   ├── NetworkEngine.h            # 协议栈内部实现组件（私有头文件）
│   ├── TimerManager.h             # 协议栈内部实现组件（私有头文件）
│   ├── TcpPacket.cpp              # 序列化/反序列化及校验和实现
│   ├── TcpSocket.cpp              # 职责 1：生命周期与阻塞用户 API (API 层)
│   ├── TcpSocket_State.cpp        # 职责 2：报文输入处理与拥塞状态机 (核心逻辑层)
│   ├── TcpSocket_Timer.cpp        # 职责 3：超时重传、零窗口探测与 TIME_WAIT (定时器层)
│   ├── NetworkEngine.cpp          # 基于 Linux POSIX 套接字的路由分发与底层 UDP 物理收发
│   ├── TimerManager.cpp           # 全局单线程计时管理器实现
│   └── litetcp.cpp                # 兼容包装实现
├── /tests                         # 全自动多进程白盒测试套件
│   ├── /common                    # 测试基础设施目录
│   │   └── TestHarness.hpp        # 自定义极简测试宏与进程管理器
│   ├── test_connect.cpp           # 建立连接测试（含常规握手、SYN包丢失超时重传）
│   ├── test_close.cpp             # 断开连接测试（含主动关闭、被动关闭、同时关闭）
│   ├── test_reliable.cpp          # 可靠传输测试（含滑动窗口流控、弱网下大文件传输）
│   └── run_tests.py               # 全自动测试编排脚本，负责 Proxy/Client/Server 生命周期管理
├── /tools                         # 辅助工具链
│   ├── network_proxy.py           # 单机 Python 异常仿真中转代理
│   └── plot_metrics.py            # 离线 CSV 性能指标绘图脚本
└── /test_results                  # [测试产物] 存放测试运行产生的日志数据和可视化图表
```

---

## 4. 构建与测试方法

### 4.1 编译环境准备

本项目运行于标准 Linux 操作系统或 POSIX 兼容环境。
* **依赖环境**：支持 C++17 标准的 GCC 编译器、CMake 3.12+、Make 或 Ninja 构建系统，以及 Python 3.x 环境。

在项目根目录下进行一键配置与编译：
```bash
# 1. 配置项目，采用 cmake 自动扫描编译依赖
cmake -B build -S .

# 2. 编译静态库与所有测试程序
cmake --build build
```
编译成功后：
* 生成的可执行测试二进制文件将输出到构建目录下的 `build/bin/` 目录。
* 静态库文件将输出到构建目录下的 `build/lib/` 目录。

### 4.2 运行自动化测试

我们使用 Python 脚本进行自动化测试的管理和流程编排，会自动在后台调起 Proxy 进行网络中转以及 Client / Server 的交互，并保证所有进程在异常或退出时被安全回收：

1. **运行全部测试用例**：
   ```bash
   python3 tests/run_tests.py all
   ```
2. **三次握手测试**：
   ```bash
   python3 tests/run_tests.py connect
   ```
3. **四次挥手测试**：
   ```bash
   python3 tests/run_tests.py close
   ```
4. **可靠传输流控测试**：
   * 默认参数运行（0.5% 丢包率，30ms 延迟，5MB 负载）：
     ```bash
     python3 tests/run_tests.py reliable
     ```
   * 自定义参数运行：
     ```bash
     # 示例：设置丢包率 8%，延迟 15ms，传输负载 2MB
     python3 tests/run_tests.py reliable --drop 0.08 --delay 15 --size 2097152
     ```

### 4.3 性能指标可视化

在可靠传输测试运行后，程序会自动将性能指标写入 `test_results/metrics_<port>.csv`。运行以下脚本可离线绘制 `cwnd` / `ssthresh` / `rwnd` 的性能波动图：
```bash
python3 tools/plot_metrics.py
```
生成的图表保存在：
* `test_results/congestion_metrics.png` (拥塞状态多视图全景图)
* `test_results/flow_control_metrics.png` (流量控制接收窗口性能图)

---

## 5. 主要亮点

1. **物理 UDP 端口复用与路由分发**：
   在 [NetworkEngine.cpp](src/NetworkEngine.cpp) 中，引擎绑定物理 UDP 端口，当收到数据时，在临界区内快速检索出与之对应的已建立连接套接字（`established_sock`）或处于监听状态的套接字（`listen_sock`）并派发处理。这实现了在单个物理 UDP 传输通道上的解复用与路由派发，为用户态协议栈提供了统一、轻量级的网络收发物理通道。
2. **标准 TCP Reno 拥塞算法与滑动窗口的结合**：
   在 [TcpSocket_State.cpp](src/TcpSocket_State.cpp) 中，发送数据的上限受限于 `std::min(cwnd, peer_rwnd)`。在收到连续 3 个冗余 ACK 时，程序会减半 `ssthresh` 并进入快速恢复，同时将 `cwnd` 膨胀为 `ssthresh + 3*MSS` 以在恢复期间维持网络吞吐；当收到推进确认号的新 ACK 后立即退出快速恢复并将 `cwnd` 收缩回 `ssthresh`，展示了经典的窗口拥塞负反馈调整机制。
3. **基于 `wait_for` 的高健壮性阻断式设计**：
   在 [TcpSocket.cpp](src/TcpSocket.cpp) 中，阻塞式接口 `connect` 与 `close` 的条件变量等待均集成了合理的时间上限（10 秒），防止了底层网络极端故障下主进程无限卡死，体现了软件设计中的“防御性编程”思想。
4. **全局单线程精确计时器与惰性删除机制**：
   在 [TimerManager.cpp](src/TimerManager.cpp) 中，轻量化重构了全局定时器系统，利用最小堆的 `std::condition_variable::wait_until` 精确阻塞，避免暴力轮询，且通过 `event_id` 比对惰性丢弃失效定时事件。利用 unregister 握手设计优雅地排除了 UAF 与死锁。
5. **动态随机初始序列号与安全握手机制**：
   在握手阶段，客户端与服务端在初始化连接和创建半连接时均采用 `std::random_device` 配合 `std::mt19937` 动态随机生成唯一的初始序列号，并通过 `ack == snd_una + 1` 动态校验对端确认。彻底去除了协议栈死等硬编码初始序号的缺陷，在算法层面上演示了标准 TCP 的安全性规范，防范了盲猜序列号等包注入劫持攻击。

---

## 6. 本框架的局限性

**LiteTCP 是一个轻量级 TCP 框架原型，并非生产级标准 TCP 协议栈。** 相比于标准内核 TCP 实现，LiteTCP 存在以下局限与简化：

1. **协议头选项缺失**：不支持标准 TCP Header 中的任何 TCP 选项，如最大报文段大小、选择性确认、时间戳选项及窗口缩放等。
2. **连接多路复用简化**：**全局仅支持单一连接的路由。** 为了简化网络引擎的设计，网络分发引擎仅维护了单个监听套接字（`listen_sock`）和单个已建立连接的套接字（`established_sock`），无法在多连接并发或高吞吐下提供多路复用支持。
3. **拥塞控制算法简化**：本框架的拥塞控制仅是对 TCP Reno 的慢启动、拥塞避免、快速重传与快速恢复核心逻辑的简化模拟，并未实现选择性确认（SACK）、限制传输和 RTT 去二义性等增强机制，不具备真实复杂网络的吞吐优化能力。
4. **异常情况处理有限**：对网络连接中的异常复位、乱序滑动窗口边缘回绕、以及复杂半开连接的诊断清理等，仅实现了最基础的超时注销，未实现复杂的连接保活与异常状态自愈协议。
