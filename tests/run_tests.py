#!/usr/bin/env python3
import subprocess
import time
import sys
import os
import glob

# 动态配置环境变量 PATH，确保测试程序二进制文件能够正确路由执行。
build_bin = os.path.abspath("build/bin")
if os.path.exists(build_bin):
    os.environ["PATH"] = build_bin + os.path.pathsep + os.environ.get("PATH", "")
else:
    os.environ["PATH"] = (
        os.path.abspath("bin") + os.path.pathsep + os.environ.get("PATH", "")
    )


def clean_test_outputs():
    # 查找并清理历史遗留的吞吐量 CSV 度量日志、校验和以及可视化的 PNG 关系图表，防止脏数据干扰。
    search_dir = "test_results" if os.path.exists("test_results") else "."
    for path in glob.glob(os.path.join(search_dir, "metrics_*.csv")):
        try:
            os.remove(path)
        except Exception:
            pass
    for path in glob.glob(os.path.join(search_dir, "checksum_*.txt")):
        try:
            os.remove(path)
        except Exception:
            pass
    for path in glob.glob(os.path.join(search_dir, "*_metrics.png")):
        try:
            os.remove(path)
        except Exception:
            pass


def run_case(test_name, role, case_num, *extra_args):
    # 根据传入参数和额外配置执行子进程用例。
    cmd = [test_name, role, str(case_num)] + list(map(str, extra_args))
    return subprocess.Popen(cmd)


def start_proxy():
    # 启动 UDP 网络中继代理，以在本地进行丢包、延时仿真。
    print("[Orchestrator] Starting network proxy...")
    proxy = subprocess.Popen(["python3", "tools/network_proxy.py"])
    time.sleep(1.0)
    return proxy


def kill_process(proc):
    # 优雅或者强制关闭未退出的子进程，回收其资源。
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()


def run_connect_tests():
    # 执行握手连接阶段的集成测试，包含标准三次握手和 SYN 报文丢弃重传测试。
    print("\n========================================")
    print("  LiteTCP Test Connect: Start Proxy  ")
    print("========================================")
    proxy = start_proxy()
    try:
        print("\n--- Running Case 1: Standard 3-Way Handshake ---")
        server = run_case("test_connect", "--server", 1)
        time.sleep(0.5)
        client = run_case("test_connect", "--client", 1)

        c_ret = client.wait()
        s_ret = server.wait()
        assert c_ret == 0, f"Client Case 1 failed with code {c_ret}"
        assert s_ret == 0, f"Server Case 1 failed with code {s_ret}"
        print("[SUCCESS] Case 1 passed!")

        print("\n--- Running Case 2: SYN Packet Drop & Retransmit ---")
        server = run_case("test_connect", "--server", 2)
        time.sleep(0.5)
        client = run_case("test_connect", "--client", 2)

        c_ret = client.wait()
        s_ret = server.wait()
        assert c_ret == 0, f"Client Case 2 failed with code {c_ret}"
        assert s_ret == 0, f"Server Case 2 failed with code {s_ret}"
        print("[SUCCESS] Case 2 passed!")

        print("========================================")
        print("  LiteTCP Test Connect All Passed!      ")
        print("========================================")
    finally:
        kill_process(proxy)


def run_close_tests():
    # 执行连接挥手释放阶段的集成测试，包括单边主动关闭及双向同时发起 close 关闭连接。
    print("\n========================================")
    print("  LiteTCP Test Close: Start Proxy     ")
    print("========================================")
    proxy = start_proxy()
    try:
        print("\n--- Running Case 1: Active Close & Passive Close ---")
        server = run_case("test_close", "--server", 1)
        time.sleep(0.5)
        client = run_case("test_close", "--client", 1)

        c_ret = client.wait()
        s_ret = server.wait()
        assert c_ret == 0, f"Client Case 1 failed with code {c_ret}"
        assert s_ret == 0, f"Server Case 1 failed with code {s_ret}"
        print("[SUCCESS] Case 1 passed!")

        print("\n--- Running Case 2: Simultaneous Close ---")
        server = run_case("test_close", "--server", 2)
        time.sleep(0.5)
        client = run_case("test_close", "--client", 2)

        c_ret = client.wait()
        s_ret = server.wait()
        assert c_ret == 0, f"Client Case 2 failed with code {c_ret}"
        assert s_ret == 0, f"Server Case 2 failed with code {s_ret}"
        print("[SUCCESS] Case 2 passed!")

        print("========================================")
        print("  LiteTCP Test Close All Passed!        ")
        print("========================================")
    finally:
        kill_process(proxy)


def run_reliable_tests(drop_rate=0.05, delay_ms=10, payload_size=5242880):
    # 执行可靠数据传输及流控测试，包含零窗口探测测试以及在丢包与延迟环境下的 custom 大文件传输。
    print("\n========================================")
    print("  LiteTCP Test Reliable: Start Proxy  ")
    print("========================================")
    proxy = start_proxy()
    try:
        print("\n--- Running Case 1: Flow Control & Zero Window Probe ---")
        server = run_case("test_reliable", "--server", 1)
        time.sleep(0.5)
        client = run_case("test_reliable", "--client", 1)

        c_ret = client.wait()
        s_ret = server.wait()
        assert c_ret == 0, f"Client Case 1 failed with code {c_ret}"
        assert s_ret == 0, f"Server Case 1 failed with code {s_ret}"
        print("[SUCCESS] Case 1 passed!")

        clean_test_outputs()

        print(
            f"\n--- Running Case 2: {payload_size / (1024*1024):.2f}MB File Transfer under {drop_rate*100:.1f}% Packet Loss & {delay_ms}ms Delay ---"
        )
        server = run_case("test_reliable", "--server", 2, payload_size)
        time.sleep(0.5)
        client = run_case(
            "test_reliable", "--client", 2, drop_rate, delay_ms, payload_size
        )

        c_ret = client.wait()
        s_ret = server.wait()
        assert c_ret == 0, f"Client Case 2 failed with code {c_ret}"
        assert s_ret == 0, f"Server Case 2 failed with code {s_ret}"

        # 比对两端算出来的应用层数据累加校验和是否相符。
        chk_client = (
            "test_results/checksum_client.txt"
            if os.path.exists("test_results/checksum_client.txt")
            else "checksum_client.txt"
        )
        chk_server = (
            "test_results/checksum_server.txt"
            if os.path.exists("test_results/checksum_server.txt")
            else "checksum_server.txt"
        )
        with open(chk_client, "r") as f:
            val_c = f.read().strip()
        with open(chk_server, "r") as f:
            val_s = f.read().strip()

        print(f"[Verification] Client Checksum: {val_c}, Server Checksum: {val_s}")
        assert val_c == val_s, f"Checksum mismatch! Client: {val_c}, Server: {val_s}"
        print("[SUCCESS] Case 2 passed!")

        # 调用绘图工具根据生成的 CSV 数据自动渲染并导出性能曲线图表。
        print("[Orchestrator] Generating performance metrics plot...")
        subprocess.run([sys.executable, "tools/plot_metrics.py", "--no-show"])

        print("========================================")
        print("  LiteTCP Test Reliable All Passed!     ")
        print("========================================")
    finally:
        kill_process(proxy)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="LiteTCP Test Orchestrator")
    parser.add_argument(
        "target",
        choices=["connect", "close", "reliable", "all"],
        default="all",
        nargs="?",
        help="Test target",
    )
    parser.add_argument(
        "--drop",
        type=float,
        default=0.02,
        help="Packet drop rate (0.0 to 1.0) for reliable test",
    )
    parser.add_argument(
        "--delay", type=int, default=20, help="Packet delay in ms for reliable test"
    )
    parser.add_argument(
        "--size",
        type=int,
        default=5242880,
        help="Payload size in bytes for reliable test (default 5MB)",
    )
    args = parser.parse_args()

    os.makedirs("test_results", exist_ok=True)

    clean_test_outputs()

    try:
        if args.target == "connect":
            run_connect_tests()
        elif args.target == "close":
            run_close_tests()
        elif args.target == "reliable":
            run_reliable_tests(
                drop_rate=args.drop, delay_ms=args.delay, payload_size=args.size
            )
        elif args.target == "all":
            run_connect_tests()
            run_close_tests()
            run_reliable_tests(
                drop_rate=args.drop, delay_ms=args.delay, payload_size=args.size
            )
        else:
            print(f"Unknown test target: {args.target}")
            sys.exit(1)
    except AssertionError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
