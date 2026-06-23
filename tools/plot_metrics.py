import csv
import sys
import os
import matplotlib.pyplot as plt


def load_csv_data(csv_path):
    # 加载 CSV 数据并将毫秒时间戳归一化并转化为以秒为单位的时间。
    timestamps = []
    cwnd_values = []
    ssthresh_values = []
    rwnd_values = []
    events = []

    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["Timestamp"] == "Timestamp":
                continue
            timestamps.append(float(row["Timestamp"]) / 1000.0)
            cwnd_values.append(float(row["cwnd"]))
            ssthresh_values.append(float(row["ssthresh"]))
            rwnd_values.append(float(row["rwnd"]))
            events.append(row.get("Event", ""))

    if not timestamps:
        return None

    t_start = timestamps[0]
    timestamps = [t - t_start for t in timestamps]
    return timestamps, cwnd_values, ssthresh_values, rwnd_values, events


def plot_advanced_metrics():
    # 自动探测合法的发送端性能数据文件。策略是遍历所有日志，寻找 cwnd 极差波动最大的文件作为真实的发送端。
    target_file = None
    search_dir = "test_results" if os.path.exists("test_results") else "."

    max_cwnd_diff = 0

    for f_name in os.listdir(search_dir):
        if f_name.startswith("metrics_") and f_name.endswith(".csv"):
            path = os.path.join(search_dir, f_name)
            data = load_csv_data(path)

            if data:
                cwnd_values = data[1]
                diff = max(cwnd_values) - min(cwnd_values)

                if diff > max_cwnd_diff:
                    max_cwnd_diff = diff
                    target_file = path

    if not target_file or max_cwnd_diff == 0:
        print("[Error] No valid sender metrics file found.")
        return

    print(f"[Orchestrator] Plotting data from: {target_file}")
    timestamps, cwnd, ssthresh, rwnd, events = load_csv_data(target_file)

    # 字体与负号样式配置。
    plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "Arial", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    # ------------------ 图 1: 拥塞控制全景与微观图 ------------------
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(11, 7), gridspec_kw={"height_ratios": [1.2, 1]}
    )

    # 绘制发送端 cwnd 与慢启动阈值 ssthresh 的全景趋势图。
    ax1.plot(
        timestamps,
        cwnd,
        label="cwnd (Congestion Window)",
        color="#1f77b4",
        linewidth=1.5,
        zorder=2,
    )
    ax1.plot(
        timestamps,
        ssthresh,
        label="ssthresh",
        color="#ff7f0e",
        linestyle="--",
        linewidth=1.5,
        zorder=1,
    )

    # 标注异常丢包导致的 RTO 超时以及触发快速重传的散点事件。
    rto_t, rto_v = [], []
    fr_t, fr_v = [], []
    for t, c, e in zip(timestamps, cwnd, events):
        if "rto_timeout" in e:
            rto_t.append(t)
            rto_v.append(c)
        elif "fast_retransmit" in e:
            fr_t.append(t)
            fr_v.append(c)

    if rto_t:
        ax1.scatter(
            rto_t,
            rto_v,
            color="#d62728",
            marker="x",
            s=30,
            label="RTO Timeout",
            zorder=3,
        )
    if fr_t:
        ax1.scatter(
            fr_t,
            fr_v,
            color="#9467bd",
            marker="o",
            s=25,
            label="Fast Retransmit",
            zorder=3,
        )

    ax1.set_title(
        "LiteTCP Sender Congestion Control Status (TCP Reno)",
        fontsize=13,
        fontweight="bold",
        pad=12,
    )
    ax1.set_ylabel("Window Size (bytes)", fontsize=10)
    ax1.grid(True, linestyle=":", alpha=0.6)
    ax1.legend(loc="upper right", frameon=True, facecolor="white", edgecolor="none")

    # 局部微观放大视图，聚焦前 3 秒以清晰展示慢启动的指数增长和拥塞避免的线性增长锯齿波形。
    zoom_limit = 3.0
    zoom_indices = [i for i, t in enumerate(timestamps) if t <= zoom_limit]

    if zoom_indices:
        z_t = [timestamps[i] for i in zoom_indices]
        z_c = [cwnd[i] for i in zoom_indices]
        z_s = [ssthresh[i] for i in zoom_indices]

        ax2.plot(z_t, z_c, color="#1f77b4", linewidth=1.8)
        ax2.plot(z_t, z_s, color="#ff7f0e", linestyle="--", linewidth=1.5)
        ax2.fill_between(z_t, z_c, color="#1f77b4", alpha=0.1)

        z_rto_t = [t for t in rto_t if t <= zoom_limit]
        z_rto_v = [cwnd[timestamps.index(t)] for t in z_rto_t]
        if z_rto_t:
            ax2.scatter(z_rto_t, z_rto_v, color="#d62728", marker="x", s=40, zorder=3)

    ax2.set_title(
        f"Microscopic View: First {zoom_limit} Seconds Dynamic",
        fontsize=11,
        fontweight="bold",
        style="italic",
    )
    ax2.set_xlabel("Time (seconds)", fontsize=10)
    ax2.set_ylabel("Window Size (bytes)", fontsize=10)
    ax2.grid(True, linestyle=":", alpha=0.6)

    plt.tight_layout()
    plt.savefig(os.path.join(search_dir, "congestion_metrics.png"), dpi=300)
    plt.close()

    # ------------------ 图 2: 接收滑动窗口流量控制图 ------------------
    plt.figure(figsize=(10, 4))
    plt.plot(
        timestamps, rwnd, label="rwnd (Receiver Window)", color="#2ca02c", linewidth=2.0
    )
    plt.fill_between(timestamps, rwnd, color="#2ca02c", alpha=0.08)

    plt.title(
        "LiteTCP Receiver Flow Control Status (rwnd)",
        fontsize=12,
        fontweight="bold",
        pad=10,
    )
    plt.xlabel("Time (seconds)", fontsize=10)
    plt.ylabel("Window Size (bytes)", fontsize=10)
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.ylim(-5000, 75000)
    plt.legend(loc="lower right", frameon=True)
    plt.tight_layout()

    plt.savefig(os.path.join(search_dir, "flow_control_metrics.png"), dpi=300)
    plt.close()
    print("[Success] Re-generated professional plots safely.")


if __name__ == "__main__":
    plot_advanced_metrics()
