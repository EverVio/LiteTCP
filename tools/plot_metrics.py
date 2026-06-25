#!/usr/bin/env python3
import csv
import sys
import os
import matplotlib.pyplot as plt


def load_csv_data(csv_path, t_start_align=None):
    timestamps = []
    cwnd_values = []
    ssthresh_values = []
    rwnd_values = []
    rto_values = []
    ooo_values = []
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
            # 兼容读取新增加的扩展指标，若未定义则提供安全缺省值
            rto_values.append(float(row.get("rto", 0.2)))
            ooo_values.append(int(row.get("ooo_count", 0)))
            events.append(row.get("Event", ""))

    if not timestamps:
        return None

    actual_start = timestamps[0]
    base_start = t_start_align if t_start_align is not None else actual_start
    timestamps = [t - base_start for t in timestamps]
    return (
        timestamps,
        cwnd_values,
        ssthresh_values,
        rwnd_values,
        rto_values,
        ooo_values,
        events,
        actual_start,
    )


def plot_advanced_metrics():
    search_dir = "test_results" if os.path.exists("test_results") else "."
    csv_files = []
    for f_name in os.listdir(search_dir):
        if f_name.startswith("metrics_") and f_name.endswith(".csv"):
            csv_files.append(os.path.join(search_dir, f_name))

    if not csv_files:
        print("[Error] No valid metrics files found.")
        return

    sender_file = None
    receiver_file = None
    max_cwnd_diff = -1

    for path in csv_files:
        data = load_csv_data(path)
        if data:
            cwnd_values = data[1]
            diff = max(cwnd_values) - min(cwnd_values)
            if diff > max_cwnd_diff:
                max_cwnd_diff = diff
                sender_file = path

    for path in csv_files:
        if path != sender_file:
            receiver_file = path
            break

    if not sender_file:
        print("[Error] No valid sender metrics file found.")
        return

    print(f"[Orchestrator] Sender data from: {sender_file}")
    if receiver_file:
        print(f"[Orchestrator] Receiver data from: {receiver_file}")

    sender_data = load_csv_data(sender_file)
    if not sender_data:
        print("[Error] Failed to load sender data.")
        return

    timestamps, cwnd, ssthresh, rwnd, rto, ooo_count_sender, events, sender_start = (
        sender_data
    )

    if receiver_file:
        receiver_data = load_csv_data(receiver_file, t_start_align=sender_start)
        if receiver_data:
            rec_timestamps, _, _, _, _, ooo_count_receiver, _, _ = receiver_data
            ooo_timestamps = rec_timestamps
            ooo_count = ooo_count_receiver
        else:
            ooo_timestamps = timestamps
            ooo_count = ooo_count_sender
    else:
        ooo_timestamps = timestamps
        ooo_count = ooo_count_sender

    plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "Arial", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    # 创建 4X1 的垂直对齐子图布局，共享 X 轴时间线
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(12, 11), sharex=True)

    # ------------------ Subplot 1: Congestion Control (cwnd / ssthresh) ------------------
    ax1.plot(timestamps, cwnd, label="cwnd", color="#1f77b4", linewidth=1.5, zorder=2)
    ax1.plot(
        timestamps,
        ssthresh,
        label="ssthresh",
        color="#ff7f0e",
        linestyle="--",
        linewidth=1.5,
        zorder=1,
    )

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
            s=40,
            label="RTO Timeout",
            zorder=3,
        )
    if fr_t:
        ax1.scatter(
            fr_t,
            fr_v,
            color="#9467bd",
            marker="o",
            s=30,
            label="Fast Retransmit",
            zorder=3,
        )

    ax1.set_title(
        "LiteTCP Transport Layer Reliability & Congestion Control Metrics (TCP Reno)",
        fontsize=13,
        fontweight="bold",
        pad=10,
    )
    ax1.set_ylabel("Window Size (bytes)", fontsize=10)
    ax1.grid(True, linestyle=":", alpha=0.6)
    ax1.legend(loc="upper right", frameon=True, facecolor="white", edgecolor="none")

    # ------------------ Subplot 2: Flow Control (rwnd) ------------------
    ax2.plot(
        timestamps,
        rwnd,
        label="rwnd (Receiver Advertised Window)",
        color="#2ca02c",
        linewidth=1.8,
    )
    ax2.fill_between(timestamps, rwnd, color="#2ca02c", alpha=0.08)

    zwp_t, zwp_v = [], []
    for t, r, e in zip(timestamps, rwnd, events):
        if "zero_window_probe" in e:
            zwp_t.append(t)
            zwp_v.append(r)
    if zwp_t:
        ax2.scatter(
            zwp_t,
            zwp_v,
            color="#e377c2",
            marker="^",
            s=35,
            label="Zero Window Probe (ZWP)",
            zorder=3,
        )

    ax2.set_ylabel("Flow Ctrl Window (bytes)", fontsize=10)
    ax2.grid(True, linestyle=":", alpha=0.6)
    ax2.legend(loc="upper right")

    # ------------------ Subplot 3: Retransmission Timeout (RTO) ------------------
    ax3.step(timestamps, rto, label="Dynamic RTO", color="#d62728", where="post")
    ax3.fill_between(timestamps, rto, color="#d62728", alpha=0.05)
    ax3.set_ylabel("Timeout (seconds)", fontsize=10)
    ax3.grid(True, linestyle=":", alpha=0.6)
    ax3.legend(loc="upper right")

    # ------------------ Subplot 4: Out-of-Order Queue (ooo_count) ------------------
    ax4.step(
        ooo_timestamps,
        ooo_count,
        label="Out-of-Order Intervals (ooo_count)",
        color="#9467bd",
        where="post",
        linewidth=1.8,
    )
    ax4.fill_between(
        ooo_timestamps, ooo_count, color="#9467bd", alpha=0.08, step="post"
    )
    ax4.set_xlabel("Time (seconds)", fontsize=10)
    ax4.set_ylabel("Discontinuous Intervals", fontsize=10)
    ax4.grid(True, linestyle=":", alpha=0.6)
    ax4.legend(loc="upper right")
    ax4.set_xlim(0, max(timestamps))

    plt.tight_layout()

    # 导出整合后的全景可靠性机制分析图表
    plt.savefig(os.path.join(search_dir, "congestion_metrics.png"), dpi=300)
    plt.close()
    print("[Success] Re-generated comprehensive professional plots safely.")


if __name__ == "__main__":
    plot_advanced_metrics()
