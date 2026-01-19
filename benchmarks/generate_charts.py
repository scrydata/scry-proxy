#!/usr/bin/env python3
"""Generate comparison charts from benchmark results."""

import json
import sys
from pathlib import Path
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np


def load_results(results_dir: Path) -> list[dict]:
    """Load all JSON result files from a directory."""
    results = []
    for path in results_dir.glob("*.json"):
        with open(path) as f:
            results.append(json.load(f))
    return results


def group_by_connections(results: list[dict]) -> dict[int, list[dict]]:
    """Group results by connection count."""
    grouped = defaultdict(list)
    for r in results:
        conn = r["config"]["connections"]
        grouped[conn].append(r)
    return dict(sorted(grouped.items()))


def group_by_proxy(results: list[dict]) -> dict[str, list[dict]]:
    """Group results by proxy name."""
    grouped = defaultdict(list)
    for r in results:
        grouped[r["label"]].append(r)
    return grouped


def plot_latency_comparison(results: list[dict], output_path: Path):
    """Generate latency percentile comparison bar chart."""
    # Group by label, pick a representative connection count (50)
    by_proxy = group_by_proxy(results)

    labels = []
    p50s = []
    p95s = []
    p99s = []

    for proxy, proxy_results in sorted(by_proxy.items()):
        # Find result with 50 connections, or closest
        target = 50
        closest = min(proxy_results, key=lambda r: abs(r["config"]["connections"] - target))
        labels.append(proxy)
        p50s.append(closest["latency_us"]["p50"])
        p95s.append(closest["latency_us"]["p95"])
        p99s.append(closest["latency_us"]["p99"])

    x = np.arange(len(labels))
    width = 0.25

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(x - width, p50s, width, label='p50', color='#2ecc71')
    ax.bar(x, p95s, width, label='p95', color='#f39c12')
    ax.bar(x + width, p99s, width, label='p99', color='#e74c3c')

    ax.set_ylabel('Latency (microseconds)')
    ax.set_title('Latency Comparison (50 connections)')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path / "latency_comparison.png", dpi=150)
    plt.close()
    print(f"Generated: {output_path / 'latency_comparison.png'}")


def plot_latency_vs_connections(results: list[dict], output_path: Path):
    """Generate latency vs connection count line chart."""
    by_proxy = group_by_proxy(results)

    fig, ax = plt.subplots(figsize=(12, 6))

    colors = plt.cm.tab10(np.linspace(0, 1, len(by_proxy)))

    for (proxy, proxy_results), color in zip(sorted(by_proxy.items()), colors):
        sorted_results = sorted(proxy_results, key=lambda r: r["config"]["connections"])
        conns = [r["config"]["connections"] for r in sorted_results]
        p99s = [r["latency_us"]["p99"] for r in sorted_results]
        ax.plot(conns, p99s, marker='o', label=proxy, color=color, linewidth=2)

    ax.set_xlabel('Concurrent Connections')
    ax.set_ylabel('p99 Latency (microseconds)')
    ax.set_title('p99 Latency vs Connection Count')
    ax.legend()
    ax.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path / "latency_vs_connections.png", dpi=150)
    plt.close()
    print(f"Generated: {output_path / 'latency_vs_connections.png'}")


def plot_throughput_comparison(results: list[dict], output_path: Path):
    """Generate throughput comparison bar chart."""
    by_proxy = group_by_proxy(results)

    labels = []
    throughputs = []

    for proxy, proxy_results in sorted(by_proxy.items()):
        target = 50
        closest = min(proxy_results, key=lambda r: abs(r["config"]["connections"] - target))
        labels.append(proxy)
        throughputs.append(closest["throughput_qps"])

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(labels, throughputs, color='#3498db')

    # Add value labels on bars
    for bar, val in zip(bars, throughputs):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50,
                f'{val:.0f}', ha='center', va='bottom', fontsize=9)

    ax.set_ylabel('Throughput (queries/second)')
    ax.set_title('Throughput Comparison (50 connections)')
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path / "throughput_comparison.png", dpi=150)
    plt.close()
    print(f"Generated: {output_path / 'throughput_comparison.png'}")


def plot_throughput_vs_connections(results: list[dict], output_path: Path):
    """Generate throughput vs connection count line chart."""
    by_proxy = group_by_proxy(results)

    fig, ax = plt.subplots(figsize=(12, 6))

    colors = plt.cm.tab10(np.linspace(0, 1, len(by_proxy)))

    for (proxy, proxy_results), color in zip(sorted(by_proxy.items()), colors):
        sorted_results = sorted(proxy_results, key=lambda r: r["config"]["connections"])
        conns = [r["config"]["connections"] for r in sorted_results]
        throughputs = [r["throughput_qps"] for r in sorted_results]
        ax.plot(conns, throughputs, marker='o', label=proxy, color=color, linewidth=2)

    ax.set_xlabel('Concurrent Connections')
    ax.set_ylabel('Throughput (queries/second)')
    ax.set_title('Throughput vs Connection Count')
    ax.legend()
    ax.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path / "throughput_vs_connections.png", dpi=150)
    plt.close()
    print(f"Generated: {output_path / 'throughput_vs_connections.png'}")


def generate_summary_table(results: list[dict], output_path: Path):
    """Generate a markdown summary table."""
    by_conn = group_by_connections(results)

    lines = ["# Benchmark Results Summary\n"]

    for conn, conn_results in by_conn.items():
        lines.append(f"\n## {conn} Connections\n")
        lines.append("| Proxy | p50 (μs) | p95 (μs) | p99 (μs) | Throughput (qps) |")
        lines.append("|-------|----------|----------|----------|------------------|")

        for r in sorted(conn_results, key=lambda x: x["label"]):
            lines.append(
                f"| {r['label']} | {r['latency_us']['p50']} | "
                f"{r['latency_us']['p95']} | {r['latency_us']['p99']} | "
                f"{r['throughput_qps']:.0f} |"
            )

    summary_path = output_path / "SUMMARY.md"
    with open(summary_path, "w") as f:
        f.write("\n".join(lines))
    print(f"Generated: {summary_path}")


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <results_directory>")
        sys.exit(1)

    results_dir = Path(sys.argv[1])
    if not results_dir.exists():
        print(f"Error: Directory not found: {results_dir}")
        sys.exit(1)

    print(f"Loading results from: {results_dir}")
    results = load_results(results_dir)

    if not results:
        print("No results found!")
        sys.exit(1)

    print(f"Found {len(results)} result files")

    # Generate all charts
    plot_latency_comparison(results, results_dir)
    plot_latency_vs_connections(results, results_dir)
    plot_throughput_comparison(results, results_dir)
    plot_throughput_vs_connections(results, results_dir)
    generate_summary_table(results, results_dir)

    print("\nAll charts generated successfully!")


if __name__ == "__main__":
    main()
