#!/usr/bin/env python3
"""
实验：模拟分布式计数器
目标：通过对比，理解分布式环境下操作的复杂性、常见问题及解决方案。
"""

import threading
import time
import random
import logging

# 配置日志，方便观察并发执行过程
logging.basicConfig(
    level=logging.INFO,
    format="(%(threadName)-10s) %(message)s",
)


class DistributedCounter:
    """分布式计数器模拟"""

    def __init__(self, nodes=3, method="operation"):
        self.nodes = [0] * nodes
        # 这个锁是Python多线程编程中为了保护 self.nodes 列表线程安全的“本地锁”。
        # 它绝不代表一个“分布式锁”。在真实的分布式系统中，没有这种跨所有节点的共享内存和全局锁。
        self.lock = threading.Lock()
        self.network_delay = (0.01, 0.05)  # 模拟网络延迟范围
        self.failure_rate = 0.1  # 模拟网络或节点故障的概率
        self.replicate_method = method

    def _simulate_network(self, operation_name, from_node, to_node=None):
        """模拟网络延迟和故障，并打印日志"""
        time.sleep(random.uniform(*self.network_delay))
        if random.random() < self.failure_rate:
            if to_node is not None:
                logging.warning(
                    f"网络故障: 节点 {from_node} -> 节点 {to_node} 的 {operation_name} 失败!"
                )
            else:
                logging.warning(f"网络故障: 节点 {from_node} 的 {operation_name} 失败!")
            return False
        return True

    def increment(self, node_id):
        """
        一个外部请求到达某个节点，使其计数器加一。
        分为两步：
        1. 本地增加计数。
        2. 将变更“异步”地复制到其他节点。
        """
        logging.info(f"节点 {node_id} 收到 increment 请求。")

        # 1. 本地自增 (原子操作)
        with self.lock:
            initial_value = self.nodes[node_id]
            self.nodes[node_id] += 1
            logging.info(
                f"节点 {node_id} 本地计数: {initial_value} -> {self.nodes[node_id]}"
            )

        # 2. 将变更异步复制到其他节点
        for i in range(len(self.nodes)):
            if i != node_id:
                # 使用下面的两种同步方法之一
                if self.replicate_method == "state":
                    method = self.replicate_state
                else:
                    method = self.replicate_operation

                threading.Thread(
                    target=method, args=(node_id, i), name=f"Sync-{node_id}to{i}"
                ).start()

    # --- 状态复制 ---
    def replicate_state(self, from_node, to_node):
        """
        直接用我的值覆盖你的值。
        这是导致“更新丢失”问题的根源。
        """
        if not self._simulate_network("状态复制", from_node, to_node):
            return

        with self.lock:
            val_from = self.nodes[from_node]
            val_to_before = self.nodes[to_node]
            # 无论对方的值是多少，都强制用我的值覆盖
            self.nodes[to_node] = val_from
            logging.info(
                f"状态复制: 节点 {from_node} ({val_from}) -> 节点 {to_node} ({val_to_before})，更新后节点 {to_node} 值为 {self.nodes[to_node]}"
            )

    # --- 操作复制 ---
    def replicate_operation(self, from_node, to_node):
        """
        告诉其他节点“我做了一次加一操作”而非“我的新值是X”。
        可以避免更新丢失问题。
        """
        if not self._simulate_network("操作复制", from_node, to_node):
            return

        with self.lock:
            val_to_before = self.nodes[to_node]
            # 将“加一”这个操作在另一个节点上重放
            self.nodes[to_node] += 1
            logging.info(
                f"操作复制: 节点 {from_node} 通知 节点 {to_node} 执行 increment。节点 {to_node} 计数: {val_to_before} -> {self.nodes[to_node]}"
            )

    def get_nodes_state(self):
        """获取所有节点当前的状态"""
        with self.lock:
            return list(self.nodes)


# --- 演示场景 ---


def demo_1(requests=20, nodes=3, replicate_method="operation"):
    """演示1: 数据不一致性 和 最终一致性"""
    print("展示在并发更新下，不同节点的数据会短暂不一致，但最终会趋于一致。")

    counter = DistributedCounter(nodes, method=replicate_method)
    # 演示刻意只让一个节点接收请求，以简化复制过程，突出不一致性
    threads = []
    for _ in range(requests):
        t = threading.Thread(target=counter.increment, args=(0,), name=f"Request-{_}")
        threads.append(t)
        t.start()

    # 在操作过程中，多次查看状态，观察不一致
    for i in range(3):
        time.sleep(0.1)
        print(f"中间状态 {i + 1}: {counter.get_nodes_state()}")

    for t in threads:
        t.join()

    # 等待所有异步的同步线程完成
    time.sleep(0.5)
    print(f"预期结果: 所有节点值都为 {requests} (如果网络没有故障)")
    print(f"最终状态: {counter.get_nodes_state()}")


def demo_2(nodes=2):
    """演示2: 更新丢失问题"""
    print("展示当两个节点并发更新并使用“状态复制”时，一个更新会被另一个覆盖。")

    # 为了清晰地展示问题，用一个特殊的计数器，只使用有问题的复制方法
    class BadCounter(DistributedCounter):
        def increment(self, node_id):
            with self.lock:
                self.nodes[node_id] += 1
                logging.info(f"节点 {node_id} 本地计数变为 {self.nodes[node_id]}")

            for i in range(len(self.nodes)):
                if i != node_id:
                    threading.Thread(
                        target=self.replicate_state,
                        args=(node_id, i),
                        name=f"Sync-{node_id}to{i}",
                    ).start()

    counter = BadCounter(nodes)
    print(f"初始状态: {counter.get_nodes_state()}")

    # 节点0 和 节点1 同时加一
    t1 = threading.Thread(target=counter.increment, args=(0,), name="Req-Node0")
    t2 = threading.Thread(target=counter.increment, args=(1,), name="Req-Node1")
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # 等待同步完成
    time.sleep(0.5)

    print("预期结果: [2, 2]")
    print(f"实际最终状态: {counter.get_nodes_state()}")


def demo_3(nodes=2):
    """演示3: 使用操作复制解决更新丢失问题"""
    print("展示通过复制“操作”而非“状态”，可以正确处理并发更新。")

    class GoodCounter(DistributedCounter):
        def increment(self, node_id):
            with self.lock:
                self.nodes[node_id] += 1
                logging.info(f"节点 {node_id} 本地计数变为 {self.nodes[node_id]}")

            for i in range(len(self.nodes)):
                if i != node_id:
                    threading.Thread(
                        target=self.replicate_operation,
                        args=(node_id, i),
                        name=f"Sync-{node_id}to{i}",
                    ).start()

    counter = GoodCounter(nodes)
    print(f"初始状态: {counter.get_nodes_state()}")

    # 节点0 和 节点1 同时加一
    t1 = threading.Thread(target=counter.increment, args=(0,), name="Req-Node0")
    t2 = threading.Thread(target=counter.increment, args=(1,), name="Req-Node1")
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # 等待同步完成
    time.sleep(0.5)

    print("预期结果: [2, 2]")
    print(f"实际最终状态: {counter.get_nodes_state()}")


if __name__ == "__main__":
    demo_1(replicate_method="operation")
    # demo_2()
    # demo_3()
    print("结束演示")
