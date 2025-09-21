import threading
import uuid
from collections import OrderedDict, defaultdict
from typing import Dict, List, Tuple


class LamportClock:
    def __init__(self):
        self.c = 0
        self._lock = threading.Lock()

    def tick(self) -> int:
        with self._lock:
            self.c += 1
            return self.c

    def send(self) -> int:
        return self.tick()

    def recv(self, incoming: int) -> int:
        with self._lock:
            self.c = max(self.c, incoming) + 1
            return self.c

    def now(self) -> int:
        with self._lock:
            return self.c


class DistributedCounter:
    """
    - 每个节点维护
      - value: 当前计数值
      - clock: LamportClock
      - log_store: {op_id -> entry}  去重存储
      - applied: 已应用的 op_id 集合
    - entry 结构: {
        "op_id": str,                 # 唯一ID
        "node": str,                  # 产生该操作的节点
        "delta": int,                 # 加多少
        "ts": int,                    # Lamport 时间戳
      }
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.clock = LamportClock()
        self.value = 0
        self.log_store: "OrderedDict[str, dict]" = OrderedDict()
        self.applied = set()
        self._peers: Dict[str, "DistributedCounter"] = {}
        self._blocked: Dict[str, bool] = defaultdict(
            lambda: False
        )  # 分区：对某个peer是否阻断

        # 记录一次“全量重放”时采用的全序
        self._last_replay_order: List[Tuple[int, str, str]] = []

    # --- wiring / partition control ---
    def attach_peers(self, peers: Dict[str, "DistributedCounter"]):
        """把 id->node 的映射传进来，通常所有节点互连。"""
        self._peers = {k: v for k, v in peers.items() if k != self.node_id}

    def block_link(self, peer_id: str, blocked: bool = True):
        """模拟网络分区：阻断与 peer 的单向连线。"""
        self._blocked[peer_id] = blocked

    def is_blocked(self, peer_id: str) -> bool:
        return self._blocked.get(peer_id, False)

    # --- core operations ---
    def _order_key(self, entry: dict) -> Tuple[int, str, str]:
        """确定性全序键：Lamport ts -> node_id -> op_id。"""
        return (entry["ts"], entry["node"], entry["op_id"])

    def _append_local_log(self, entry: dict):
        """本地持久化日志（去重）。"""
        if entry["op_id"] not in self.log_store:
            self.log_store[entry["op_id"]] = entry

    def _broadcast(self, entry: dict):
        """尽力广播（遇到分区则丢弃；日志仍在本地，等 sync() 再补齐）。"""
        for pid, peer in self._peers.items():
            if self.is_blocked(pid):
                continue
            # 模拟“收消息即合并时钟”的效果
            peer.receive_entry(entry)

    def increment(self, k: int = 1) -> str:
        """生成操作，写本地日志，尝试广播。"""
        ts = self.clock.send()
        op_id = f"{self.node_id}-{uuid.uuid4().hex[:8]}"
        entry = {
            "op_id": op_id,
            "node": self.node_id,
            "delta": int(k),
            "ts": ts,
        }
        # 1) 本地追加日志
        self._append_local_log(entry)
        # 2) 可立即应用（加法可交换；若操作不可交换，可选择只在“确定全序”后应用）
        self.apply_if_ready()
        # 3) 广播
        self._broadcast(entry)
        return op_id

    def receive_entry(self, entry: dict):
        """接收远端日志，合并 Lamport 并存储去重，然后尝试应用。"""
        self.clock.recv(entry["ts"])
        self._append_local_log(entry)
        self.apply_if_ready()

    def apply_if_ready(self):
        """
        这里的“就绪”很宽松：计数加法可交换，因此只要没应用过就应用。
        若要严格按全序应用，可在 sync() 里统一“排序重放”。
        """
        for op_id, e in list(self.log_store.items()):
            if op_id in self.applied:
                continue
            self.value += e["delta"]
            self.applied.add(op_id)

    # --- anti-entropy / resync ---
    def get_log_snapshot(self) -> List[dict]:
        return list(self.log_store.values())

    def merge_logs(self, entries: List[dict]):
        changed = False
        for e in entries:
            if e["op_id"] not in self.log_store:
                self.log_store[e["op_id"]] = e
                changed = True
                # 合并 Lamport
                self.clock.recv(e["ts"])
        if changed:
            # 统一重放：对不可交换操作尤其重要；对加法是幂等的再确认
            self.replay_in_global_order()

    def replay_in_global_order(self):
        """用全序键重放一遍（幂等：只对未应用项生效）。"""
        ordered = sorted(self.log_store.values(), key=self._order_key)
        self._last_replay_order = [self._order_key(e) for e in ordered]
        for e in ordered:
            if e["op_id"] in self.applied:
                continue
            self.value += e["delta"]
            self.applied.add(e["op_id"])

    def sync(self):
        """
        反熵：与可达的 peers 互换日志，归并后按全序重放。
        可以多次调用；网络恢复后最终一致。
        """
        for pid, peer in self._peers.items():
            if self.is_blocked(pid):
                continue
            # 双向交换
            self.merge_logs(peer.get_log_snapshot())
            peer.merge_logs(self.get_log_snapshot())

    # --- helpers for demo ---
    def __repr__(self):
        return f"Node({self.node_id}): val={self.value} applied={len(self.applied)} log={len(self.log_store)}"

    def debug_status(self) -> str:
        return (
            f"[{self.node_id}] value={self.value} "
            f"applied={len(self.applied)} "
            f"log={len(self.log_store)} "
            f"clock={self.clock.now()}"
        )

    def debug_last_order(self) -> List[Tuple[int, str, str]]:
        return self._last_replay_order[:]


if __name__ == "__main__":
    # 创建 3 个节点并互连
    n0, n1, n2 = (
        DistributedCounter("n0"),
        DistributedCounter("n1"),
        DistributedCounter("n2"),
    )
    peers = {"n0": n0, "n1": n1, "n2": n2}
    for n in peers.values():
        n.attach_peers(peers)

    # 模拟网络分区：n1 <-> n2 断开
    n1.block_link("n2", True)
    n2.block_link("n1", True)

    # 各自并发 increment
    n0.increment(1)  # +1
    n1.increment(2)  # +2
    n2.increment(3)  # +3
    print("分区期间：")
    print(n0.debug_status())
    print(n1.debug_status())
    print(n2.debug_status())

    # 恢复网络
    n1.block_link("n2", False)
    n2.block_link("n1", False)

    # 反熵同步（可多次调用；此处一次足够）
    n0.sync()
    n1.sync()
    n2.sync()

    print("\n网络恢复并 sync 后：")
    print(n0.debug_status(), "order:", n0.debug_last_order())
    print(n1.debug_status(), "order:", n1.debug_last_order())
    print(n2.debug_status(), "order:", n2.debug_last_order())

    # 所有节点应收敛到相同的 value = 1+2+3 = 6
    assert n0.value == n1.value == n2.value == 6
    print("\n所有节点达成一致：", n0.value)
