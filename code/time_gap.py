import time
import random
import threading
import uuid


class HybridLogicalClock:
    """HLC = (pt, lc), pt=physical ms, lc=logical counter."""

    def __init__(self, skew_ms=0):
        self.pt = 0
        self.lc = 0
        self.skew_ms = skew_ms
        self._lock = threading.Lock()

    def _now_ms(self):
        return int(time.time() * 1000) + self.skew_ms

    def tick(self):
        """Local event update, return (pt, lc)."""
        with self._lock:
            now = self._now_ms()
            self.pt = max(self.pt, now)
            if self.pt == now:
                self.lc += 1
            else:
                self.lc = 0
            return (self.pt, self.lc)

    def send(self):
        """Call before attaching to outgoing message."""
        return self.tick()

    def recv(self, msg_ts):
        """Merge incoming (pt, lc) and return updated (pt, lc)."""
        # msg_ts could be a tuple or a dict containing HLC
        if isinstance(msg_ts, dict):
            if "hlc" in msg_ts:
                mpt, mlc = msg_ts["hlc"]
            elif "hlc_timestamp" in msg_ts:
                mpt, mlc = msg_ts["hlc_timestamp"]
            elif "timestamp" in msg_ts:
                mpt, mlc = msg_ts["timestamp"]
            else:
                raise ValueError("Incoming message missing HLC.")
        else:
            mpt, mlc = msg_ts

        with self._lock:
            now = self._now_ms()
            new_pt = max(self.pt, mpt, now)
            if new_pt == mpt == now:
                new_lc = max(self.lc, mlc) + 1
            elif new_pt == mpt:
                new_lc = mlc + 1
            elif new_pt == now:
                new_lc = self.lc + 1
            else:
                new_lc = 0
            self.pt, self.lc = new_pt, new_lc
            return (self.pt, self.lc)

    def current(self):
        with self._lock:
            return (self.pt, self.lc)


class Network:
    def __init__(self, drop_prob=0.1, delay_range=(0.01, 0.1)):
        self.nodes = {}
        self.drop_prob = drop_prob
        self.delay_range = delay_range

    def register(self, node):
        self.nodes[node.node_id] = node

    def send(self, message):
        src = message["from"]
        dst = message["to"]
        delay = random.uniform(*self.delay_range)

        def _deliver():
            time.sleep(delay)
            if random.random() < self.drop_prob:
                print(f"网络失败：{src} -> {dst} (drop)")
                return
            target = self.nodes.get(dst)
            if target is None:
                print(f"投递失败：目标节点不存在 {dst}")
                return
            target.receive_message(message)

        threading.Thread(target=_deliver, daemon=True).start()
        return delay


class DistributedRPCSystem:
    def __init__(self, node_id, network: Network, skew_ms=0):
        self.node_id = node_id
        self.hlc = HybridLogicalClock(skew_ms=skew_ms)  # 偏差
        self._skew_ms = skew_ms
        self.network = network
        self.log = []
        self.message_log = []  # 记录所有消息（发送/接收）
        self.request_history = set()  # 防重复的请求ID
        self.network.register(self)

    def _log_send(self, message, delay):
        self.message_log.append(
            {
                "action": "send",
                "message": message,
                "wall_time": self.wall_time(),
                "hlc_time": message["hlc"],  # 发送时所附带的 HLC
                "delay": delay,
            }
        )

    def _log_recv(self, message, recv_hlc):
        self.message_log.append(
            {
                "action": "receive",
                "message": message,
                "wall_time": self.wall_time(),
                "hlc_time": recv_hlc,  # 接收端合并后的 HLC
            }
        )

    def call_remote(self, target_node, method, args):
        """发起远程调用（request）。"""
        request_id = str(uuid.uuid4())
        hlc_ts = self.hlc.send()  # 发送前 tick

        request = {
            "id": request_id,
            "from": self.node_id,
            "to": target_node,
            "method": method,
            "args": args,
            "hlc": hlc_ts,
            "type": "request",
        }

        delay = self.network.send(request)
        print(
            f"[SEND] {self.node_id} -> {target_node} | {method} | HLC={hlc_ts} | 延迟≈{delay:.3f}s"
        )
        self._log_send(request, delay)
        return request_id

    def receive_message(self, message):
        """接收消息（统一入口：request/response）。"""
        # 幂等/去重（针对 request）
        if message["type"] == "request" and message["id"] in self.request_history:
            print(f"[DUP] {self.node_id} 检测到重复请求：{message['id']}")
            return

        # HLC 合并
        recv_hlc = self.hlc.recv(message["hlc"])
        self._log_recv(message, recv_hlc)

        # 标记已处理（仅记录 request 去重；response 在此示例不去重）
        if message["type"] == "request":
            self.request_history.add(message["id"])

        # 分派处理
        if message["type"] == "request":
            self.process_request(message)
        elif message["type"] == "response":
            self.process_response(message)

    def process_request(self, request):
        """处理 request 并发送 response。"""
        method = request["method"]
        args = request["args"]
        print(f"[PROC] {self.node_id} 处理请求：{method}({args})")

        # 模拟处理时间
        time.sleep(random.uniform(0.01, 0.05))

        # 构造响应（发送前 tick）
        response_hlc = self.hlc.send()
        response = {
            "id": str(uuid.uuid4()),
            "from": self.node_id,
            "to": request["from"],
            "original_request_id": request["id"],
            "result": f"处理结果：{method}({args})",
            "hlc": response_hlc,
            "type": "response",
        }

        delay = self.network.send(response)
        print(
            f"[RESP] {self.node_id} -> {request['from']} | HLC={response_hlc} | 延迟≈{delay:.3f}s"
        )
        self._log_send(response, delay)

    def process_response(self, response):
        """处理 response（演示中仅打印）。"""
        print(
            f"[RECV-RESP] {self.node_id} 收到响应 for {response.get('original_request_id')} | HLC={self.hlc.current()}"
        )

    def wall_time(self):
        """节点自己的墙钟 = 系统时间 + 偏差"""
        return time.time() + self._skew_ms / 1000.0

    # 可扩展：本地业务操作
    def local_event(self, note="local"):
        ts = self.hlc.tick()
        self.log.append((note, ts))
        return ts


def demonstrate_time_gaps():
    net = Network(drop_prob=0.10, delay_range=(0.01, 0.10))

    # 创建三个节点
    skews = {"node0": -150, "node1": +150, "node2": +50}  # 单位: ms
    nodes = {}
    for i in range(3):
        nid = f"node{i}"
        nodes[nid] = DistributedRPCSystem(nid, net, skew_ms=skews[nid])

    # 并发发送请求，观察时间顺序
    def send_requests():
        # 节点0向节点1发送转账请求
        nodes["node0"].call_remote(
            "node1", "transfer", {"amount": 100, "to": "account_B"}
        )
        time.sleep(0.05)

        # 节点1向节点2发送查询请求
        nodes["node1"].call_remote("node2", "query_balance", {"account": "account_B"})
        time.sleep(0.05)

        # 节点2向节点0发送审计请求
        nodes["node2"].call_remote("node0", "audit", {"transaction_id": "tx_123"})

    t = threading.Thread(target=send_requests)
    t.start()
    t.join()

    # 等待所有可能的传输/处理完成
    time.sleep(0.6)

    all_messages = []
    for node_id, node in nodes.items():
        for msg_info in node.message_log:
            all_messages.append((node_id, msg_info))

    # ---- A) 本地时间排序 ----
    print("\n=== 本地时间排序（按 wall_time） ===")
    naive = sorted(all_messages, key=lambda x: x[1]["wall_time"])
    for node_id, msg_info in naive:
        t = msg_info["wall_time"]
        action = msg_info["action"]
        kind = msg_info["message"].get("method") or msg_info["message"].get("type")
        print(f"wall {t:.6f} | {node_id} {action} {kind}")

    # ---- B) 按 HLC 顺序输出 ----
    print("\n=== 消息时序分析（按 HLC 排序） ===")
    # 加入 node_id 作为并列时的稳定 tie-breaker
    all_messages.sort(key=lambda x: (x[1]["hlc_time"], x[0], x[1]["action"]))
    for node_id, msg_info in all_messages:
        hlc_time = msg_info["hlc_time"]
        action = msg_info["action"]
        message = msg_info["message"]
        kind = message.get("method") or message.get("type")
        delay = msg_info.get("delay")
        if action == "send":
            print(
                f"HLC {hlc_time} | {node_id} {action} {kind} -> {message['to']} (delay≈{delay:.3f}s)"
            )
        else:
            print(f"HLC {hlc_time} | {node_id} {action} {kind} <- {message['from']}")

    # ---- C) 逐消息ID成对检查 send < recv ----
    print("\n=== 因果校验（send < recv） ===")
    # 对 request 用 id 成对，对 response 用其独立 id 成对
    send_map = {}
    for node_id, mi in all_messages:
        m = mi["message"]
        if mi["action"] == "send":
            send_map.setdefault(m["id"], []).append(mi["hlc_time"])
    for node_id, mi in all_messages:
        m = mi["message"]
        if mi["action"] == "receive":
            sid = m["id"]
            if sid not in send_map:
                print(f"[WARN] 找不到发送端：{sid}")
                continue
            # 只要有任意一次发送，保证 send_hlc < recv_hlc
            recv_hlc = mi["hlc_time"]
            ok = any(sh < recv_hlc for sh in send_map[sid])
            print(
                f"{sid[:8]}... send<recv ? {'OK' if ok else 'FAIL'}  (recv={recv_hlc}, sends={send_map[sid]})"
            )


if __name__ == "__main__":
    demonstrate_time_gaps()
