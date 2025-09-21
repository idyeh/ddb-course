"""
Raft 青春版
重点：选举、心跳、日志复制（逐条）、多数派提交、简单状态机
"""

from __future__ import annotations
from dataclasses import dataclass
import random
from typing import List, Dict, Optional, Tuple

# ========= 演示日志开关 =========
# 总闸
DEBUG = True

# 分闸
DBG = {
    "election": True,  # 选举/角色切换
    "send_append": True,  # Leader 侧发送 AppendEntries
    "follower_append": True,  # Follower 侧收到并应答 AppendEntries
    "append_resp": True,  # Leader 侧收到 APPEND_RESP
    "apply": True,  # 状态机应用
}


def dlog(cat: str, s: str):
    """按类目和总开关控制"""
    if DEBUG and DBG.get(cat, False):
        print(s, flush=True)


# ========= 演示参数 =========
SEED = 2025
N = 5  # 节点数（奇数便于产生多数派）
HEARTBEAT_EVERY = 4  # Leader 心跳间隔（tick）
ELECTION_RANGE = (12, 20)  # 选举超时范围（tick）
NET_DELAY_RANGE = (2, 4)  # 消息网络延迟（tick）
DROP_RATE = 0.0  # 丢包率（例如0.1）
T_MAX = 220  # 总仿真步数

# 在选出 Leader 后，注入 1 笔转账命令；随后故意整死 Leader 再看重选
CRASH_LEADER_AT = 120
RECOVER_LEADER_AT = 170

random.seed(SEED)


# ========= Raft 日志条目与消息 =========
@dataclass
class Entry:
    term: int
    cmd: str  # 例如 "transfer A B 100"


# 消息用字典结构
def msg_request_vote(term, cand_id, last_term, last_idx):
    return {
        "type": "RV",
        "term": term,
        "cand": cand_id,
        "last_term": last_term,
        "last_idx": last_idx,
    }


def msg_request_vote_resp(term, voter_id, granted):
    return {"type": "RV_RESP", "term": term, "voter": voter_id, "granted": granted}


def msg_append(
    term, leader_id, prev_idx, prev_term, entries: List[Entry], leader_commit
):
    # 只发 0/1 条，entries 可序列化
    return {
        "type": "APPEND",
        "term": term,
        "leader": leader_id,
        "prev_idx": prev_idx,
        "prev_term": prev_term,
        "entries": entries,
        "leader_commit": leader_commit,
    }


def msg_append_resp(term, from_id, success, match_index):
    return {
        "type": "APPEND_RESP",
        "term": term,
        "from": from_id,
        "success": success,
        "match_index": match_index,
    }


# ========= 网络 =========
class Network:
    def __init__(self):
        self.q: List[Tuple[int, int, dict]] = []  # (deliver_tick, to_id, msg)

    def send(self, now_tick: int, to_id: int, msg: dict):
        if random.random() < DROP_RATE:
            return  # 丢包
        delay = random.randint(*NET_DELAY_RANGE)
        self.q.append((now_tick + delay, to_id, msg))

    def deliver(self, now_tick: int) -> List[Tuple[int, dict]]:
        """返回应在 now_tick 投递的消息: [(to_id, msg), ...]"""
        ready, rest = [], []
        for t, to_id, msg in self.q:
            if t <= now_tick:
                ready.append((to_id, msg))
            else:
                rest.append((t, to_id, msg))
        self.q = rest
        return ready


# ========= Raft 节点 =========
class Node:
    def __init__(self, i: int, peers: List[int], net: Network):
        self.id = i
        self.peers = peers
        self.net = net

        # 角色
        self.state = "Follower"  # Follower / Candidate / Leader
        self.alive = True

        # 任期与投票
        self.current_term = 0
        self.voted_for: Optional[int] = None

        # 日志与提交
        self.log: List[Entry] = []  # [(term, cmd)]
        self.commit_index = -1
        self.last_applied = -1

        # Leader 专用复制状态
        self.next_index: Dict[int, int] = {}
        self.match_index: Dict[int, int] = {}

        # 计时
        self.rand_election_reset(0)
        self.last_heartbeat_seen = 0
        self.heartbeat_tick = 0

        # 状态机（银行账户）
        self.state_machine = {"A": 1000, "B": 1000}

        # 统计
        self.votes_granted = 0

    # ---- 工具 ----
    def majority(self) -> int:
        return len(self.peers) // 2 + 1

    def last_log_index(self) -> int:
        return len(self.log) - 1

    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else -1

    def rand_election_reset(self, now_tick: int):
        self.election_deadline = now_tick + random.randint(*ELECTION_RANGE)

    # ---- 角色切换 ----
    def become_follower(self, term: int):
        if self.state != "Follower":
            print(f"[Tick?] Node{self.id}: -> Follower (term={term})")
        self.state = "Follower"
        self.current_term = term
        self.voted_for = None

    def become_candidate(self, now_tick: int):
        self.state = "Candidate"
        self.current_term += 1
        self.voted_for = self.id
        self.votes_granted = 1  # 自投一票
        self.rand_election_reset(now_tick)
        print(f"[{now_tick}] Node{self.id}: -> Candidate term={self.current_term}")
        # 广播 RequestVote
        for peer in self.peers:
            if peer == self.id:
                continue
            self.net.send(
                now_tick,
                peer,
                msg_request_vote(
                    self.current_term,
                    self.id,
                    self.last_log_term(),
                    self.last_log_index(),
                ),
            )

    def become_leader(self, now_tick: int):
        self.state = "Leader"
        # 初始化复制指针
        last = self.last_log_index() + 1
        self.next_index = {p: last for p in self.peers}
        self.match_index = {p: -1 for p in self.peers}
        self.heartbeat_tick = now_tick
        dlog(
            "election",
            f"[{now_tick}] Node{self.id}: ⭐ LEADER elected (term={self.current_term})",
        )
        # 立刻发一轮心跳（空 Append）
        self.send_heartbeats(now_tick)

    # ---- Tick 驱动 ----
    def tick(self, now_tick: int):
        if not self.alive:
            return

        # 选举超时
        if self.state != "Leader" and now_tick >= self.election_deadline:
            self.become_candidate(now_tick)

        # Leader 发心跳 / 推进复制
        if self.state == "Leader" and now_tick - self.heartbeat_tick >= HEARTBEAT_EVERY:
            self.send_heartbeats(now_tick)
            self.heartbeat_tick = now_tick

        # 应用提交到状态机
        self.apply_commits(now_tick)

    # ---- 消息处理 ----
    def on_msg(self, now_tick: int, msg: dict):
        if not self.alive:
            return

        t = msg["term"]
        # 任期落后要追
        if t > self.current_term:
            self.become_follower(t)

        if msg["type"] == "RV":
            self.handle_request_vote(now_tick, msg)
        elif msg["type"] == "RV_RESP":
            if self.state == "Candidate" and t == self.current_term and msg["granted"]:
                self.votes_granted += 1
                if self.votes_granted >= self.majority():
                    self.become_leader(now_tick)
        elif msg["type"] == "APPEND":
            self.handle_append(now_tick, msg)
        elif msg["type"] == "APPEND_RESP":
            if self.state == "Leader" and t == self.current_term:
                self.handle_append_resp(now_tick, msg)

    # ---- RequestVote ----
    def handle_request_vote(self, now_tick: int, m: dict):
        cand_term = m["term"]
        cand = m["cand"]
        cand_last_term, cand_last_idx = m["last_term"], m["last_idx"]

        grant = False
        if cand_term < self.current_term:
            grant = False
        else:
            # 日志新鲜度：先比 term，再比 idx
            my_last_term, my_last_idx = self.last_log_term(), self.last_log_index()
            up_to_date = (cand_last_term > my_last_term) or (
                cand_last_term == my_last_term and cand_last_idx >= my_last_idx
            )
            free_or_same = self.voted_for in (None, cand)
            if up_to_date and free_or_same:
                self.voted_for = cand
                self.rand_election_reset(now_tick)
                grant = True
                # 若原本是 Candidate，成为 Follower
                if self.state != "Follower":
                    self.become_follower(cand_term)
        self.net.send(
            now_tick, cand, msg_request_vote_resp(self.current_term, self.id, grant)
        )

    # ---- AppendEntries ----
    def handle_append(self, now_tick: int, m: dict):
        leader_term = m["term"]
        leader_id = m["leader"]
        prev_idx, prev_term = m["prev_idx"], m["prev_term"]
        entries: List[Entry] = m["entries"]
        leader_commit = m["leader_commit"]

        # 心跳：看到 Leader 活着
        self.rand_election_reset(now_tick)
        if leader_term < self.current_term:
            dlog(
                "follower_append",
                f"[{now_tick}] Node{self.id}: REJECT APPEND from Leader{leader_id} "
                f"(term back {leader_term} < {self.current_term})",
            )
            self.net.send(
                now_tick,
                leader_id,
                msg_append_resp(
                    self.current_term, self.id, False, self.last_log_index()
                ),
            )
            return

        # Log Matching 检查
        if prev_idx >= 0:
            if prev_idx > self.last_log_index() or self.log[prev_idx].term != prev_term:
                # 不匹配：让 Leader 回退
                dlog(
                    "follower_append",
                    f"[{now_tick}] Node{self.id}: APPEND NACK (too short: have_last={self.last_log_index()}, "
                    f"need_prev={prev_idx})",
                )
                self.net.send(
                    now_tick,
                    leader_id,
                    msg_append_resp(
                        self.current_term, self.id, False, self.last_log_index()
                    ),
                )
                return
            if self.log[prev_idx].term != prev_term:
                # term 不匹配：要求回退
                dlog(
                    "follower_append",
                    f"[{now_tick}] Node{self.id}: APPEND NACK (term-mismatch at {prev_idx}: "
                    f"local={self.log[prev_idx].term} != prev_term={prev_term})",
                )
                self.net.send(
                    now_tick,
                    leader_id,
                    msg_append_resp(
                        self.current_term, self.id, False, self.last_log_index()
                    ),
                )
                return

        # 追加/覆盖冲突
        added = 0
        if entries:
            # 按顺序逐个处理（本演示每次最多 1 条）
            idx = prev_idx + 1
            ent = entries[0]
            # 覆盖冲突
            if self.last_log_index() >= idx and self.log[idx].term != ent.term:
                self.log = self.log[:idx]
            if self.last_log_index() < idx:
                self.log.append(ent)
                added = 1
            else:
                # 相同 term 则认为相同（幂等）
                pass

        # 推进提交
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.last_log_index())
        dlog(
            "follower_append",
            f"[{now_tick}] Node{self.id} -> Leader{leader_id}: "
            f"APPEND_RESP OK match={self.last_log_index()} (added={added}) commit={self.commit_index}",
        )
        self.net.send(
            now_tick,
            leader_id,
            msg_append_resp(self.current_term, self.id, True, self.last_log_index()),
        )

    def handle_append_resp(self, now_tick: int, m: dict):
        frm = m["from"]
        success = m["success"]
        match = m["match_index"]
        if success:
            old_next = self.next_index[frm]
            self.match_index[frm] = match
            self.next_index[frm] = match + 1
            dlog(
                "append_resp",
                f"[{now_tick}] Leader{self.id} <= Node{frm}: "
                f"APPEND_RESP success match={match} next_index: {old_next} -> {self.next_index[frm]}",
            )
            self.try_advance_commit(now_tick)
        else:
            old_next = self.next_index[frm]
            # 回退并重试（最小化实现：-1）
            self.next_index[frm] = max(0, self.next_index[frm] - 1)
            dlog(
                "append_resp",
                f"[{now_tick}] Leader{self.id} <= Node{frm}: "
                f"APPEND_RESP FAIL (follower_match={match}) next_index: {old_next} -> {self.next_index[frm]}",
            )
            # 立刻重发一次
            self.send_append_one(now_tick, frm)

    # ---- Leader：心跳与发送复制 ----
    def send_heartbeats(self, now_tick: int):
        if self.state != "Leader":
            return
        for p in self.peers:
            if p == self.id:
                continue
            self.send_append_one(now_tick, p)

    def send_append_one(self, now_tick: int, follower: int):
        prev_idx = self.next_index[follower] - 1
        prev_term = self.log[prev_idx].term if prev_idx >= 0 else -1
        # 逐条发：从 next_index 开始拿 1 条
        entries = []
        if self.next_index[follower] <= self.last_log_index():
            entries = [self.log[self.next_index[follower]]]
        dlog(
            "send_append",
            f"[{now_tick}] Leader{self.id} -> Node{follower}: "
            f"APPEND(prev={prev_idx}/{prev_term}, send={len(entries)}, leaderCommit={self.commit_index})",
        )
        self.net.send(
            now_tick,
            follower,
            msg_append(
                self.current_term,
                self.id,
                prev_idx,
                prev_term,
                entries,
                self.commit_index,
            ),
        )

    # ---- 多数派提交（仅当前任期） ----
    def try_advance_commit(self, now_tick: int):
        # 只推进当前任期的日志
        for i in range(self.last_log_index(), self.commit_index, -1):
            if self.log[i].term != self.current_term:
                continue
            # 统计有多少副本（含 Leader 自己）已匹配到 >= i
            cnt = 1  # self
            for p, m_idx in self.match_index.items():
                if p == self.id:  # 不会出现，但以防万一
                    continue
                if m_idx >= i:
                    cnt += 1
            if cnt >= self.majority():
                self.commit_index = i
                print(f"[{now_tick}] Leader{self.id}: commit -> {self.commit_index}")
                break

    # ---- 应用到状态机（银行转账） ----
    def apply_commits(self, now_tick: int):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            ent = self.log[self.last_applied]
            self.apply_cmd(ent.cmd, now_tick)

    def apply_cmd(self, cmd: str, now_tick: int):
        # 仅支持：transfer A B 100
        try:
            op, a, b, amt = cmd.split()
            amt = int(amt)
            if op != "transfer":
                return
            self.state_machine[a] -= amt
            self.state_machine[b] += amt
            if self.id == 0:  # 只让一个节点打印，避免刷屏
                dlog(
                    "apply",
                    f"[{now_tick}] Node{self.id} APPLY: {cmd} ; State={self.state_machine}",
                )
        except Exception:
            pass

    # ---- 客户端请求入口（仅 Leader 调用） ----
    def propose(self, now_tick: int, cmd: str):
        if self.state != "Leader" or not self.alive:
            return False
        self.log.append(Entry(self.current_term, cmd))
        # 自己等价于已匹配
        self.match_index[self.id] = self.last_log_index()
        # 立刻对各 Follower 发送一次
        for p in self.peers:
            if p == self.id:
                continue
            self.send_append_one(now_tick, p)
        print(
            f"[{now_tick}] Leader{self.id}: propose -> {cmd} (idx={self.last_log_index()})"
        )
        return True


# ========= 仿真驱动 =========
def run_demo():
    net = Network()
    nodes = [Node(i, list(range(N)), net) for i in range(N)]

    # 演示脚本控制
    injected = False
    crashed_leader_id: Optional[int] = None

    for now in range(T_MAX + 1):
        # 1) tick
        for nd in nodes:
            nd.tick(now)

        # 2) 网络投递
        for to_id, msg in net.deliver(now):
            nodes[to_id].on_msg(now, msg)

        # 3) 若产生 Leader，注入一笔交易（仅限一次）
        leader = first_leader(nodes)
        if leader and not injected and now > 30:
            ok = leader.propose(now, "transfer A B 100")
            if ok:
                injected = True

        # 4) 整死 Leader，观察重选；再恢复
        if leader and now == CRASH_LEADER_AT:
            leader.alive = False
            crashed_leader_id = leader.id
            print(f"[{now}] !!! Crash Leader{leader.id}")

        if crashed_leader_id is not None and now == RECOVER_LEADER_AT:
            nodes[crashed_leader_id].alive = True
            nodes[crashed_leader_id].rand_election_reset(now)
            print(f"[{now}] >>> Recover Node{crashed_leader_id}")
            crashed_leader_id = None

    # 打印各节点终态
    print("\n=== FINAL STATES ===")
    for nd in nodes:
        print(
            f"Node{nd.id} term={nd.current_term} role={nd.state:9} "
            f"commit={nd.commit_index} applied={nd.last_applied} "
            f"SM={nd.state_machine}"
        )


def first_leader(nodes: List[Node]) -> Optional[Node]:
    for nd in nodes:
        if nd.state == "Leader" and nd.alive:
            return nd
    return None


if __name__ == "__main__":
    run_demo()
