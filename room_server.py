"""
桌面勇者 —— 房间中转服务器
职责：房间管理 + 消息转发（不处理游戏逻辑，仅做透明代理）

本地测试：python room_server.py
部署到 Render.com / Railway：
  1. 推到 GitHub 仓库的 server/ 目录
  2. 设置环境变量 PORT、AUTH_TOKEN
  3. 启动命令：python room_server.py
"""
import asyncio
import hashlib
import hmac
import json
import logging
import os
import secrets
import socket
import sys
import time

import websockets
from websockets.server import WebSocketServerProtocol

# ── 日志降噪（重要）────────────────────────────────────────
# 服务开在公网，8765 端口会被全世界的扫描器/爬虫持续探测。每一次非 WebSocket
# 的连接（扫描器发几字节就断）都会让 websockets 打一整段 traceback：
#
#     opening handshake failed
#     EOFError: stream ends after 31 bytes, before end of line
#     ...（约 45 行）
#
# 实测线上日志被刷到 **225 万行 / 119MB**，约 5 万次失败握手。
# 致命之处在于 StandardError 是**同步写盘**、跑在事件循环线程上 ——
# 扫描高峰每秒几十次失败 → 每秒写几千行 → **事件循环被写盘卡住** →
# 正常玩家的心跳（20s PING/PONG）超时 → 服务端主动断开。
# 表现就是「服务器的连接每隔一段时间就断，重启一下又好」。
#
# 压到 CRITICAL：握手失败对我们是纯噪声（不是我们的 bug，也无需处理）。
# 真正的服务端异常仍会以 CRITICAL 或未捕获异常的形式暴露出来。
logging.getLogger("websockets").setLevel(logging.CRITICAL)
logging.getLogger("websockets.server").setLevel(logging.CRITICAL)
logging.getLogger("websockets.asyncio").setLevel(logging.CRITICAL)


# ── 热路径日志节流（2026-10-12）────────────────────────────────
# ⚠️ **为什么必须有**：systemd 下 `print()` 走 **journald，是同步阻塞写**，
#    而这几句跑在 **asyncio 事件循环**里。按「每个连接写一次」的话，
#    连接一多（公网端口天天被扫）就把循环堵住 —— 本工程记过一次同类事故：
#    扫描器噪声让日志涨到 119MB、事件循环写盘卡死、客户端集体掉线。
#    当时只把 `websockets` 自己的 logger 压到 CRITICAL，**我们自己的 print 漏了**。
#
# 2026-10-12 线上实测（同一台机器）：WS 握手 中位 4314ms / 最大 14414ms，
# 而服务端自报 `conns=1` —— 主因在机器侧（用户态被饿死，连 SSH 都拿不到 SSH 横幅），
# 但「每连接一次同步写」会**放大**它，而且是我们自己能控的那一半。
#
# 为什么是**节流**而不是删掉：连接/断开这两条恰恰是排查时最有用的，
# 只是不该按「每连接一次」的粒度写。1 秒一次既保住信息，又把写量压成常数。
_PRINT_MIN_INTERVAL = 1.0
_last_print_at: dict = {}


def _tprint(key: str, msg: str) -> None:
    """按 `key` 节流的 print —— 同一个 key 最快 1 秒写一次。

    只给**热路径**（每连接 / 每次握手失败）用；低频的（开局、GM 操作）直接 print。
    """
    now = time.monotonic()
    if now - _last_print_at.get(key, 0.0) < _PRINT_MIN_INTERVAL:
        return
    _last_print_at[key] = now
    print(msg, flush=True)

# ── 配置 ────────────────────────────────────────────────────
AUTH_TOKEN   = os.environ.get("AUTH_TOKEN", "").strip()
MAX_MSG_SIZE = 64 * 1024        # 64KB 单条消息上限
MAX_CODE_LEN = 10               # 房间码最大长度
MAX_PET_SIZE = 2048             # pet_info 最大字节数
ROOM_TIMEOUT = 30 * 60          # 30 分钟无活动自动清理
MAX_PLAYERS  = 5                # v1.5: 最多5人房间
CODE_CHARS   = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
GEN_MAX_RETRY = 100             # 房间码生成最大重试

# ── 全局状态 ──────────────────────────────────────────────
rooms: dict[str, dict] = {}
conns: dict[str, dict] = {}
battles: dict[str, str] = {}   # 对战中的玩家 ws_id → 对手 ws_id

# ── 随机匹配队列 ─────────────────────────────────────────
# 真人对战只做弱鸡对决，匹配队列固定用这一个 key（见 on_match_request）。
MATCH_GAME = "weak"
match_queues: dict[str, list] = {}   # {game_type: [ws_id, ...]}
match_pets: dict[str, dict] = {}     # {ws_id: pet_info} 排队中的宠物信息
match_ctx: dict[str, str] = {}       # {ws_id: game_type} 排队状态

# ── 数据根目录：打包后放 exe 同目录，开发时在 server/，保证跨重启持久化 ──
def _data_base() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))

_DATA_BASE = _data_base()

# ── 云存档 ──────────────────────────────────────────────────
SAVE_DIR = os.path.join(_DATA_BASE, "saves")
os.makedirs(SAVE_DIR, exist_ok=True)

# ── 登录会话（session token 持久化，跨重启/断线保持登录态）──
SESSION_DIR = os.path.join(_DATA_BASE, "sessions")
os.makedirs(SESSION_DIR, exist_ok=True)
SESSION_TTL = 30 * 24 * 3600   # 30 天有效期

def _save_path(token: str) -> str:
    """每个用户一个存档文件（token 或 ws_id 哈希）"""
    safe = "".join(c for c in token if c.isalnum())[:32] or "default"
    return os.path.join(SAVE_DIR, f"{safe}.json")


# ── 金币异常增长检测 ──
COIN_SINGLE_GAIN_MAX = int(os.environ.get("COIN_SINGLE_GAIN_MAX", 20_000_000))  # 单次上传增量上限
COIN_DAILY_GAIN_MAX  = int(os.environ.get("COIN_DAILY_GAIN_MAX", 50_000_000))   # 每日净增长上限
COIN_LARGE_GAIN      = int(os.environ.get("COIN_LARGE_GAIN", 500_000))          # 单次大额增长阈值
COIN_DAILY_LARGE_COUNT = int(os.environ.get("COIN_DAILY_LARGE_COUNT", 10))      # 每日大额增长次数上限


def _audit_path(token: str) -> str:
    safe = "".join(c for c in token if c.isalnum())[:32] or "default"
    return os.path.join(SAVE_DIR, f"{safe}.audit.json")


def _load_coin_audit(token: str) -> dict:
    try:
        with open(_audit_path(token), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_coin_audit(token: str, audit: dict):
    try:
        with open(_audit_path(token), "w", encoding="utf-8") as f:
            json.dump(audit, f)
    except Exception:
        pass


def cloud_save(token: str, data: dict, skip_check: bool = False) -> bool:
    """保存存档，带金币异常增长检测；返回是否成功保存"""
    new_coins = int(data.get("coins", 0) or 0)
    old = cloud_load(token)
    old_coins = int(old.get("coins", 0) or 0)
    delta = new_coins - old_coins
    now = time.time()

    # 每日净增长检测（只累计正增长；GM 发金币时跳过检测、不累计）
    today = time.strftime("%Y%m%d")
    audit = _load_coin_audit(token)
    if audit.get("date") != today:
        audit = {"date": today, "daily_gain": 0, "large_count": 0}

    if not skip_check:
        daily_gain = audit.get("daily_gain", 0) + max(0, delta)
        large_count = audit.get("large_count", 0)
        if delta > COIN_LARGE_GAIN:
            large_count += 1
        # 异常：单次暴增 / 每日净增长超限 / 大额增长次数超限 → 拒绝保存
        if (delta > COIN_SINGLE_GAIN_MAX or daily_gain > COIN_DAILY_GAIN_MAX
                or large_count > COIN_DAILY_LARGE_COUNT):
            print(f"[!] 金币异常：{token} 单次+{delta} 今日净增+{daily_gain} 大额{large_count}次，拒绝保存")
            return False
        audit["daily_gain"] = daily_gain
        audit["large_count"] = large_count
        _save_coin_audit(token, audit)

    data["_ts"] = now
    try:
        with open(_save_path(token), "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception:
        return False
    return True


def cloud_load(token: str) -> dict:
    try:
        with open(_save_path(token), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _session_path(token: str) -> str:
    safe = "".join(c for c in token if c.isalnum())[:64] or "default"
    return os.path.join(SESSION_DIR, f"{safe}.json")


def new_session(player_id: str) -> str:
    """创建会话，返回 token（持久化到磁盘）"""
    token = secrets.token_hex(32)
    with open(_session_path(token), "w", encoding="utf-8") as f:
        json.dump({"player_id": str(player_id), "created_at": time.time(),
                   "expires_at": time.time() + SESSION_TTL}, f)
    return token


def verify_session(token: str) -> str | None:
    """校验会话 token，返回 player_id；无效或过期返回 None"""
    if not token:
        return None
    try:
        with open(_session_path(token), encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return None
    if time.time() > data.get("expires_at", 0):
        return None
    return str(data.get("player_id", ""))


def revoke_session(token: str):
    """撤销会话（登出时调用）"""
    try:
        os.remove(_session_path(token))
    except Exception:
        pass


def clean_sessions():
    """清理过期会话文件"""
    if not os.path.isdir(SESSION_DIR):
        return
    now = time.time()
    for fn in os.listdir(SESSION_DIR):
        if not fn.endswith(".json"):
            continue
        path = os.path.join(SESSION_DIR, fn)
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        if now > data.get("expires_at", 0):
            try:
                os.remove(path)
            except Exception:
                pass


# ── 工具函数 ──────────────────────────────────────────────
def new_id() -> str:
    return secrets.token_hex(6)


def gen_code() -> str:
    """生成6位房间码，最多重试 GEN_MAX_RETRY 次"""
    for _ in range(GEN_MAX_RETRY):
        code = "".join(secrets.choice(CODE_CHARS) for _ in range(6))
        if code not in rooms:
            return code
    # 极端情况：7亿分之一的概率，panic
    raise RuntimeError("无法生成唯一房间码，房间数已达上限")


# ── 密码哈希 ──────────────────────────────────────────────
PBKDF2_ITERATIONS = 100_000


def _hash_password(pwd: str) -> str:
    """PBKDF2-SHA256 加盐哈希，格式 pbkdf2_sha256$iter$salt$hex"""
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", pwd.encode("utf-8"),
                             salt.encode("ascii"), PBKDF2_ITERATIONS)
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt}${dk.hex()}"


def _verify_password(pwd: str, stored: str) -> bool:
    """校验密码；兼容旧版明文存储（迁移兜底）"""
    if not stored or "$" not in stored:
        return stored == pwd
    try:
        algo, iters, salt, hx = stored.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        dk = hashlib.pbkdf2_hmac("sha256", pwd.encode("utf-8"),
                                 salt.encode("ascii"), int(iters))
        return hmac.compare_digest(dk.hex(), hx)
    except Exception:
        return False


def pack(**kw) -> str:
    return json.dumps(kw)


def error(msg: str) -> str:
    return pack(type="error", msg=msg)


async def send(ws_id: str, data: str):
    c = conns.get(ws_id)
    if not c:
        return
    try:
        await c["ws"].send(data)
    except websockets.exceptions.ConnectionClosed:
        pass


# ── 消息处理器 ────────────────────────────────────────────
async def broadcast_to_room(ws_id: str, data: str):
    """广播给房间内所有其他成员"""
    c = conns.get(ws_id)
    if not c or not c["room"]:
        return
    room = rooms.get(c["room"])
    if not room:
        return
    for m in room["members"]:
        if m["ws_id"] != ws_id:
            await send(m["ws_id"], data)


async def broadcast_to_room_all(code: str, data: str):
    """广播给房间内所有成员（包括发送者）"""
    room = rooms.get(code)
    if not room:
        return
    for m in room["members"]:
        await send(m["ws_id"], data)


# ── 消息处理器 ────────────────────────────────────────────
async def on_create(ws_id: str, msg: dict):
    if conns[ws_id]["room"]:
        await send(ws_id, error("已在房间中，请先离开"))
        return
    code = gen_code()
    pet = msg.get("pet", {})
    if len(json.dumps(pet)) > MAX_PET_SIZE:
        pet = {"name": "房主的宠物"}
    room = {"members": [], "created_at": time.time(), "last_active": time.time()}
    # S2 / S4：房主坐 1 号，同样走**入座的唯一出口** ——
    # 别在这里手写列表字面量：一处手写、一处走函数，两边早晚对不上。
    seat = _add_member(room, ws_id, pet)
    rooms[code] = room
    conns[ws_id]["room"] = code
    conns[ws_id]["role"] = "host"
    # `seat` 单独回一份：客户端要知道**自己**坐几号，
    # 而 `members` 一直是「不含自己」的列表，在里面找不到自己。
    await send(ws_id, pack(type="room_created", code=code, members=[], seat=seat))
    print(f"[+] 房间 {code} 由 {ws_id} 创建")


async def on_join(ws_id: str, msg: dict):
    if conns[ws_id]["room"]:
        await send(ws_id, error("已在房间中，请先离开"))
        return
    code = msg.get("code", "").upper().strip()
    if len(code) > MAX_CODE_LEN:
        await send(ws_id, error(f"房间码过长（最多{MAX_CODE_LEN}字符）"))
        return
    room = rooms.get(code)
    if not room:
        await send(ws_id, error(f"房间 [{code}] 不存在"))
        return
    pet = msg.get("pet", {})
    if len(json.dumps(pet)) > MAX_PET_SIZE:
        pet = {"name": "好友的宠物"}

    # S2/S4：入座走**唯一出口**（它内部按座位号判满员 —— 比数人数严格：
    # 成员列表万一有重复或脏座位号，数人数会放过一个「其实没座位」的人）
    seat = _add_member(room, ws_id, pet)
    if seat == 0:
        await send(ws_id, error(f"房间 [{code}] 已满（最多{MAX_PLAYERS}人）"))
        return
    new_member = room["members"][-1]
    conns[ws_id]["room"] = code
    conns[ws_id]["role"] = "guest"

    # 告诉加入者房间信息（不包括自己），以及**自己坐几号**
    others = [m for m in room["members"] if m["ws_id"] != ws_id]
    await send(ws_id, pack(type="room_joined", code=code, members=others,
                           seat=seat))
    # 告诉其他人有新人来了
    await broadcast_to_room(ws_id, pack(type="member_joined", member=new_member))
    print(f"[+] {ws_id} 加入房间 {code}（共{len(room['members'])}人）")


async def on_leave(ws_id: str):
    c = conns.get(ws_id)
    if not c or not c["room"]:
        return
    code = c["room"]
    room = rooms.get(code)
    if not room:
        return
    role = c["role"]
    c["room"] = None; c["role"] = None
    _clear_battle(ws_id)

    # 从成员列表移除
    room["members"] = [m for m in room["members"] if m["ws_id"] != ws_id]

    if not room["members"]:
        # 房间空了，删除
        del rooms[code]
        print(f"[-] 房间 {code} 解散（无成员）")
        return

    if role == "host":
        # 房主解散：通知所有剩余成员退出，删除房间
        await broadcast_to_room_all(code, pack(type="room_disbanded"))
        del rooms[code]
        print(f"[-] 房主解散房间 {code}（通知{len(room['members'])}人退出）")
        return

    # 普通成员离开：通知其他人
    await broadcast_to_room_all(code, pack(type="member_left", ws_id=ws_id,
        members=room["members"]))
    print(f"[-] {ws_id}({role}) 离开房间 {code}（剩{len(room['members'])}人）")


async def on_sync(ws_id: str, msg: dict):
    c = conns.get(ws_id)
    if not c or not c["room"]:
        return
    room = rooms.get(c["room"])
    if room:
        room["last_active"] = time.time()
    payload = {k: v for k, v in msg.items() if k != "type"}
    payload["from_ws_id"] = ws_id
    await broadcast_to_room(ws_id, pack(type="remote_sync", **payload))


def _room_of(ws_id: str):
    """这条连接所在的房间码（None = 不在任何房间）"""
    return conns.get(ws_id, {}).get("room")


def _free_seat(room: dict) -> int:
    """房间里**最小的空座位号**（1 起）；满了返回 0。

    为什么是「最小空号」而不是「人数 + 1」：座位号要**不漂移**。
    2 号走了、再来个人补上，他应该坐 2 号 —— 取 len+1 的话他会坐 3 号，
    而 4/5 人局的回合顺序是按座位号走的，凭空多个空号会让顺序莫名其妙。

    ⚠️ 座位号是 4/5 人身份局的**地基**：身份、阵营、回合顺序、点名目标、
    结算列表全要按它对齐。而 `members` 里原来只有 `ws_id`，偏偏 `ws_id`
    **每次连接都重新随机生成**（`new_id()`）—— 拿它当座位用不了，
    断线重连也认不回原位。
    """
    used = {m.get("seat") for m in room["members"]}
    for i in range(1, MAX_PLAYERS + 1):
        if i not in used:
            return i
    return 0


def _seat_of(ws_id: str) -> int:
    """这条连接在它房间里坐几号；不在房间返回 0。"""
    code = _room_of(ws_id)
    room = rooms.get(code) if code else None
    if not room:
        return 0
    for m in room["members"]:
        if m["ws_id"] == ws_id:
            return int(m.get("seat") or 0)
    return 0


def _add_member(room: dict, ws_id: str, pet) -> int:
    """把一个人放进房间，返回他的座位号；**满了返回 0，且什么都不改**。

    ★ 这是「入座」的**唯一出口**（S4）—— `on_join`、匹配建房、以及以后任何
    新的组局路径都必须走它。为什么要有这个出口：匹配建房原来是**直接构造
    `members` 列表**的，把 `MAX_PLAYERS` 整个绕过去了。2 人匹配看不出来，
    但将来做 4 / 5 人匹配时，那一处只要忘了判就会**静默超员**
    ——「6 个人进了 5 人房」，而引擎那边会直接按座位号越界。
    """
    seat = _free_seat(room)
    if seat == 0:
        return 0
    room["members"].append(_member(ws_id, pet, seat))
    room["last_active"] = time.time()
    return seat


def _make_match_room(pairs: list):
    """给一批匹配到的玩家建一间临时房。

    返回 `(房间码, {ws_id: 座位号})`；**坐不下返回 None，且一个房间都不留下**
    —— 别留半间房（那会让后续 `_same_room` 之类的校验看到一间残缺的房）。
    """
    code = gen_code()
    room = {"members": [], "created_at": time.time(), "last_active": time.time()}
    seats = {}
    for wid, pt in pairs:
        seat = _add_member(room, wid, pt)
        if seat == 0:
            return None
        seats[wid] = seat
    rooms[code] = room
    return code, seats


def _member(ws_id: str, pet, seat: int) -> dict:
    """房间成员的结构 —— **只在这里定义一次**。

    建房 / 加入 / 匹配建房三处都要造它。散着写早晚有一处漏字段，
    而漏掉的若是 `seat` 这种「没有它也能跑、只是悄悄错位」的字段，最难查。
    """
    return {"ws_id": ws_id, "pet": pet, "joined_at": time.time(), "seat": seat}


def _touch_room(ws_id: str) -> None:
    """给「房间最近有活动」打点（S3）。

    ⚠️ 为什么必须补（2026-10-15）：`last_active` 原来**只**在
    create_room / join_room / sync 三处刷新，而 1v1 对局全程只发 `pvp_move`、
    **根本不发 `sync`** —— 于是一局超过 `ROOM_TIMEOUT`（30 分钟）的对局会被
    cleaner 判成「空闲房间」拆掉。

    拆掉的后果不只是房间没了：cleaner 会把 `conns[...]["room"]` 一起清空，
    而 S1 新增的同房间校验从此会**拒掉双方所有的定向消息** → 对局当场冻住。
    所以「对局消息算不算房间活动」这件事，是和 S1 绑在一起的。
    """
    code = _room_of(ws_id)
    if code and code in rooms:
        rooms[code]["last_active"] = time.time()


def _same_room(ws_id: str, target_id: str) -> bool:
    """定向投递前的**同房间校验**（S1）。

    ⚠️ 为什么必须有（2026-10-15）：`target_ws_id` 是**客户端自报**的，
    服务端原来一句话都不问就 `send(target)` —— 于是任何客户端都能把
    `pvp_move` / `game_invite` / `pvp_accept` / `pvp_decline` 定向发给
    **全服任意一条连接**（`ws_id` 是 12 位随机 hex、猜不到，但服务端零设防）。

    做 4/5 人身份局之前必须先堵上：那条路会把「谁能收到什么」整个交到客户端手里，
    而身份局的全部价值就在「谁该收到什么」。
    """
    a = _room_of(ws_id)
    b = _room_of(target_id)
    return bool(a) and a == b


async def _reject_cross_room(ws_id: str, target: str, what: str) -> None:
    """跨房间定向投递：记一笔日志并回绝。

    ⚠️ 回的是「对方不在同一房间」，**不是**「目标不存在」——
    后者等于给出一个可以拿来探测 `ws_id` 是否存在的接口。这是攻击面，别回细节。
    """
    print(f"[!] 跨房间定向投递被拒：{what}  from={ws_id} to={target}", flush=True)
    await send(ws_id, error("对方不在同一房间，消息未送达"))


def _clear_battle(ws_id: str):
    """清除某个玩家的对战状态"""
    opp = battles.pop(ws_id, None)
    if opp:
        battles.pop(opp, None)


def _battle_over(card: str, game: str) -> bool:
    """判断 pvp_move 是否表示对战结束"""
    if card in ("forfeit:", "battle_end"):
        return True
    if game == "weak" and card.startswith("end:"):
        parts = card.split(":")
        if len(parts) >= 4:
            try:
                return int(parts[3]) != 0  # winner != 0 表示已分胜负
            except ValueError:
                return False
    return False


async def on_pvp_move(ws_id: str, msg: dict):
    target = msg.get("target_ws_id", "")
    if _battle_over(msg.get("card", ""), msg.get("game", "")):
        _clear_battle(ws_id)
    payload = {k: v for k, v in msg.items() if k not in ("type", "target_ws_id")}
    payload["from_ws_id"] = ws_id
    if target:
        if not _same_room(ws_id, target):          # S1
            await _reject_cross_room(ws_id, target, "pvp_move")
            return
        _touch_room(ws_id)                          # S3
        await send(target, pack(type="pvp_move", **payload))
    else:
        _touch_room(ws_id)                          # S3
        await broadcast_to_room(ws_id, pack(type="pvp_move", **payload))


async def on_game_invite(ws_id: str, msg: dict):
    target_id = msg.get("target_ws_id", "")
    payload = {k: v for k, v in msg.items() if k != "type"}
    payload["from_ws_id"] = ws_id
    # 自己正在对战中 → 不能再邀请别人
    if ws_id in battles:
        await send(ws_id, error("你正在对战中，无法发起邀请"))
        return
    if target_id:
        if not _same_room(ws_id, target_id):        # S1
            await _reject_cross_room(ws_id, target_id, "game_invite")
            return
        if target_id in battles:
            await send(ws_id, error("对方正在对战"))
            return
        _touch_room(ws_id)                          # S3
        await send(target_id, pack(type="game_invite", **payload))
    else:
        _touch_room(ws_id)                          # S3
        await broadcast_to_room(ws_id, pack(type="game_invite", **payload))


async def on_bubble(ws_id: str, msg: dict):
    payload = {k: v for k, v in msg.items() if k != "type"}
    payload["from_ws_id"] = ws_id
    _touch_room(ws_id)                              # S3
    await broadcast_to_room(ws_id, pack(type="remote_bubble", **payload))


async def on_pvp_accept(ws_id: str, msg: dict):
    target = msg.get("target_ws_id", "")
    payload = {k: v for k, v in msg.items() if k not in ("type", "target_ws_id")}
    payload["from_ws_id"] = ws_id
    if target:
        # ⚠️ 顺序要紧：校验必须在**写 `battles` 之前** —— 否则跨房间的伪造 accept
        #    虽发不出去，却已经在服务端把双方标成「对战中」了。
        if not _same_room(ws_id, target):           # S1
            await _reject_cross_room(ws_id, target, "pvp_accept")
            return
        battles[ws_id] = target
        battles[target] = ws_id
        _touch_room(ws_id)                          # S3
        await send(target, pack(type="pvp_accept", **payload))
    else:
        _touch_room(ws_id)                          # S3
        await broadcast_to_room(ws_id, pack(type="pvp_accept", **payload))


async def on_pvp_decline(ws_id: str, msg: dict):
    target = msg.get("target_ws_id", "")
    if target:
        if not _same_room(ws_id, target):           # S1
            await _reject_cross_room(ws_id, target, "pvp_decline")
            return
        _touch_room(ws_id)                          # S3
        await send(target, pack(type="pvp_decline", from_ws_id=ws_id))
    else:
        _touch_room(ws_id)                          # S3
        await broadcast_to_room(ws_id, pack(type="pvp_decline", from_ws_id=ws_id))


# ── 随机匹配 ──────────────────────────────────────────────
async def on_match_request(ws_id: str, msg: dict):
    """加入匹配队列，若已有同类型等待者则配对

    ⚠️ `game` **一律强制为弱鸡对决，不接受客户端指定**。
    队列是按 game 分桶的，放任客户端自报 game 会把玩家池静默拆开：
    报 "rps" 的人只会和同样报 "rps" 的人配对，而客户端本来就只有弱鸡这一个
    匹配入口 —— 结果是双方各自排队、谁也等不到人，还看不出是坏了。
    真人对战目前只做弱鸡对决，所以这里不需要那个自由度。
    """
    game = MATCH_GAME
    pet = msg.get("pet", {})
    if ws_id in match_ctx:
        return  # 已在匹配中
    if conns[ws_id].get("room"):
        await send(ws_id, error("已在房间中，请先离开"))
        return
    q = match_queues.setdefault(game, [])
    if q:
        # 有人等待 → 配对
        peer_id = q.pop(0)
        match_ctx.pop(peer_id, None)
        match_ctx.pop(ws_id, None)
        peer_pet = match_pets.pop(peer_id, {})
        match_pets.pop(ws_id, None)
        # 建立临时对战房间，复用 pvp_move 广播
        # ★ S4：走 `_make_match_room`（内部 `_add_member`）—— 原来这里是
        #   **直接构造 members 列表**的，绕过了 `MAX_PLAYERS`。2 人匹配看不出问题，
        #   将来做 N 人匹配时那一处忘了判就会静默超员。
        _made = _make_match_room([(peer_id, peer_pet), (ws_id, pet)])
        if _made is None:
            # 坐不下 → **不能开局，也不能静默超员**。理论上到不了（2 < MAX_PLAYERS），
            # 但真到了多半是 `MAX_PLAYERS` 被配小了，宁可两边都收到明确错误。
            print(f"[!] 匹配建房失败：坐不下（MAX_PLAYERS={MAX_PLAYERS}）"
                  f" {peer_id} vs {ws_id}", flush=True)
            await send(ws_id, error("匹配失败：房间坐不下（服务端配置异常）"))
            await send(peer_id, error("匹配失败：房间坐不下（服务端配置异常）"))
            return
        code, _seats = _made
        conns[peer_id]["room"] = code
        conns[peer_id]["role"] = "host"
        conns[ws_id]["room"] = code
        conns[ws_id]["role"] = "guest"
        seed = secrets.token_hex(3)
        await send(peer_id, pack(type="match_found", game=game, is_host=True,
                                 seat=_seats[peer_id], seed=seed, opponent=pet))
        await send(ws_id, pack(type="match_found", game=game, is_host=False,
                               seat=_seats[ws_id], seed=seed, opponent=peer_pet))
        print(f"[+] 匹配成功 {peer_id} vs {ws_id}（{game}）")
    else:
        # 入队等待
        q.append(ws_id)
        match_ctx[ws_id] = game
        match_pets[ws_id] = pet
        await send(ws_id, pack(type="match_waiting", game=game))
        print(f"[*] {ws_id} 进入匹配队列（{game}），等待对手")


async def on_match_cancel(ws_id: str, msg: dict):
    """取消匹配"""
    game = match_ctx.pop(ws_id, None)
    if game and ws_id in match_queues.get(game, []):
        match_queues[game].remove(ws_id)
    match_pets.pop(ws_id, None)
    await send(ws_id, pack(type="match_canceled"))


# ── 连接主处理器 ──────────────────────────────────────────
HANDLERS = {
    "create_room":   on_create,
    "join_room":     on_join,
    "leave_room":    lambda ws_id, _: on_leave(ws_id),
    "sync":          on_sync,
    "pvp_move":      on_pvp_move,
    "game_invite":   on_game_invite,
    "bubble":        on_bubble,
    "pvp_accept":    lambda ws_id, msg: on_pvp_accept(ws_id, msg),
    "pvp_decline":   lambda ws_id, msg: on_pvp_decline(ws_id, msg),
    "match_request": lambda ws_id, msg: on_match_request(ws_id, msg),
    "match_cancel":  lambda ws_id, msg: on_match_cancel(ws_id, msg),
    # 云存档
    "sync_upload":   lambda ws_id, msg: on_sync_upload(ws_id, msg),
    "sync_download": lambda ws_id, msg: on_sync_download(ws_id, msg),
    "resume_session": lambda ws_id, msg: on_resume_session(ws_id, msg),
    "logout":        lambda ws_id, msg: on_logout(ws_id, msg),
    "check_name":    lambda ws_id, msg: on_check_name(ws_id, msg),
    "register_user": lambda ws_id, msg: on_register_user(ws_id, msg),
    "login_user":    lambda ws_id, msg: on_login_user(ws_id, msg),
    "change_password": lambda ws_id, msg: on_change_password(ws_id, msg),
    "gm_search":     lambda ws_id, msg: on_gm_search(ws_id, msg),
    "gm_give":       lambda ws_id, msg: on_gm_give(ws_id, msg),
    "gm_gen_invite":   lambda ws_id, msg: on_gm_gen_invite(ws_id, msg),
    "gm_invite_count": lambda ws_id, msg: on_gm_invite_count(ws_id, msg),
    "validate_invite": lambda ws_id, msg: on_validate_invite(ws_id, msg),
    "auth":          lambda ws_id, msg: on_auth(ws_id, msg),
}


async def on_auth(ws_id: str, msg: dict):
    """连接认证消息：认证已在 handle 主循环完成，这里无需额外操作"""
    pass

def _is_authenticated(ws_id: str, player_id: str) -> bool:
    """校验该连接是否已通过 player_id 的登录认证"""
    c = conns.get(ws_id)
    return bool(c) and bool(player_id) and c.get("auth_player_id") == str(player_id)


async def on_sync_upload(ws_id: str, msg: dict):
    """客户端上传存档（需已登录认证，防越权写他人存档）"""
    pid = str(msg.get("player_id", ""))
    if not _is_authenticated(ws_id, pid):
        await send(ws_id, error("云存档未认证，请先登录"))
        return
    if not cloud_save(pid, msg.get("data", {})):
        await send(ws_id, error("金币增长异常，存档未保存"))
        return
    await send(ws_id, pack(type="sync_uploaded", ts=time.time()))

async def on_sync_download(ws_id: str, msg: dict):
    """客户端下载存档（需已登录认证，防越权读他人存档）"""
    pid = str(msg.get("player_id", ""))
    if not _is_authenticated(ws_id, pid):
        await send(ws_id, error("云存档未认证，请先登录"))
        return
    data = cloud_load(pid)
    await send(ws_id, pack(type="sync_data", data=data))


async def on_resume_session(ws_id: str, msg: dict):
    """用会话 token 恢复登录态（重启/断线重连后调用）"""
    token = msg.get("session_token", "")
    pid = verify_session(token)
    if not pid:
        await send(ws_id, pack(type="resume_result", ok=False))
        return
    conns[ws_id]["auth_player_id"] = pid
    player_registry[pid] = ws_id
    await send(ws_id, pack(type="resume_result", ok=True, user_id=pid))


async def on_logout(ws_id: str, msg: dict):
    """登出：撤销会话并清除该连接认证状态"""
    revoke_session(msg.get("session_token", ""))
    c = conns.get(ws_id)
    if c:
        pid = c.get("auth_player_id", "")
        if pid and player_registry.get(pid) == ws_id:
            player_registry.pop(pid, None)
        c["auth_player_id"] = None
    await send(ws_id, pack(type="logout_result", ok=True))



# ── GM 配置 ──────────────────────────────────────────────
GM_SECRET = os.environ.get("GM_SECRET", "").strip()  # GM 工具认证密钥；未设置则禁用 GM

# ── 玩家注册表（player_id → ws_id）───────────────────────
player_registry: dict[str, str] = {}  # 记录在线玩家的 ws_id

# ── 注册 ────────────────────────────────────────────────
REGISTER_DIR = os.path.join(_DATA_BASE, "register")
os.makedirs(REGISTER_DIR, exist_ok=True)

def _reg_path(name: str) -> str:
    safe = "".join(c for c in name if c.isalnum())[:32] or "default"
    return os.path.join(REGISTER_DIR, f"{safe}.json")


def _load_reg(name: str) -> dict | None:
    """按名字读取账号记录"""
    path = _reg_path(name)
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _find_by_user_id(user_id: str) -> dict | None:
    """按 8 位 ID 查找账号记录（一个 ID 对应一个账号）"""
    if not user_id or not os.path.isdir(REGISTER_DIR):
        return None
    for fn in os.listdir(REGISTER_DIR):
        if not fn.endswith(".json"):
            continue
        try:
            with open(os.path.join(REGISTER_DIR, fn), encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        if str(data.get("user_id", "")) == str(user_id):
            return data
    return None


def migrate_passwords():
    """启动时把 register/ 里的明文密码一次性转成 PBKDF2 哈希（幂等）"""
    if not os.path.isdir(REGISTER_DIR):
        return
    migrated = 0
    for fn in os.listdir(REGISTER_DIR):
        if not fn.endswith(".json"):
            continue
        path = os.path.join(REGISTER_DIR, fn)
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        stored = data.get("password", "")
        if stored and "$" not in stored:  # 明文 → 哈希
            data["password"] = _hash_password(stored)
            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                migrated += 1
            except Exception:
                pass
    if migrated:
        print(f"[!] 已迁移 {migrated} 个明文密码为哈希")


async def on_check_name(ws_id: str, msg: dict):
    name = msg.get("name", "").strip()
    if not name:
        await send(ws_id, pack(type="check_name_result", exists=False, msg="名字不能为空"))
        return
    exists = os.path.exists(_reg_path(name))
    await send(ws_id, pack(type="check_name_result", exists=exists))

async def on_register_user(ws_id: str, msg: dict):
    name = msg.get("name", "").strip()
    pwd = msg.get("password", "")
    uid = msg.get("user_id", "")
    if not name or not uid:
        await send(ws_id, pack(type="register_result", ok=False, msg="名字或ID不能为空"))
        return
    path = _reg_path(name)
    if os.path.exists(path):
        await send(ws_id, pack(type="register_result", ok=False, msg="名字已被使用"))
        return
    if _find_by_user_id(uid):
        await send(ws_id, pack(type="register_result", ok=False, msg="该ID已被使用"))
        return
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"name": name, "password": _hash_password(pwd),
                       "user_id": uid, "created_at": time.time()}, f)
        save_path = _save_path(uid)
        if not os.path.exists(save_path):
            # 云存档只存游戏数据，账号身份（名字/密码/ID）在 register/ 目录
            cloud_save(uid, {"coins": 5000})
        conns[ws_id]["auth_player_id"] = uid
        player_registry[uid] = ws_id
        session = new_session(uid)
        await send(ws_id, pack(type="register_result", ok=True,
                               session_token=session))
    except Exception as e:
        await send(ws_id, pack(type="register_result", ok=False, msg=f"注册失败: {e}"))


async def on_login_user(ws_id: str, msg: dict):
    """服务端校验登录：名字或 ID + 密码，返回账号名与 ID"""
    name = msg.get("name", "").strip()
    user_id = msg.get("user_id", "").strip()
    pwd = msg.get("password", "")
    if user_id:
        data = _find_by_user_id(user_id)
    elif name:
        data = _load_reg(name)
    else:
        data = None
    if not data:
        await send(ws_id, pack(type="login_result", ok=False, msg="账号不存在，请先注册"))
        return
    if not _verify_password(pwd, data.get("password", "")):
        await send(ws_id, pack(type="login_result", ok=False, msg="密码错误"))
        return
    uid = str(data.get("user_id", ""))
    conns[ws_id]["auth_player_id"] = uid
    player_registry[uid] = ws_id
    session = new_session(uid)
    await send(ws_id, pack(type="login_result", ok=True,
                           user_id=uid,
                           name=data.get("name", ""),
                           session_token=session))


async def on_change_password(ws_id: str, msg: dict):
    """改密：校验旧密码后写入新密码（账号按 ID 或名字定位）"""
    name = msg.get("name", "").strip()
    user_id = msg.get("user_id", "").strip()
    old_pwd = msg.get("old_password", "")
    new_pwd = msg.get("new_password", "")
    if not new_pwd:
        await send(ws_id, pack(type="change_password_result", ok=False, msg="新密码不能为空"))
        return
    if user_id:
        data = _find_by_user_id(user_id)
    elif name:
        data = _load_reg(name)
    else:
        data = None
    if not data:
        await send(ws_id, pack(type="change_password_result", ok=False, msg="账号不存在，请先注册"))
        return
    if not _verify_password(old_pwd, data.get("password", "")):
        await send(ws_id, pack(type="change_password_result", ok=False, msg="旧密码错误"))
        return
    data["password"] = _hash_password(new_pwd)
    try:
        with open(_reg_path(data.get("name", name)), "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        await send(ws_id, pack(type="change_password_result", ok=False, msg=f"修改失败: {e}"))
        return
    await send(ws_id, pack(type="change_password_result", ok=True))


# ── GM 命令 ─────────────────────────────────────────────
def _gm_auth_check(msg: dict) -> bool:
    """GM 认证：未配置 GM_SECRET 时一律拒绝，配置后常量时间比对

    ⚠️ 新代码请用 `_gm_auth_fail(msg, ws_id)` —— 它能区分「没配」和「填错」，
    而且**带失败限流**。这个布尔版不带限流，别在新代码里用。
    """
    return not _gm_auth_fail(msg)


# ── GM 认证失败限流（2026-09-28 加；2026-10-14 起**默认关闭**）──────
# 为什么当初要加（2026-09-28）：GM 接口原先**没有任何频率限制** —— 失败多少次
# 都不拦。而密钥是 6 位纯数字（100 万种组合），按 100 次/秒算 ≈ 2.8 小时就能
# 爆破完，拿到之后可以给任意玩家发任意物品/金币、拉全服玩家名单。
# 限流之后这条路才堵上；否则弱密钥等于没设。
#
# ⚠️⚠️ **为什么 2026-10-14 改成默认关闭**（用户要求）：
#   限流的**前提是密钥弱**。而「按来源 IP 封禁」有个用户直接撞上的副作用 ——
#   **同一条宽带下的几台电脑公网 IP 是同一个**，一台手滑，其余的也一起被挡在
#   门外；6 位密钥本来就容易手滑。用户的诉求是「换到别的电脑也能随时登」。
#
#   所以改成：**用长密钥**（systemd 主 unit 里那个 48 位 hex）**+ 限流默认关**。
#   长密钥爆不动，本来就不需要限流 —— 两个措施是配套的，别只做一个：
#   关限流而仍用 6 位密钥 = 把爆破口重新打开。
#
# 想再打开：环境变量 `GM_RATE_LIMIT=1`（下面三个参数照旧可调）。
# 代码一行没删 —— 哪天密钥又换短的，打开就能用。
GM_RATE_LIMIT = os.environ.get("GM_RATE_LIMIT", "0").strip().lower() not in ("", "0", "false", "no")

#   GM_FAIL_WINDOW  统计窗口（秒，默认 60）
#   GM_FAIL_MAX     窗口内允许的失败次数（默认 5）
#   GM_FAIL_BLOCK   超限后封禁多久（秒，默认 300）
GM_FAIL_WINDOW = int(os.environ.get("GM_FAIL_WINDOW", 60))
GM_FAIL_MAX    = int(os.environ.get("GM_FAIL_MAX", 5))
GM_FAIL_BLOCK  = int(os.environ.get("GM_FAIL_BLOCK", 300))

_gm_fails: dict[str, list] = {}     # ip → 窗口内的失败时间戳
_gm_blocked: dict[str, float] = {}  # ip → 解封时刻


def _gm_client_ip(ws_id: str) -> str:
    """取连接的来源 IP；拿不到就退化成 ws_id。

    退化成 ws_id 也能用（按连接限流），只是攻击者重连就能绕过 ——
    但拿得到 IP 是常态，退化分支只是兜底。
    """
    try:
        ws = conns.get(ws_id, {}).get("ws")
        addr = getattr(ws, "remote_address", None)
        return (addr[0] if addr else "") or ws_id
    except Exception:
        return ws_id


def _gm_block_remaining(ip: str) -> int:
    """还被封着就返回剩余秒数，否则 0（顺便清掉已过期的封禁）"""
    until = _gm_blocked.get(ip, 0)
    now = time.time()
    if until > now:
        return int(until - now) + 1
    if until:
        _gm_blocked.pop(ip, None)
    return 0


def _gm_note_fail(ip: str) -> int:
    """记一次失败。返回**本次是否触发了封禁**（触发了就返回封禁秒数，否则 0）。

    返回而不是返回 None，是为了让「触发封禁的那一次请求」就能立刻回
    「次数过多」—— 否则状态在这一次变了、提示却要等下一次才出现。

    `GM_RATE_LIMIT` 关着时**只记日志、不封禁** —— 运维仍然要能看到
    「有人在试密钥」，但那不该把操作者本人挡在门外（见文件里那段说明）。
    """
    now = time.time()
    lst = [t for t in _gm_fails.get(ip, []) if t > now - GM_FAIL_WINDOW]
    lst.append(now)
    if not GM_RATE_LIMIT:
        _gm_fails[ip] = lst
        print(f"[!] GM 认证失败（来源 {ip}，窗口内第 {len(lst)} 次；"
              f"GM_RATE_LIMIT 关闭，不封禁）", flush=True)
        return 0
    blocked = 0
    if len(lst) >= GM_FAIL_MAX:
        _gm_blocked[ip] = now + GM_FAIL_BLOCK
        lst = []
        blocked = GM_FAIL_BLOCK
        print(f"[!] GM 认证窗口内失败 {GM_FAIL_MAX} 次，封禁 {ip} {GM_FAIL_BLOCK}s", flush=True)
    _gm_fails[ip] = lst
    return blocked


def _gm_note_ok(ip: str) -> None:
    """认证成功就清账 —— 免得手滑几次之后，改对了还被自己之前的失败封着"""
    _gm_fails.pop(ip, None)
    _gm_blocked.pop(ip, None)


def _gm_prune_rate_limit() -> int:
    """清掉过期的失败记录与封禁（由 cleaner 每分钟调一次），返回清掉的条数。

    为什么要清：这两个字典按 IP 建键，扫端口/爆破的人会不断换 IP，
    不清就是一条缓慢的内存泄漏。
    """
    now = time.time()
    n = 0
    for ip in list(_gm_fails):
        kept = [t for t in _gm_fails[ip] if t > now - GM_FAIL_WINDOW]
        if kept:
            _gm_fails[ip] = kept
        else:
            _gm_fails.pop(ip, None); n += 1
    for ip in list(_gm_blocked):
        if _gm_blocked[ip] <= now:
            _gm_blocked.pop(ip, None); n += 1
    return n


def _gm_auth_fail(msg: dict, ws_id: str = "") -> str:
    """**空串 = 通过**；否则返回该回给客户端的原因。

    为什么要区分（2026-09-28）：两种失败的原因完全不同，但原先都回
    「GM认证失败」，客户端分不出来 —— 用户明明填对了密钥却一直被拒，
    而真正的原因是服务端压根没配（fail-closed 设计），界面上没有任何线索。

    「未配置」这句话**不泄露任何东西**：它不是关于密钥本身的信息，
    攻击者本来也只能看到"失败"，跟服务端启动日志里那行提示是同一件事。
    """
    ip = _gm_client_ip(ws_id) if ws_id else ""
    # ⚠️ `GM_RATE_LIMIT` 关着时**连查都不查** —— 否则封禁字典里万一还留着
    #    上次开着时写进去的记录，关了限流却仍被挡（改完配置得能立刻生效）。
    if ip and GM_RATE_LIMIT:
        wait = _gm_block_remaining(ip)
        if wait:
            # 限流提示**先于**其它判断：被封了就别说密钥对不对，
            # 否则等于免费告诉爆破者"这个密钥是错的"，省了他一轮
            return f"GM认证失败次数过多，请 {wait} 秒后再试"
    if not GM_SECRET:
        return "GM 功能未启用：服务端未配置 GM_SECRET 环境变量（此时任何密钥都会被拒绝）"
    if not hmac.compare_digest(str(msg.get("gm_secret", "")), GM_SECRET):
        # 触发封禁的那一次就直接告知，不让状态变了、提示却慢一拍
        blocked = _gm_note_fail(ip) if ip else 0
        if blocked:
            return f"GM认证失败次数过多，请 {blocked} 秒后再试"
        return "GM认证失败"
    if ip:
        _gm_note_ok(ip)
    return ""


# ── 邀请码管理 ──────────────────────────────────────────
INVITE_DIR = os.path.join(_DATA_BASE, "invites")
INVITE_FILE = os.path.join(INVITE_DIR, "invites.json")
os.makedirs(INVITE_DIR, exist_ok=True)
INVITE_CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
INVITE_CODE_PREFIX = "LBD"


def _load_invites() -> dict:
    if not os.path.exists(INVITE_FILE):
        return {"codes": {}}
    try:
        with open(INVITE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"codes": {}}


def _save_invites(data: dict):
    with open(INVITE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _gen_invite_code() -> str:
    data = _load_invites()
    existing = set(data["codes"].keys())
    for _ in range(1000):
        suffix = "".join(secrets.choice(INVITE_CODE_CHARS) for _ in range(5))
        code = INVITE_CODE_PREFIX + suffix
        if code not in existing:
            return code
    raise RuntimeError("无法生成唯一邀请码")


async def on_gm_gen_invite(ws_id: str, msg: dict):
    """GM生成邀请码（需GM认证）"""
    _fail = _gm_auth_fail(msg, ws_id)
    if _fail:
        await send(ws_id, pack(type="gm_gen_invite_result", ok=False, msg=_fail))
        return
    data = _load_invites()
    code = _gen_invite_code()
    data["codes"][code] = {"used": False, "generated_at": time.time()}
    _save_invites(data)
    count = sum(1 for c in data["codes"].values() if not c["used"])
    await send(ws_id, pack(type="gm_gen_invite_result", ok=True, code=code, remaining=count))
    print(f"[GM] 生成邀请码: {code} (剩余 {count})")


async def on_gm_invite_count(ws_id: str, msg: dict):
    """GM查询剩余邀请码数量（需GM认证）"""
    _fail = _gm_auth_fail(msg, ws_id)
    if _fail:
        await send(ws_id, pack(type="gm_invite_count_result", ok=False, msg=_fail))
        return
    data = _load_invites()
    count = sum(1 for c in data["codes"].values() if not c["used"])
    total = len(data["codes"])
    await send(ws_id, pack(type="gm_invite_count_result", ok=True, remaining=count, total=total))


async def on_validate_invite(ws_id: str, msg: dict):
    """游戏客户端验证邀请码（无需认证）"""
    code = msg.get("code", "").upper().strip()
    if not code:
        await send(ws_id, pack(type="invite_validation", ok=False, msg="邀请码不能为空"))
        return
    data = _load_invites()
    entry = data["codes"].get(code)
    if entry is None:
        await send(ws_id, pack(type="invite_validation", ok=False, msg="邀请码无效"))
        return
    if entry.get("used", False):
        await send(ws_id, pack(type="invite_validation", ok=False, msg="邀请码已被使用"))
        return
    entry["used"] = True
    entry["used_at"] = time.time()
    entry["used_by"] = msg.get("player_name", "unknown")
    _save_invites(data)
    await send(ws_id, pack(type="invite_validation", ok=True, msg="验证成功"))
    print(f"[+] 邀请码 {code} 被 {entry.get('used_by', '?')} 使用")


async def on_gm_search(ws_id: str, msg: dict):
    _fail = _gm_auth_fail(msg, ws_id)
    if _fail:
        await send(ws_id, pack(type="gm_search_result", ok=False, msg=_fail))
        return
    query = msg.get("query", "").strip()
    results = []
    if os.path.isdir(REGISTER_DIR):
        for fn in os.listdir(REGISTER_DIR):
            if not fn.endswith(".json"):
                continue
            try:
                with open(os.path.join(REGISTER_DIR, fn), encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                continue
            pid = str(data.get("user_id", ""))
            pname = data.get("name", "")
            if query and query not in pid and query.lower() not in pname.lower():
                continue
            save = cloud_load(pid)
            results.append({
                "user_id": pid,
                "name": pname,
                "coins": save.get("coins", 0),
                "online": pid in player_registry,
            })
    await send(ws_id, pack(type="gm_search_result", ok=True, results=results))

async def on_gm_give(ws_id: str, msg: dict):
    _fail = _gm_auth_fail(msg, ws_id)
    if _fail:
        await send(ws_id, pack(type="gm_give_result", ok=False, msg=_fail))
        return
    pid = msg.get("user_id", "")
    gtype = msg.get("give_type", "coins")
    amount = msg.get("amount", 0)
    item_id = msg.get("item_id", "")

    if not pid:
        await send(ws_id, pack(type="gm_give_result", ok=False, msg="未指定玩家ID"))
        return

    save = cloud_load(pid)
    detail = ""

    if gtype == "coins":
        coins = int(save.get("coins", 0)) + int(amount)
        save["coins"] = coins
        detail = f"+{amount:,} 金币 (当前 {coins:,})"
    elif gtype == "item":
        owned = save.get("owned_items", "")
        owned_list = owned.split(",") if owned else []
        if item_id not in owned_list:
            owned_list.append(item_id)
        save["owned_items"] = ",".join(owned_list)
        detail = f"获得物品 {item_id}"

    cloud_save(pid, save, skip_check=True)

    if pid in player_registry:
        pw_id = player_registry[pid]
        await send(pw_id, pack(type="gm_sync", data=save))
        detail += " (在线，已推送)"

    print(f"[GM] 向 {pid} 发放: {detail}")
    await send(ws_id, pack(type="gm_give_result", ok=True, detail=detail))


async def handle(ws: WebSocketServerProtocol, path: str = "/"):
    ws_id = new_id()
    conns[ws_id] = {"ws": ws, "room": None, "role": None, "auth_player_id": None}
    # 热路径：公网端口天天被扫，这两句以前是**每连接一次**同步写 journald
    _tprint("conn_up", f"[>] {ws_id} 已连接  （在线: {len(conns)}）")

    auth_passed = (AUTH_TOKEN == "")   # 未配置 token 则跳过认证

    try:
        async for raw in ws:
            # 消息大小限制
            if len(raw) > MAX_MSG_SIZE:
                await send(ws_id, error("消息过大"))
                continue

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await send(ws_id, error("无效 JSON"))
                continue

            # ── 连接认证 ──────────────────────────────────
            if not auth_passed:
                token = msg.get("auth_token", "")
                if token != AUTH_TOKEN:
                    await send(ws_id, error("认证失败：token 不匹配"))
                    _tprint("auth_fail", f"[!] {ws_id} 认证失败")
                    return
                auth_passed = True
                await send(ws_id, pack(type="auth_ok"))

            t = msg.get("type", "")
            if t == "ping":
                await send(ws_id, pack(type="pong", ts=time.time(),
                                       rooms=len(rooms), conns=len(conns)))
                continue

            handler = HANDLERS.get(t)
            if handler:
                await handler(ws_id, msg)
            else:
                await send(ws_id, error(f"未知消息类型: {t}"))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await on_leave(ws_id)
        _clear_battle(ws_id)
        # 清理匹配队列状态
        game = match_ctx.pop(ws_id, None)
        if game and ws_id in match_queues.get(game, []):
            match_queues[game].remove(ws_id)
        match_pets.pop(ws_id, None)
        # 清理玩家注册表（仅认证过的连接才注册过）
        c = conns.get(ws_id, {})
        pid = c.get("auth_player_id", "")
        if pid and player_registry.get(pid) == ws_id:
            player_registry.pop(pid, None)
        conns.pop(ws_id, None)
        _tprint("conn_down", f"[<] {ws_id} 断开  （在线: {len(conns)}）")


# ── 空闲房间清理 ──────────────────────────────────────────
async def cleaner():
    """周期清理：过期会话文件 + 超时空闲房间。

    ⚠️ 整轮必须包在 try 里。原来没有保护 —— 里面任何一处抛异常都会让这个
    `while True` 协程**永久死掉**（房间不再回收、会话不再清理），
    异常还会向上冒到 main() 的 `async with` 把服务一起带走。
    周期性任务"死一次就再也不跑"是最难查的一类故障：进程还活着、CPU 也正常。
    """
    while True:
        await asyncio.sleep(60)
        try:
            # ⚠️ **挪到线程池**：`clean_sessions()` 要逐个 `open()`/`json.load()`
            #    sessions/ 下的每个文件 —— 纯同步 I/O。跑在事件循环上的话，
            #    卡多久完全取决于**文件有多少**（这台的 sessions/ 从 8 月攒到现在），
            #    而它就卡在「每 60 秒一次」这个节骨眼上。
            #    低频任务丢给线程零代价，循环不再被它顶住。
            await asyncio.get_running_loop().run_in_executor(None, clean_sessions)
            _gm_prune_rate_limit()      # 清过期的 GM 失败记录/封禁（防按 IP 缓慢泄漏）
            now = time.time()
            expired = [c for c, r in rooms.items()
                       if now - r["last_active"] > ROOM_TIMEOUT]
            for code in expired:
                room = rooms.pop(code, None)
                if not room:
                    continue
                for m in room.get("members", []):
                    ws_id = m.get("ws_id")
                    if ws_id and ws_id in conns:
                        await send(ws_id, pack(type="room_timeout"))
                        conns[ws_id]["room"] = None
                        conns[ws_id]["role"] = None
                print(f"[!] 房间 {code} 超时清理", flush=True)
        except Exception as e:
            # 不 raise：单轮失败不该终结整个清理任务
            print(f"[!] cleaner 本轮异常（跳过，下轮继续）: {type(e).__name__}: {e}",
                  flush=True)


async def watchdog_feeder():
    """给 systemd 看门狗喂狗（unit 里配了 WatchdogSec 才生效）。

    为什么需要：如果事件循环卡死，进程**还活着**但不再响应任何连接 ——
    这时 systemd 的 `Restart=always` 完全不起作用（它只在进程退出时重启），
    表现就是"服务器断联，手动重启一下才好"。

    这个协程跑在**同一个事件循环**上：循环一卡，喂狗自然停止，
    systemd 就会按 WatchdogSec 把它杀掉重启。服务健康时它永远不触发。
    """
    sock_path = os.environ.get("NOTIFY_SOCKET", "")
    if not sock_path:
        return          # 没配 watchdog 就静默跳过，不影响本地/容器运行
    print("[*] systemd 看门狗已启用", flush=True)
    while True:
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            try:
                s.connect(sock_path)
                s.send(b"WATCHDOG=1")
            finally:
                s.close()
        except Exception:
            pass
        await asyncio.sleep(20)     # 必须小于 WatchdogSec


# ── 启动 ──────────────────────────────────────────────────
async def main():
    migrate_passwords()
    port = int(os.environ.get("PORT", 8765))
    print(f"桌面勇者 WebSocket 服务器")
    print(f"监听端口: {port}")
    print(f"本地测试地址: ws://localhost:{port}")
    if AUTH_TOKEN:
        print(f"已启用 Token 认证")
    if GM_SECRET:
        # 顺带报密钥长度 —— 运维一眼能看出「是不是还在用短密钥」。
        # ⚠️ 只报**长度**，不报内容（这句会进 journald，日志可能被人看到）。
        print(f"GM 已启用（密钥长度 {len(GM_SECRET)}）")
        if GM_RATE_LIMIT:
            print(f"GM 失败限流: 开（窗口 {GM_FAIL_WINDOW}s / {GM_FAIL_MAX} 次 → 封 {GM_FAIL_BLOCK}s，按来源 IP）")
        else:
            print("GM 失败限流: 关（默认；失败只记日志不封禁）"
                  + ("（密钥偏短，建议改成 32 位以上随机串）" if len(GM_SECRET) < 16 else ""))
    else:
        print(f"GM 已禁用（未设置 GM_SECRET 环境变量）")
    print("-" * 40)

    # ── 起服务（绑定失败要重试）────────────────────────────
    # 旧实例刚退出时端口未必立刻释放（TIME_WAIT / 信号被延迟处理），
    # 这时直接 bind 会抛 OSError 98 秒退，systemd 每 RestartSec=5 拉起一次、
    # 每次秒退 → 刷屏式崩溃循环，而且会连带把「谁在占端口」这件事藏起来。
    # 实测线上曾因此崩了 67 次（MainPID 变成 0，systemd 失去跟踪，
    # 而端口被一个不受管的遗留进程顶着 → 表现为「服务时好时坏」）。
    # 退避重试就能扛过那几秒；真的被别人长期占用时，最后一次仍然会抛出，
    # 只是这时候日志里已经有足够线索了。
    #
    # ⚠️ errno 必须同时认 Linux 和 Windows：Linux 是 98(EADDRINUSE)，
    #    Windows 是 10048(WSAEADDRINUSE)。只判 98 的话补丁在 Windows 上等于没写
    #    ——本地一测就发现直接 re-raise 了。
    EADDRINUSE = (98, 10048)
    for attempt in range(6):
        try:
            async with websockets.serve(handle, "0.0.0.0", port):
                # cleaner 与 watchdog 都跑在同一个事件循环上：
                # 循环一卡，喂狗停止 → systemd 按 WatchdogSec 杀掉重启
                await asyncio.gather(cleaner(), watchdog_feeder())
            return
        except OSError as e:
            if e.errno not in EADDRINUSE or attempt == 5:
                raise
            print(f"[!] 端口 {port} 被占用，2 秒后重试（{attempt + 1}/6）"
                  f" —— 若反复出现，用 `ss -lptn 'sport = :{port}'` 查占用者",
                  flush=True)
            await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(main())
