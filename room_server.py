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
    room = _new_room()
    # S2 / S4：房主坐 1 号，同样走**入座的唯一出口** ——
    # 别在这里手写列表字面量：一处手写、一处走函数，两边早晚对不上。
    seat = _add_member(room, ws_id, pet)
    rooms[code] = room
    conns[ws_id]["room"] = code
    conns[ws_id]["role"] = "host"
    _sync_member_roles(room)          # ★ S9：room 建好之后立刻抄一份
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
    if room.get("in_game"):
        await send(ws_id, error("这一局已经开始了，等下一局"))
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
    _sync_member_roles(room)          # ★ S9

    # 告诉加入者房间信息（不包括自己），以及**自己坐几号**
    others = [m for m in room["members"] if m["ws_id"] != ws_id]
    await send(ws_id, pack(type="room_joined", code=code, members=others,
                           seat=seat))
    # 告诉其他人有新人来了
    await broadcast_to_room(ws_id, pack(type="member_joined", member=new_member))
    print(f"[+] {ws_id} 加入房间 {code}（共{len(room['members'])}人）")


def _host_of(room: dict) -> str:
    """本房房主的 ws_id。**现读**，不缓存 —— 房主身份是会转移的（S7）。"""
    for m in room["members"]:
        if conns.get(m["ws_id"], {}).get("role") == "host":
            return m["ws_id"]
    return ""


def _authority_of(room: dict) -> str:
    """本房**当前权威**的 ws_id（M2-d）。

    开局时 = 房主；房主掉线后转给剩下的人 —— 那一刻「房主」与「权威」
    才第一次分开（房主是**房间**的主人，权威是**这一局引擎**的持有者）。

    ⚠️ 现读，不缓存。权威掉了要**回落到房主**：不然三条对局消息会**全都没人收**
    （房主还在、却因为 `authority` 指着一条死连接而全体失声）。
    """
    a = room.get("authority")
    return a if a and a in conns else (_host_of(room) or "")


def _in_game_room(ws_id: str):
    """取「发送者所在的对局房」。返回 `(code, room, 拒绝原因)`；没被拒时原因为空串。

    S8 的三条消息**都只在対局中才有意义** —— 大厅里发它们说明客户端状态不对，
    与其静默转发，不如明确拒掉（省得拿它去探测别人的房间）。
    """
    code = _room_of(ws_id)
    room = rooms.get(code) if code else None
    if not room:
        return None, None, "不在房间中"
    if not room.get("in_game"):
        return code, None, "本房没有正在进行的对局"
    return code, room, ""


async def on_identity_action(ws_id: str, msg: dict):
    """**客户端 → 房主**：点名 / 出牌 / 农场主技 / 结束回合（S8）。

    三条都要查，少一条这个协议就是敞开的：

      · 同房间（S1 那套）
      · 目标**必须是本房的房主** —— 不查的话，任意客户端能把「我的动作」发给
        同房的**另一个客户端**，那个客户端分不清这是权威指令还是别人的动作
      · 房间在**对局中**

    ★ 服务端还会**自己算**一个 `from_seat` 塞进去（用 S6 的座位表），
    而不是信客户端自报的座位号 —— 房主靠它认「这是谁的动作」，
    信客户端的字段等于把「我能代表几号位」交给客户端自己说。
    """
    code, room, why = _in_game_room(ws_id)
    if why:
        await send(ws_id, error(why))
        return
    target = msg.get("target_ws_id", "")
    if not _same_room(ws_id, target):
        await _reject_cross_room(ws_id, target, "identity_action")
        return
    if target != _authority_of(room):
        await send(ws_id, error("对局消息只能发给当前权威"))
        return
    payload = {k: v for k, v in msg.items() if k not in ("type", "target_ws_id")}
    payload["from_ws_id"] = ws_id
    payload["from_seat"] = _seat_of_ws(room, ws_id)     # ← 服务端算的，覆盖客户端自报
    _touch_room(ws_id)                                   # S3：长局别被 cleaner 拆
    await send(target, pack(type="identity_action", **payload))


async def on_identity_state(ws_id: str, msg: dict):
    """**房主 → 其余人**：公开状态广播（S8）。

    ⚠️⚠️ **必须查发送者是不是房主**，这是本协议的要害：不查的话，任意一个
    客户端都能往全房广播伪造的「权威状态」（改血量、改棋盘、冒充翻牌），
    而别人没有任何办法分辨真假 —— 那等于把裁决权交给了每一个人。

    不回发发送者（房主本来就是状态的来源，自己不需要收回来）。
    """
    code, room, why = _in_game_room(ws_id)
    if why:
        await send(ws_id, error(why))
        return
    if ws_id != _authority_of(room):
        await send(ws_id, error("只有当前权威能广播对局状态"))
        return
    payload = {k: v for k, v in msg.items() if k != "type"}
    payload["from_ws_id"] = ws_id
    _touch_room(ws_id)
    await broadcast_to_room(ws_id, pack(type="identity_state", **payload))


async def on_identity_private(ws_id: str, msg: dict):
    """**房主 → 某一个人**：私有信息（自己的手牌 / 自己的身份 / 收网提示）（S8）。

    ★ 这一条就是身份局的**信息隔离**赖以成立的地方（策划案 §2.4）：
    「谁能看见谁的身份」在单机里是条渲染规则，到了联机必须变成**投递规则**。
    服务端在这里仍然看不懂内容（纯转发），但保证两件事：
    **只有权威能发**、**只发给同房的那一个人**。

    ⚠️ 不查发送者的话，任意客户端都能给同房的人发伪造的「你是黄鼠狼」——
    而那条信息的真假，收信人**无从验证**。
    """
    code, room, why = _in_game_room(ws_id)
    if why:
        await send(ws_id, error(why))
        return
    if ws_id != _authority_of(room):
        await send(ws_id, error("只有当前权威能发私有信息"))
        return
    target = msg.get("target_ws_id", "")
    if not _same_room(ws_id, target):
        await _reject_cross_room(ws_id, target, "identity_private")
        return
    payload = {k: v for k, v in msg.items() if k not in ("type", "target_ws_id")}
    payload["from_ws_id"] = ws_id
    _touch_room(ws_id)
    await send(target, pack(type="identity_private", **payload))


async def _migrate_authority(code: str, room: dict, gone_ws: str) -> bool:
    """★ M2-d：对局中**权威**离开 → 把这一局交给下一个在线的人。返回成不成。

    ⚠️ 只有**服务端存着快照**时才做得成（权威每 0.5 秒推一份）。没有快照
    （服务端刚重启 / 那一局还没推过第一份）就返回 False，由调用方退回老行为
    「中止本局」—— **别假装能接手**。

    ⚠️ 交出去的是**快照**，所以接手的那个人的机器上会**第一次**拥有
    「全部身份 + 全部手牌」。这是方案 B 的信任边界的**必然扩张**：
    能作弊的人从「房主」变成「房主 + 他掉线时的接任者」——
    换来的是一局不会因为一个人拔网就作废。
    ⚠️ 界面上那句披露（E7）说的是「房主」，权威迁移之后**要跟着改**。
    """
    snap = room.get("snapshot")
    if not isinstance(snap, dict):
        return False
    _cand = [m for m in room["members"]
             if not m.get("offline") and m["ws_id"] != gone_ws]
    if not _cand:
        return False
    _next = min(_cand, key=lambda m: m["seat"])
    _new_ws = _next["ws_id"]
    _prev_seat = _seat_of_ws(room, gone_ws)
    room["authority"] = _new_ws
    conns[_new_ws]["role"] = "host"
    _sync_member_roles(room)
    room["last_active"] = time.time()
    _seat = int(_next.get("seat") or 0)
    await send(_new_ws, pack(
        type="authority_granted", snapshot=snap, seat=_seat,
        seat_map=_seat_map_now(room), n_players=room.get("n_players"),
        # 新权威要把这几席交给 AI（含刚走的那个）—— 与 C11 同一个动作，
        # 只是他知道的时机比 `member_offline` 早，所以在这里一并告诉他。
        offline_seats=[int(m["seat"]) for m in room["members"] if m.get("offline")]
                      + ([int(_prev_seat)] if _prev_seat else []),
        prev_seat=int(_prev_seat)))
    await broadcast_to_room_all(code, pack(
        type="authority_changed", authority_ws_id=_new_ws, seat=_seat,
        members=room["members"]))
    print(f"[+] 权威迁移：{gone_ws} 离开 → {_new_ws} 接手（{_seat} 号位），"
          f"对局继续", flush=True)
    return True


async def on_authority_snapshot(ws_id: str, msg: dict):
    """★ M2-d：**权威**定期把整局快照推给服务端存着（服务端**不解析**它）。

    为什么必须**定期推**、而不是掉线那一刻才算：房主拔网 = **他那一台的状态
    已经没了** ——「掉线时再生成快照」物理上不可能。

    为什么存服务端、而不是直接推给「下一任权威」：快照里有**全部身份与全部手牌**。
    实时推给某个玩家 = 把能作弊的人从 1 个扩成 2 个（而且他在掉线前就能看了）；
    给服务端则信任边界**一个字节都不变** —— 服务端拿到的是一份**不解析**的 blob，
    这也不违反它「不跑游戏逻辑」的定位（它只是**存**，不是**算**）。

    ⚠️ 只有**当前权威**能写。不查的话，任何客户端都能塞一份伪造的整局状态，
    而掉线时新权威会**照着它复活这一局**。
    """
    code, room, why = _in_game_room(ws_id)
    if why:
        await send(ws_id, error(why))
        return
    if ws_id != _authority_of(room):
        await send(ws_id, error("只有当前权威能提交对局快照"))
        return
    _snap = msg.get("snapshot")
    if not isinstance(_snap, dict):
        return
    room["snapshot"] = _snap
    _touch_room(ws_id)


async def _abort_game_and_transfer_host(code: str, room: dict, gone_ws: str):
    """对局中房主离开：**中止本局**，房主身份转给剩下的人，房间留着继续用。

    ⚠️⚠️ 为什么是「中止」而不是「其余人继续打完」：本方案是**房主权威**
    （策划案 §3.1）—— **只有房主那一台跑引擎**，其余人手里只有「投影过的状态」。
    房主一断，**没有第二台机器知道真相**，对局物理上无法继续。
    「继续打完」要**权威迁移**（把权威侧状态或动作日志交给新权威），
    那是 M2 的活；这里先明确中止，别让玩家对着一个不动的牌桌干等。

    （**非房主**离开是另一回事：房主那一台把那一席 `is_ai` 置真即可，
    引擎现成支持混合座位，对局**继续** —— 那条路不需要服务端做任何事，
    只要别解散房间就行，而大厅那条路本来就不解散。）
    """
    # ⚠️⚠️ **先把走的人从成员表里摘掉**。
    #    M2-d 之后，`on_leave` 的对局分支是「先试迁移、再走这里」—— 那条路
    #    会**留着他等回座**（C12），所以走到「中止」这一条时他还在名单上。
    #    不摘掉的话 `min(members)` 会选中**已经离开的那个人**当新房主，
    #    然后广播一个指向死连接的 `host_ws_id`（实测：`verify_host_leave` 当场 8 条红）。
    room["members"] = [m for m in room["members"] if m["ws_id"] != gone_ws]
    # 房间回到「可以用」的状态：剩下的人不用退房重进就能再开一局
    room["in_game"] = False
    room["n_players"] = None
    room["seat_map"] = None
    room["authority"] = None       # ★ M2-d：别让下一局读到这一局的权威/快照
    room["snapshot"] = None
    # ★ 2026-08-16：**准备状态也要复位**。不复位的话，下一局谁都不用再准备
    #   就能开 —— 而用户要的是「所有人都点了准备才能开」。
    for _m in room["members"]:
        _m["ready"] = False
    room["last_active"] = time.time()
    # 房主身份转移：交给**座位号最小**的剩下的人。
    # ⚠️ 别用「成员列表的第一个」—— 那个顺序取决于加入/离开的历史，
    #    同一个局面在不同机器上不一定一样；座位号是确定性的。
    # ⚠️ S9：**必须排除掉线的人**（成员表会保留 offline 的人，C12 要认回座位）。
    _alive = [m for m in room["members"] if not m.get("offline")]
    if not _alive:
        # 一个人都没了（只剩掉线的 ghost）→ 房间没用了
        if not room["members"]:
            del rooms[code]
            print(f"[-] 房主 {gone_ws} 对局中离开 → 房间 {code} 空了，删除")
        return
    _next = min(_alive, key=lambda m: m["seat"])
    conns[_next["ws_id"]]["role"] = "host"
    _sync_member_roles(room)          # ★ S9：转移也要抄进成员表
    # 先补一条 `member_left`（老客户端靠它清成员列表/收宠物），
    # 再发 `game_aborted`（新客户端靠它知道「本局没了、回大厅」）
    await broadcast_to_room_all(code, pack(type="member_left", ws_id=gone_ws,
                                           members=room["members"]))
    await broadcast_to_room_all(code, pack(type="game_aborted", reason="host_left",
                                           host_ws_id=_next["ws_id"],
                                           host_seat=int(_next.get("seat") or 0),
                                           members=room["members"]))
    print(f"[-] 房主 {gone_ws} 对局中离开 → 房间 {code} 中止本局，"
          f"房主转给 {_next['ws_id']}（剩{len(room['members'])}人）", flush=True)


async def on_leave(ws_id: str):
    c = conns.get(ws_id)
    if not c or not c["room"]:
        return
    code = c["room"]
    room = rooms.get(code)
    if not room:
        # ★ 2026-08-16：房已经没了（比如刚被解散）——
        #   **必须把自己那个悬空引用清掉**，否则这个人再也
        #   建不了房、进不了房（「已在房间中」），而「退出房间」也救不了他
        #   —— 两边都是这个早退，于是死锁。
        c["room"] = None
        c["role"] = None
        return
    role = c["role"]
    c["room"] = None; c["role"] = None
    _clear_battle(ws_id)

    # ★ S9/C12（2026-10-15）：**对局中**不把成员删掉，只标 `offline`。
    #
    # 为什么：删了他就**认不回原座位**了 —— 重连时无据可查（`members` 里没有他，
    # `seat_map` 里那条还指着一条已经不存在的 ws_id）。而「立刻托管 + 可回座」
    # 这两条要求他的座位 / pet / 身份都留着。
    #
    # ⚠️ **大厅里维持原行为**（删 + `member_left`）：那里的语义就是「人走了」，
    #    而且 `verify_room_lobby` / `verify_host_leave` 的断言靠它。
    #
    # ★ M2-d：房主（= 权威）在对局中离开，**先试权威迁移** —— 服务端存着他
    #    定期推来的快照，交给座位号最小的另一个在线的人，这一局**接着打完**。
    #    没有快照可交（服务端刚重启 / 那一局还没推过第一份）才退回老行为
    #    「中止本局」—— **别假装能接手**。
    if room.get("in_game"):
        if role == "host" and not await _migrate_authority(code, room, ws_id):
            await _abort_game_and_transfer_host(code, room, ws_id)
            return
        _seat = _seat_of_ws(room, ws_id)
        for m in room["members"]:
            if m["ws_id"] == ws_id:
                m["offline"] = True
                if m.get("role") == "host":
                    # 权威已经交出去了，他回来是**普通成员**（C12 那条路能接住）
                    m["role"] = "guest"
        # 全掉光了 → 房间没用了（不留一间全是 ghost 的房）
        if not any(not m.get("offline") for m in room["members"]):
            del rooms[code]
            print(f"[-] 房间 {code} 解散（对局中全体离线）")
            return
        room["last_active"] = time.time()
        await broadcast_to_room_all(code, pack(type="member_offline", seat=_seat,
                                               ws_id=ws_id,
                                               members=room["members"]))
        print(f"[-] {ws_id}({role}) 对局中掉线 → 房间 {code} 的 {_seat} 号位标记离线"
              f"（成员保留，等他回座）")
        return

    # 从成员列表移除
    room["members"] = [m for m in room["members"] if m["ws_id"] != ws_id]

    if not room["members"]:
        # 房间空了，删除
        del rooms[code]
        print(f"[-] 房间 {code} 解散（无成员）")
        return

    if role == "host":
        # ★ S7：**对局中**房主离开 → 不解散。中止本局、把房主转给剩下的人，
        #   房间留着（剩下的人可以直接再开一局）。
        if room.get("in_game"):
            await _abort_game_and_transfer_host(code, room, ws_id)
            return
        # ★ 2026-10-20（用户 #6）：**大厅里房主退出 → 房主继承，不再解散**。
        #   规则（用户已拍）：**2 号位优先**，2 号不在则按座位号从小到大
        #   找第一个在线的人。只剩自己一人时上一步已经删房了。
        _next = _inherit_host_seat(room)
        if _next is None:
            # 剩下的人都不可用（理论上到不了）→ 维持老行为：解散
            await _dispose_room(code, room)
            return
        _new_ws = next((m["ws_id"] for m in room["members"]
                        if int(m.get("seat") or 0) == _next), "")
        _c = conns.get(_new_ws)
        if _c:
            _c["role"] = "host"
        _sync_member_roles(room)
        # 先广播「有人走了」（旧客户端靠它清好友宠物/列表），再广播「谁接了房主」。
        await broadcast_to_room_all(code, pack(type="member_left", ws_id=ws_id,
            members=room["members"]))
        await broadcast_to_room_all(code, pack(type="host_changed",
            host_seat=_next, host_ws_id=_new_ws, members=room["members"],
            in_game=False))
        print(f"[+] 房主离开 {code} → 房主转给 {_next} 号位（{_new_ws}）")
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


def _new_room() -> dict:
    """一间空房的状态 —— **只在这里定义一次**。

    建房 / 匹配建房都造它。散着写早晚有一处漏字段，而漏掉的若是
    `in_game` 这种「没有它也能跑、只是大厅行为不对」的字段，最难查。
    """
    return {"members": [], "created_at": time.time(), "last_active": time.time(),
            "in_game": False,      # S5：开局之后不许再有人进来
            "n_players": None,     # S5：本局几人（4 / 5），未开局为 None
            # S6：开局那一刻钉下来的座位表，**单一事实来源**。
            # `[{"seat":1,"ws_id":"..."} | {"seat":3,"ws_id":None}]`，
            # `ws_id is None` = 这一席由 AI 补位。
            "seat_map": None,
            # ★ M2-d：**当前权威**的 ws_id。开局时 = 房主；房主掉线后转给
            #   座位号最小的另一个在线的人（那时「房主」与「权威」才第一次分开）。
            "authority": None,
            # ★ M2-d：权威定期推来的**整局快照**（不解释的 blob）。
            #   ⚠️ 存一份在服务端，而不是实时推给「下一任权威」——
            #   快照里有**全部身份与全部手牌**，推给某个玩家等于把能作弊的人
            #   从 1 个扩成 2 个（而且他在掉线前就能看了）。
            #   给服务端则信任边界**一个字节都不变**（服务端不解析它）。
            "snapshot": None}


def _seat_map(room: dict, n_players: int) -> list:
    """开局时的座位表：`[{"seat": 1, "ws_id": "..."} | {"seat": 3, "ws_id": None}, ...]`

    `ws_id is None` = **这一席由 AI 补位**（引擎侧 `Seat.is_ai` 现成支持混合座位）。

    ★ 游戏座位**直接沿用房间座位号**，不做重映射 —— 玩家在大厅里看到
    「你是 2 号位」，进局之后还是 2 号位。重映射会让「几号位」在开局那一刻
    悄悄变一次，而身份局的座位（回合顺序、点名、结算列表）是要读一整局的东西。
    """
    humans = {m["seat"]: m["ws_id"] for m in room["members"]}
    return [{"seat": i, "ws_id": humans.get(i)}
            for i in range(1, n_players + 1)]


def _seat_map_now(room: dict) -> list:
    """本房**当前**的座位表。

    未开局返回**空** —— 「哪几席是 AI」在开局前是**不确定的**（取决于房主
    最后选 4 还是 5、以及那时谁还在座），所以这里不猜、也不给预览。
    已开局返回开局那一刻钉下来的那一份。
    """
    return list(room.get("seat_map") or [])


def _seat_of_ws(room: dict, ws_id: str) -> int:
    """对局里这条连接占几号席；不在座位表里返回 0。

    ⚠️ 与 `_seat_of` 的区别：`_seat_of` 读的是**大厅成员表**（开局前，
    谁坐几号由加入顺序定），这个读的是**开局钉下来的座位表**。
    对局开始之后，一切以座位表为准 —— 大厅成员表在那一刻就冻结了。
    """
    for x in _seat_map_now(room):
        if x.get("ws_id") == ws_id:
            return int(x["seat"])
    return 0


def _is_ai_seat(room: dict, seat: int) -> bool:
    """这一席是不是 AI 补位（S6）。不在座位表里返回 False。"""
    for x in _seat_map_now(room):
        if int(x["seat"]) == seat:
            return x.get("ws_id") is None
    return False


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
    # ★ S9：`player_id` **从连接上读**，不从消息里读 —— 那是服务端自己的事实
    _pid = (conns.get(ws_id) or {}).get("auth_player_id")
    room["members"].append(_member(ws_id, pet, seat, player_id=_pid))
    room["last_active"] = time.time()
    return seat


def _make_match_room(pairs: list):
    """给一批匹配到的玩家建一间临时房。

    返回 `(房间码, {ws_id: 座位号})`；**坐不下返回 None，且一个房间都不留下**
    —— 别留半间房（那会让后续 `_same_room` 之类的校验看到一间残缺的房）。
    """
    code = gen_code()
    room = _new_room()
    seats = {}
    for wid, pt in pairs:
        seat = _add_member(room, wid, pt)
        if seat == 0:
            return None
        seats[wid] = seat
    rooms[code] = room
    return code, seats


def _member(ws_id: str, pet, seat: int, player_id=None, role="guest") -> dict:
    """房间成员的结构 —— **只在这里定义一次**。

    建房 / 加入 / 匹配建房三处都要造它。散着写早晚有一处漏字段，
    而漏掉的若是 `seat` 这种「没有它也能跑、只是悄悄错位」的字段，最难查。
    """
    return {"ws_id": ws_id, "pet": pet, "joined_at": time.time(), "seat": seat,
            # S5：大厅里的「准备好了」。默认 False —— 让「没准备」是显式动作，
            # 而不是「没操作过也算准备」（后者在 4/5 人房里等于没有这个闸）
            "ready": False,
            # ★ S9（M2）：**能在重连之后把人认回来的身份**。
            #   `ws_id` 每次连接都重新随机（`new_id()`），拿它认不回原位；
            #   `player_id` 来自 `conns[ws_id]["auth_player_id"]` —— 那是
            #   `resume_session` **校验过 token** 才写上的，**绝不信客户端自报**。
            "player_id": player_id,
            # ★ S9：房主 / 普通成员。重连要把这个身份一起还回去 ——
            #   那一刻旧的连接已经没了（`conns[old_ws_id]` 被 `finally` 清掉），
            #   只能从成员表里读。
            "role": role,
            # ★ C12：断线中。**对局中**掉线不把人从成员表删掉，只标这个 ——
            #   删了他就认不回原座位了。
            "offline": False}


def _sync_member_roles(room: dict) -> None:
    """把 `conns[...]["role"]` 抄进成员表（S9）。

    为什么要有这一步：`role` 原本**只**存在 `conns` 里，而重连时旧连接已经没了。
    抄一份到成员表，`on_rejoin` 才能把身份还回去。
    """
    for m in room["members"]:
        _r = (conns.get(m["ws_id"]) or {}).get("role")
        if _r:
            m["role"] = _r


def _inherit_host_seat(room: dict):
    """大厅房主退出时，**谁接房主**（用户 2026-10-20 #6）。

    规则：**2 号位优先**；2 号位不在（或已掉线）则按座位号从小到大
    找第一个在线的人。返回座位号，没有可用的人返回 None。

    ⚠️ 为什么不直接「取最小座位号」：用户明确要 2 号优先。两者在
    「2 号还在」时结果相同，不同的是 2 号早退之后 —— 取最小号会选 3 号，
    这正好也是用户接受的递推规则。所以「2 号优先 + 最小号兜底」写成一条
    即可，但**先判 2 号**是明确的意图，别在将来被"优化"掉。
    """
    _alive = [m for m in room["members"] if not m.get("offline")]
    if not _alive:
        return None
    for m in _alive:
        if int(m.get("seat") or 0) == 2:
            return 2
    return min(int(m.get("seat") or 0) for m in _alive)


async def _dispose_room(code: str, room: dict) -> None:
    """**真正解散**一个房间：通知 + 清掉所有成员的房间引用 + 删房。

    ★ 2026-10-20 抽出：原来这段只写在 `on_leave` 的大厅房主分支里。
    现在「房主退出」变成了**继承**，解散只剩两条路（只剩自己 / 显式解散），
    两处必须走同一段清理 —— 尤其 `conns[...]["room"]` 那条：
    不清的话他们发 `leave_room` 时 `rooms.get(code)` 为 None **直接 return**，
    一个字段都不清 → **死锁**（除了重连没有任何出路）。
    """
    await broadcast_to_room_all(code, pack(type="room_disbanded"))
    for _m in list(room.get("members") or []):
        _c = conns.get(_m.get("ws_id") or "")
        if _c:
            _c["room"] = None
            _c["role"] = None
    rooms.pop(code, None)
    print(f"[-] 房间 {code} 解散（通知{len(room.get('members') or [])}人退出）")


async def on_disband_room(ws_id: str, msg: dict):
    """★ 2026-10-20：房主**显式解散**房间。

    ⚠️ 为什么需要单独一条消息：`#6` 让「房主离开」变成「转让房主」之后，
    如果「解散」还复用 `leave_room`，房主就**再也没有办法解散房间**了。
    客户端把「解散房间」按钮指向这里，把「退出房间」指向 `leave_room`。
    """
    c = conns.get(ws_id)
    if not c or not c["room"]:
        await send(ws_id, error("不在房间中"))
        return
    code = c["room"]
    room = rooms.get(code)
    if not room:
        await send(ws_id, error("房间不存在"))
        return
    if c.get("role") != "host":
        await send(ws_id, error("只有房主能解散房间"))
        return
    if room.get("in_game"):
        # 对局中解散 → 先让所有人回到大厅语义，再解散
        await broadcast_to_room_all(code, pack(type="game_aborted",
            reason="host_disband", members=room["members"]))
    await _dispose_room(code, room)


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


async def on_ready(ws_id: str, msg: dict):
    """大厅里报「准备好了 / 取消准备」（S5）。

    广播给**全房（含自己）**：准备状态是公开信息，而且客户端要立刻回显
    自己那一位的状态，让发送者也走同一条路比「本地先改、等广播对账」简单。
    """
    code = _room_of(ws_id)
    room = rooms.get(code) if code else None
    if not room:
        await send(ws_id, error("不在房间中"))
        return
    if room.get("in_game"):
        await send(ws_id, error("对局已经开始了"))
        return
    want = bool(msg.get("ready", True))
    for m in room["members"]:
        if m["ws_id"] == ws_id:
            m["ready"] = want
            break
    _touch_room(ws_id)
    await broadcast_to_room_all(code, pack(type="member_ready", ws_id=ws_id,
                                          ready=want, members=room["members"]))


async def on_start_game(ws_id: str, msg: dict):
    """房主开局：定人数（**只能 4 / 5**），不够的席位由 **AI 补位**（S5）。

    ⚠️ 服务端**不跑引擎** —— 本方案的权威模型是「房主权威」
    （`策划案/弱鸡杀_联机方案_v1.md` §3.1）。这里只做三件事：
    **校验、定座位表、广播**；真正建 `IdentitySession` 的是房主端。
    """
    code = _room_of(ws_id)
    room = rooms.get(code) if code else None
    if not room:
        await send(ws_id, error("不在房间中"))
        return
    if conns[ws_id]["role"] != "host":
        await send(ws_id, error("只有房主能开局"))
        return
    if room.get("in_game"):
        await send(ws_id, error("对局已经开始了"))
        return
    try:
        n = int(msg.get("n_players") or 0)
    except (TypeError, ValueError):
        n = 0
    # ⚠️ **只允许 4 / 5**：引擎的 `STATS` 只有这两张表
    #    （`weak_identity.py` 的 `STATS = {4: {...}, 5: {...}}`）。
    #    放一个 3 或 6 过去，房主端建 session 会直接 KeyError —— 而且是
    #    **房主那一台崩**，其他人只看到「开局没反应」。所以闸设在这里。
    if n not in (4, 5):
        await send(ws_id, error("人数只能是 4 或 5"))
        return
    if len(room["members"]) > n:
        await send(ws_id, error(f"房间里已经有 {len(room['members'])} 个人，"
                                f"装不进 {n} 人局"))
        return
    # 座位号是沿用房间号的，所以「最大座位号」也不能超过人数
    # （比如 2 号走了、来个人补了 5 号，这时开 4 人局就装不下）
    _max_seat = max(m["seat"] for m in room["members"])
    if _max_seat > n:
        await send(ws_id, error(f"最大座位号是 {_max_seat}，装不进 {n} 人局"))
        return
    # 除房主外都要先准备 —— **房主的「开局」本身就是他的准备**（最少摩擦：
    # 否则房主要先点一次准备、再点一次开局，那一步没有信息量）
    _notready = [m for m in room["members"]
                 if not m.get("ready") and m["ws_id"] != ws_id]
    if _notready:
        await send(ws_id, error(f"还有 {len(_notready)} 人没准备"))
        return
    room["in_game"] = True
    room["n_players"] = n
    room["last_active"] = time.time()
    # ★ S6：先钉进房间，再广播**同一份** —— 别广播时现算一次、之后又现算一次，
    #   那样「客户端看到的」与「服务端记的」就有了两个来源，早晚分叉。
    room["seat_map"] = _seat_map(room, n)
    room["authority"] = ws_id              # ★ M2-d：开局时权威 = 房主
    room["snapshot"] = None                # 上一局的快照不能留给这一局
    await broadcast_to_room_all(code, pack(
        type="game_start", n_players=n, host_ws_id=ws_id,
        seat_map=room["seat_map"]))
    print(f"[+] 房间 {code} 开局：{n} 人局，真人 {len(room['members'])} 个"
          f"（AI 补 {n - len(room['members'])} 席）", flush=True)


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
        # ★ 2026-08-16（扫描挖出来的）：**匹配建房也要登记 `battles`**。
        #   原来只有好友邀请那条路（`on_pvp_accept`）写它，而
        #   `on_game_invite` 的两道互斥守卫（「你正在对战中」/「对方正在对战」）
        #   读的正是它 → **正在打匹配局的人仍可以被邀请、也能去邀请别人**，
        #   两局叠在一起。
        battles[peer_id] = ws_id
        battles[ws_id] = peer_id
        conns[peer_id]["room"] = code
        conns[peer_id]["role"] = "host"
        conns[ws_id]["room"] = code
        conns[ws_id]["role"] = "guest"
        _sync_member_roles(rooms[code])   # ★ S9
        seed = secrets.token_hex(3)
        # ★ 2026-08-16：**把临时房的房间码也带上**。原来不带，客户端只好
        #   `_room = msg.get("code", "match")` —— **凭空造一个叫 "match" 的房号**，
        #   而它再没有地方会被清掉 → `in_room` 永久为真
        #   （右键菜单一直多一项「退出房间」），而且重连时会去
        #   `rejoin_room("match")`。给了真房号之后，匹配局重连也能认回座位。
        await send(peer_id, pack(type="match_found", game=game, is_host=True,
                                 code=code, seat=_seats[peer_id], seed=seed,
                                 opponent=pet))
        await send(ws_id, pack(type="match_found", game=game, is_host=False,
                               code=code, seat=_seats[ws_id], seed=seed,
                               opponent=peer_pet))
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
    "ready":         lambda ws_id, msg: on_ready(ws_id, msg),
    "start_game":    lambda ws_id, msg: on_start_game(ws_id, msg),
    # ★ 2026-10-20（用户 #6）：大厅「退出房间」= 转让房主，「解散房间」单独一条。
    "disband_room":  lambda ws_id, msg: on_disband_room(ws_id, msg),
    # S8：对局消息 —— 客户端↔权威。三条的收信人各不相同，服务端逐条查权限
    "identity_action":  lambda ws_id, msg: on_identity_action(ws_id, msg),
    "identity_state":   lambda ws_id, msg: on_identity_state(ws_id, msg),
    "identity_private": lambda ws_id, msg: on_identity_private(ws_id, msg),
    # ★ S9/C12：重连回座（认人靠已认证的 player_id，不信客户端自报）
    "rejoin_room":      lambda ws_id, msg: on_rejoin(ws_id, msg),
    # ★ M2-d：权威定期推整局快照（服务端只存不解析）
    "authority_snapshot": lambda ws_id, msg: on_authority_snapshot(ws_id, msg),
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


async def on_rejoin(ws_id: str, msg: dict):
    """★ S9/C12：**重连回座** —— 把断线的人放回他原来那一席。

    三条硬要求，少一条这个口子就是敞开的：

      · **必须已认证** —— 认人靠 `conns[ws_id]["auth_player_id"]`（那是
        `resume_session` **校验过 token** 才写上的）。**绝不信客户端自报的 id**：
        信了的话，任何人报一个 player_id 就能**顶掉别人的座位**，而身份局里
        「坐到哪一席」就是「你是谁」。
      · 房间必须**正在对局中** —— 大厅里没有「回座」这回事，重新 `join_room` 就行。
      · 那个 player_id 必须**确实**是本房某条 `offline` 成员，否则明确拒绝
        （**给回执**，不静默）。

    ⚠️ 房主**不在**这条路上：对局中房主离开走 S7 的「中止本局」。等权威迁移
    做完，房主也能回座 —— 那时这里按 `m["role"]` 把 host 身份一起还回去即可
    （`role` 已经抄进成员表了，就是为那一刻留的）。
    """
    if conns[ws_id]["room"]:
        await send(ws_id, error("已在房间中，请先离开"))
        return
    pid = conns[ws_id].get("auth_player_id")
    if not pid:
        await send(ws_id, pack(type="rejoin_result", ok=False,
                               reason="未登录（重连回座要靠账号认人）"))
        return
    code = str(msg.get("code") or "").upper().strip()
    room = rooms.get(code)
    if not room:
        # `gone` 让客户端知道「这间房没了」→ 它该把自己的 `_room` 清掉，
        # 而不是留着一个陈旧值（那会让界面假装还在房里）。
        await send(ws_id, pack(type="rejoin_result", ok=False, gone=True,
                               reason="房间已经不在了"))
        return
    if not room.get("in_game"):
        # `lobby` = 房间还在、只是不在对局里 → 客户端退回普通的 `join_room`
        # （大厅里没有「回座」这回事，座位是重新分配的）。
        await send(ws_id, pack(type="rejoin_result", ok=False, lobby=True,
                               reason="这一局已经结束了"))
        return
    _m = next((m for m in room["members"]
               if m.get("player_id") == pid and m.get("offline")), None)
    if _m is None:
        await send(ws_id, pack(type="rejoin_result", ok=False, lobby=True,
                               reason="没有你的座位"))
        return

    _seat = int(_m.get("seat") or 0)
    _m["ws_id"] = ws_id              # 换上新的连接
    _m["offline"] = False
    conns[ws_id]["room"] = code
    conns[ws_id]["role"] = _m.get("role") or "guest"
    # ⚠️ `seat_map` 里那条原来指着**旧的 ws_id** —— 不换的话，
    #    S8 之后所有按座位号定向的消息都会发给一个不存在的连接（**静默丢失**）。
    for _x in (room.get("seat_map") or []):
        if int(_x.get("seat") or 0) == _seat:
            _x["ws_id"] = ws_id
    room["last_active"] = time.time()

    await send(ws_id, pack(type="rejoin_result", ok=True, seat=_seat, code=code,
                           role=conns[ws_id]["role"],
                           # 房主的 ws_id 也一并回 —— 客户端要重建牌桌（上行发给谁）。
                           # 原来它靠的是自己记的那份，重开进程就没了。
                           host_ws_id=_host_of(room),
                           n_players=room.get("n_players"),
                           seat_map=_seat_map_now(room)))
    await broadcast_to_room_all(code, pack(type="member_online", seat=_seat,
                                           ws_id=ws_id, members=room["members"]))
    print(f"[+] {ws_id} 重连回座 → 房间 {code} 的 {_seat} 号位")


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
