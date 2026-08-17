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
import os
import secrets
import sys
import time

import websockets
from websockets.server import WebSocketServerProtocol

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

# ── 随机匹配队列 ─────────────────────────────────────────
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

def cloud_save(token: str, data: dict):
    data["_ts"] = time.time()
    try:
        with open(_save_path(token), "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception:
        pass

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
    member = {"ws_id": ws_id, "pet": pet, "joined_at": time.time()}
    rooms[code] = {
        "members":     [member],
        "created_at":  time.time(),
        "last_active": time.time(),
    }
    conns[ws_id]["room"] = code
    conns[ws_id]["role"] = "host"
    await send(ws_id, pack(type="room_created", code=code, members=[]))
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
    if len(room["members"]) >= MAX_PLAYERS:
        await send(ws_id, error(f"房间 [{code}] 已满（最多{MAX_PLAYERS}人）"))
        return

    pet = msg.get("pet", {})
    if len(json.dumps(pet)) > MAX_PET_SIZE:
        pet = {"name": "好友的宠物"}

    new_member = {"ws_id": ws_id, "pet": pet, "joined_at": time.time()}
    room["members"].append(new_member)
    room["last_active"] = time.time()
    conns[ws_id]["room"] = code
    conns[ws_id]["role"] = "guest"

    # 告诉加入者房间信息（不包括自己）
    others = [m for m in room["members"] if m["ws_id"] != ws_id]
    await send(ws_id, pack(type="room_joined", code=code, members=others))
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

    # 从成员列表移除
    room["members"] = [m for m in room["members"] if m["ws_id"] != ws_id]

    if not room["members"]:
        # 房间空了，删除
        del rooms[code]
        print(f"[-] 房间 {code} 解散（无成员）")
        return

    # 通知其他人
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


async def on_pvp_move(ws_id: str, msg: dict):
    payload = {k: v for k, v in msg.items() if k != "type"}
    payload["from_ws_id"] = ws_id
    await broadcast_to_room(ws_id, pack(type="pvp_move", **payload))


async def on_game_invite(ws_id: str, msg: dict):
    target_id = msg.get("target_ws_id", "")
    payload = {k: v for k, v in msg.items() if k != "type"}
    payload["from_ws_id"] = ws_id
    if target_id:
        await send(target_id, pack(type="game_invite", **payload))
    else:
        await broadcast_to_room(ws_id, pack(type="game_invite", **payload))


async def on_bubble(ws_id: str, msg: dict):
    payload = {k: v for k, v in msg.items() if k != "type"}
    payload["from_ws_id"] = ws_id
    await broadcast_to_room(ws_id, pack(type="remote_bubble", **payload))


async def on_pvp_accept(ws_id: str, msg: dict):
    target = msg.get("target_ws_id", "")
    payload = {k: v for k, v in msg.items() if k not in ("type", "target_ws_id")}
    payload["from_ws_id"] = ws_id
    if target:
        await send(target, pack(type="pvp_accept", **payload))
    else:
        await broadcast_to_room(ws_id, pack(type="pvp_accept", **payload))


async def on_pvp_decline(ws_id: str, msg: dict):
    target = msg.get("target_ws_id", "")
    if target:
        await send(target, pack(type="pvp_decline", from_ws_id=ws_id))
    else:
        await broadcast_to_room(ws_id, pack(type="pvp_decline", from_ws_id=ws_id))


# ── 随机匹配 ──────────────────────────────────────────────
async def on_match_request(ws_id: str, msg: dict):
    """加入匹配队列，若已有同类型等待者则配对"""
    game = msg.get("game", "weak")
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
        code = gen_code()
        rooms[code] = {
            "members": [
                {"ws_id": peer_id, "pet": peer_pet, "joined_at": time.time()},
                {"ws_id": ws_id, "pet": pet, "joined_at": time.time()},
            ],
            "created_at": time.time(),
            "last_active": time.time(),
        }
        conns[peer_id]["room"] = code
        conns[peer_id]["role"] = "host"
        conns[ws_id]["room"] = code
        conns[ws_id]["role"] = "guest"
        seed = secrets.token_hex(3)
        await send(peer_id, pack(type="match_found", game=game, is_host=True,
                                 seed=seed, opponent=pet))
        await send(ws_id, pack(type="match_found", game=game, is_host=False,
                               seed=seed, opponent=peer_pet))
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
    cloud_save(pid, msg.get("data", {}))
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
    """GM 认证：未配置 GM_SECRET 时一律拒绝，配置后常量时间比对"""
    if not GM_SECRET:
        return False
    return hmac.compare_digest(str(msg.get("gm_secret", "")), GM_SECRET)


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
    if not _gm_auth_check(msg):
        await send(ws_id, pack(type="gm_gen_invite_result", ok=False, msg="GM认证失败"))
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
    if not _gm_auth_check(msg):
        await send(ws_id, pack(type="gm_invite_count_result", ok=False, msg="GM认证失败"))
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
    if not _gm_auth_check(msg):
        await send(ws_id, pack(type="gm_search_result", ok=False, msg="GM认证失败"))
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
    if not _gm_auth_check(msg):
        await send(ws_id, pack(type="gm_give_result", ok=False, msg="GM认证失败"))
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

    cloud_save(pid, save)

    if pid in player_registry:
        pw_id = player_registry[pid]
        await send(pw_id, pack(type="gm_sync", data=save))
        detail += " (在线，已推送)"

    print(f"[GM] 向 {pid} 发放: {detail}")
    await send(ws_id, pack(type="gm_give_result", ok=True, detail=detail))


async def handle(ws: WebSocketServerProtocol, path: str = "/"):
    ws_id = new_id()
    conns[ws_id] = {"ws": ws, "room": None, "role": None, "auth_player_id": None}
    print(f"[>] {ws_id} 已连接  （在线: {len(conns)}）")

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
                    print(f"[!] {ws_id} 认证失败")
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
        print(f"[<] {ws_id} 断开  （在线: {len(conns)}）")


# ── 空闲房间清理 ──────────────────────────────────────────
async def cleaner():
    while True:
        await asyncio.sleep(60)
        clean_sessions()
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
            print(f"[!] 房间 {code} 超时清理")


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
        print(f"GM 已启用（密钥已配置）")
    else:
        print(f"GM 已禁用（未设置 GM_SECRET 环境变量）")
    print("-" * 40)

    async with websockets.serve(handle, "0.0.0.0", port):
        await cleaner()


if __name__ == "__main__":
    asyncio.run(main())
