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
import json
import os
import secrets
import time

import websockets
from websockets.server import WebSocketServerProtocol

# ── 配置 ────────────────────────────────────────────────────
AUTH_TOKEN   = os.environ.get("AUTH_TOKEN", "").strip()
MAX_MSG_SIZE = 64 * 1024        # 64KB 单条消息上限
MAX_CODE_LEN = 10               # 房间码最大长度
MAX_PET_SIZE = 2048             # pet_info 最大字节数
ROOM_TIMEOUT = 30 * 60          # 30 分钟无活动自动清理
CODE_CHARS   = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
GEN_MAX_RETRY = 100             # 房间码生成最大重试

# ── 全局状态 ──────────────────────────────────────────────
rooms: dict[str, dict] = {}
conns: dict[str, dict] = {}

# ── 云存档 ──────────────────────────────────────────────────
SAVE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "saves")
os.makedirs(SAVE_DIR, exist_ok=True)

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


async def broadcast_peer(ws_id: str, data: str):
    """转发给同房间的另一方"""
    c = conns.get(ws_id)
    if not c or not c["room"]:
        return
    room = rooms.get(c["room"])
    if not room:
        return
    peer_id = room["guest_ws_id"] if c["role"] == "host" else room["host_ws_id"]
    if peer_id:
        await send(peer_id, data)


# ── 消息处理器 ────────────────────────────────────────────
async def on_create(ws_id: str, msg: dict):
    if conns[ws_id]["room"]:
        await send(ws_id, error("已在房间中，请先离开"))
        return
    code = gen_code()
    rooms[code] = {
        "host_ws_id":  ws_id,
        "guest_ws_id": None,
        "host_pet":    msg.get("pet", {}),
        "guest_pet":   None,
        "created_at":  time.time(),
        "last_active": time.time(),
    }
    conns[ws_id]["room"] = code
    conns[ws_id]["role"] = "host"
    await send(ws_id, pack(type="room_created", code=code))
    print(f"[+] 房间 {code} 由 {ws_id} 创建")


async def on_join(ws_id: str, msg: dict):
    if conns[ws_id]["room"]:
        await send(ws_id, error("已在房间中，请先离开"))
        return
    code = msg.get("code", "").upper().strip()
    # 输入校验
    if len(code) > MAX_CODE_LEN:
        await send(ws_id, error(f"房间码过长（最多{MAX_CODE_LEN}字符）"))
        return
    room = rooms.get(code)
    if not room:
        await send(ws_id, error(f"房间 [{code}] 不存在"))
        return
    if room["guest_ws_id"] is not None:
        await send(ws_id, error(f"房间 [{code}] 已满"))
        return

    pet = msg.get("pet", {})
    # pet_info 大小限制
    if len(json.dumps(pet)) > MAX_PET_SIZE:
        pet = {"name": "好友的宠物"}

    room["guest_ws_id"] = ws_id
    room["guest_pet"]   = pet
    room["last_active"] = time.time()
    conns[ws_id]["room"] = code
    conns[ws_id]["role"] = "guest"

    await send(ws_id, pack(
        type     = "room_joined",
        code     = code,
        host_pet = room["host_pet"],
    ))
    await send(room["host_ws_id"], pack(
        type      = "guest_arrived",
        guest_pet = pet,
    ))
    print(f"[+] {ws_id} 加入房间 {code}")


async def on_leave(ws_id: str):
    c = conns.get(ws_id)
    if not c or not c["room"]:
        return
    code = c["room"]
    room = rooms.get(code)
    if not room:
        return

    if c["role"] == "host":
        if room["guest_ws_id"]:
            await send(room["guest_ws_id"], pack(type="host_left"))
            guest = conns.get(room["guest_ws_id"])
            if guest:
                guest["room"] = None
                guest["role"] = None
        del rooms[code]
        print(f"[-] 房间 {code} 解散（房主离开）")
    else:
        room["guest_ws_id"] = None
        room["guest_pet"]   = None
        await send(room["host_ws_id"], pack(type="guest_left"))
        print(f"[-] 访客 {ws_id} 离开房间 {code}")

    c["room"] = None
    c["role"] = None


async def on_sync(ws_id: str, msg: dict):
    c = conns.get(ws_id)
    if not c or not c["room"]:
        return
    room = rooms.get(c["room"])
    if room:
        room["last_active"] = time.time()
    payload = {k: v for k, v in msg.items() if k != "type"}
    await broadcast_peer(ws_id, pack(type="remote_sync", **payload))


async def on_pvp_move(ws_id: str, msg: dict):
    payload = {k: v for k, v in msg.items() if k != "type"}
    await broadcast_peer(ws_id, pack(type="pvp_move", **payload))


async def on_game_invite(ws_id: str, msg: dict):
    payload = {k: v for k, v in msg.items() if k != "type"}
    await broadcast_peer(ws_id, pack(type="game_invite", **payload))


async def on_bubble(ws_id: str, msg: dict):
    payload = {k: v for k, v in msg.items() if k != "type"}
    await broadcast_peer(ws_id, pack(type="remote_bubble", **payload))


# ── 连接主处理器 ──────────────────────────────────────────
HANDLERS = {
    "create_room":   on_create,
    "join_room":     on_join,
    "leave_room":    lambda ws_id, _: on_leave(ws_id),
    "sync":          on_sync,
    "pvp_move":      on_pvp_move,
    "game_invite":   on_game_invite,
    "bubble":        on_bubble,
    "pvp_accept":    lambda ws_id, msg: broadcast_peer(
                         ws_id, pack(type="pvp_accept",
                                     **{k:v for k,v in msg.items() if k!="type"})),
    "pvp_decline":   lambda ws_id, msg: broadcast_peer(
                         ws_id, pack(type="pvp_decline")),
    # 云存档
    "sync_upload":   lambda ws_id, msg: on_sync_upload(ws_id, msg),
    "sync_download": lambda ws_id, msg: on_sync_download(ws_id, msg),
    "check_name":    lambda ws_id, msg: on_check_name(ws_id, msg),
    "login_user":    lambda ws_id, msg: on_login_user(ws_id, msg),
    "register_user": lambda ws_id, msg: on_register_user(ws_id, msg),
    "gm_search":     lambda ws_id, msg: on_gm_search(ws_id, msg),
    "gm_give":       lambda ws_id, msg: on_gm_give(ws_id, msg),
    "gm_gen_invite": lambda ws_id, msg: on_gm_gen_invite(ws_id, msg),
    "gm_invite_count": lambda ws_id, msg: on_gm_invite_count(ws_id, msg),
    "validate_invite": lambda ws_id, msg: on_validate_invite(ws_id, msg),
    "auth":          lambda ws_id, msg: on_auth(ws_id, msg),
}


async def on_auth(ws_id: str, msg: dict):
    """客户端auth消息：追踪player_id用于GM推送"""
    pid = msg.get("player_id") or msg.get("auth_token", "")
    if pid and conns[ws_id].get("player_id") != pid:
        conns[ws_id]["player_id"] = pid
        player_registry[pid] = ws_id

async def on_sync_upload(ws_id: str, msg: dict):
    """客户端上传存档"""
    token = msg.get("auth_token", ws_id)
    data = msg.get("data", {})
    cloud_save(token, data)
    await send(ws_id, pack(type="sync_uploaded", ts=time.time()))

async def on_sync_download(ws_id: str, msg: dict):
    """客户端下载存档"""
    token = msg.get("auth_token", ws_id)
    data = cloud_load(token)
    await send(ws_id, pack(type="sync_data", data=data))



# ── GM 配置 ──────────────────────────────────────────────
GM_SECRET = os.environ.get("GM_SECRET", "gm888")  # GM 工具认证密钥

# ── 邀请码管理 ──────────────────────────────────────────
INVITE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "invites")
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

# ── 玩家注册表（player_id → ws_id）───────────────────────
player_registry: dict[str, str] = {}  # 记录在线玩家的 ws_id

# ── 注册 ────────────────────────────────────────────────
REGISTER_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "register")
os.makedirs(REGISTER_DIR, exist_ok=True)

def _reg_path(name: str) -> str:
    safe = "".join(c for c in name if c.isalnum())[:32] or "default"
    return os.path.join(REGISTER_DIR, f"{safe}.json")

async def on_check_name(ws_id: str, msg: dict):
    name = msg.get("name", "").strip()
    if not name:
        await send(ws_id, pack(type="check_name_result", exists=False, msg="名字不能为空"))
        return
    exists = os.path.exists(_reg_path(name))
    await send(ws_id, pack(type="check_name_result", exists=exists))

def _find_by_uid(uid: str) -> dict | None:
    """通过 user_id 查找注册信息"""
    if not os.path.isdir(REGISTER_DIR):
        return None
    for fn in os.listdir(REGISTER_DIR):
        if not fn.endswith(".json"):
            continue
        try:
            with open(os.path.join(REGISTER_DIR, fn), encoding="utf-8") as f:
                data = json.load(f)
            if str(data.get("user_id", "")) == str(uid):
                return data
        except Exception:
            continue
    return None

async def on_login_user(ws_id: str, msg: dict):
    """验证登录：支持名字或8位ID + 密码"""
    name = msg.get("name", "").strip()
    uid = msg.get("user_id", "").strip()
    pwd = msg.get("password", "")

    if not name and not uid:
        await send(ws_id, pack(type="login_result", ok=False, msg="请输入名字或ID"))
        return

    data = None
    if uid:
        data = _find_by_uid(uid)
    if not data and name:
        path = _reg_path(name)
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                pass

    if not data:
        await send(ws_id, pack(type="login_result", ok=False, msg="账号不存在"))
        return
    if data.get("password", "") != pwd:
        await send(ws_id, pack(type="login_result", ok=False, msg="密码错误"))
        return
    await send(ws_id, pack(type="login_result", ok=True,
                           user_id=data.get("user_id", ""),
                           name=data.get("name", name)))

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
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"name": name, "password": pwd, "user_id": uid, "created_at": time.time()}, f)
        save_path = _save_path(uid)
        if not os.path.exists(save_path):
            cloud_save(uid, {"player_name": name, "player_id": uid,
                           "player_password": pwd, "coins": 5000, "_ts": time.time()})
        await send(ws_id, pack(type="register_result", ok=True))
    except Exception as e:
        await send(ws_id, pack(type="register_result", ok=False, msg=f"注册失败: {e}"))

# ── GM 命令 ─────────────────────────────────────────────
def _gm_auth_check(msg: dict) -> bool:
    return msg.get("gm_secret", "") == GM_SECRET

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
    conns[ws_id] = {"ws": ws, "room": None, "role": None}
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

            # ── 认证 / 玩家追踪 ──────────────────────────
            if not auth_passed:
                token = msg.get("auth_token", "")
                if token != AUTH_TOKEN:
                    await send(ws_id, error("认证失败：token 不匹配"))
                    print(f"[!] {ws_id} 认证失败")
                    return
                auth_passed = True
                await send(ws_id, pack(type="auth_ok"))
                # fall through 继续追踪玩家ID

            # 追踪玩家ID（从 auth 消息或 sync 消息中提取，用于GM推送）
            pid = msg.get("player_id") or msg.get("auth_token", "")
            if pid and pid != AUTH_TOKEN and conns[ws_id].get("player_id") != pid:
                conns[ws_id]["player_id"] = pid
                player_registry[pid] = ws_id

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
        # 清理玩家注册表
        c = conns.get(ws_id, {})
        pid = c.get("player_id", "")
        if pid and player_registry.get(pid) == ws_id:
            player_registry.pop(pid, None)
        conns.pop(ws_id, None)
        print(f"[<] {ws_id} 断开  （在线: {len(conns)}）")


# ── 空闲房间清理 ──────────────────────────────────────────
async def cleaner():
    while True:
        await asyncio.sleep(60)
        now = time.time()
        expired = [c for c, r in rooms.items()
                   if now - r["last_active"] > ROOM_TIMEOUT]
        for code in expired:
            room = rooms.pop(code, None)
            if not room:
                continue
            for ws_id in (room["host_ws_id"], room["guest_ws_id"]):
                if ws_id and ws_id in conns:
                    await send(ws_id, pack(type="room_timeout"))
                    conns[ws_id]["room"] = None
                    conns[ws_id]["role"] = None
            print(f"[!] 房间 {code} 超时清理")


# ── 启动 ──────────────────────────────────────────────────
async def main():
    port = int(os.environ.get("PORT", 8765))
    print(f"桌面勇者 WebSocket 服务器")
    print(f"监听端口: {port}")
    print(f"本地测试地址: ws://localhost:{port}")
    if AUTH_TOKEN:
        print(f"已启用 Token 认证")
    print("-" * 40)

    async with websockets.serve(handle, "0.0.0.0", port):
        await cleaner()


if __name__ == "__main__":
    asyncio.run(main())
