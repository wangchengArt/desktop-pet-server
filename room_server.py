"""
桌面勇者 —— 房间中转服务器
职责：房间管理 + 消息转发（不处理游戏逻辑，仅做透明代理）

本地测试：python room_server.py
部署到 Render.com：
  1. 推到 GitHub 仓库的 server/ 目录
  2. Render New Web Service → 根目录选 server/
  3. 启动命令：python room_server.py
  4. 自动获得 wss://xxx.onrender.com 地址
"""
import asyncio
import json
import os
import secrets
import time

import websockets
from websockets.server import WebSocketServerProtocol

# ── 全局状态 ──────────────────────────────────────────────
# rooms[code] = {"host_ws_id", "guest_ws_id", "host_pet", "guest_pet",
#                "created_at", "last_active"}
rooms: dict[str, dict] = {}

# conns[ws_id] = {"ws": WebSocket, "room": code|None, "role": "host"|"guest"|None}
conns: dict[str, dict] = {}

ROOM_TIMEOUT = 30 * 60   # 30 分钟无活动自动清理
CODE_CHARS   = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # 去掉易混淆字符


# ── 工具函数 ──────────────────────────────────────────────
def new_id() -> str:
    return secrets.token_hex(6)

def gen_code() -> str:
    code = "".join(secrets.choice(CODE_CHARS) for _ in range(6))
    return code if code not in rooms else gen_code()

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
    room = rooms.get(code)
    if not room:
        await send(ws_id, error(f"房间 [{code}] 不存在"))
        return
    if room["guest_ws_id"] is not None:
        await send(ws_id, error(f"房间 [{code}] 已满"))
        return

    pet = msg.get("pet", {})
    room["guest_ws_id"] = ws_id
    room["guest_pet"]   = pet
    room["last_active"] = time.time()
    conns[ws_id]["room"] = code
    conns[ws_id]["role"] = "guest"

    # 通知访客：加入成功，附带房主宠物信息
    await send(ws_id, pack(
        type     = "room_joined",
        code     = code,
        host_pet = room["host_pet"],
    ))
    # 通知房主：有新访客
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
        # 房主离开：通知访客，解散房间
        if room["guest_ws_id"]:
            await send(room["guest_ws_id"], pack(type="host_left"))
            conns[room["guest_ws_id"]]["room"] = None
            conns[room["guest_ws_id"]]["role"] = None
        del rooms[code]
        print(f"[-] 房间 {code} 解散（房主离开）")
    else:
        # 访客离开：通知房主
        room["guest_ws_id"] = None
        room["guest_pet"]   = None
        await send(room["host_ws_id"], pack(type="guest_left"))
        print(f"[-] 访客 {ws_id} 离开房间 {code}")

    c["room"] = None
    c["role"] = None


async def on_sync(ws_id: str, msg: dict):
    """宠物位置/状态同步 —— 透明转发"""
    c = conns.get(ws_id)
    if not c or not c["room"]:
        return
    rooms[c["room"]]["last_active"] = time.time()
    # 重新打包避免 type 字段冲突
    payload = {k: v for k, v in msg.items() if k != "type"}
    await broadcast_peer(ws_id, pack(type="remote_sync", **payload))


async def on_pvp_move(ws_id: str, msg: dict):
    """小游戏回合指令 —— 透明转发"""
    payload = {k: v for k, v in msg.items() if k != "type"}
    await broadcast_peer(ws_id, pack(type="pvp_move", **payload))


async def on_game_invite(ws_id: str, msg: dict):
    """游戏邀请 —— 透明转发"""
    payload = {k: v for k, v in msg.items() if k != "type"}
    await broadcast_peer(ws_id, pack(type="game_invite", **payload))


async def on_bubble(ws_id: str, msg: dict):
    """气泡消息 —— 透明转发"""
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
    # 对战邀请应答 —— 透明转发
    "pvp_accept":    lambda ws_id, msg: broadcast_peer(
                         ws_id, pack(type="pvp_accept",
                                     **{k:v for k,v in msg.items() if k!="type"})),
    "pvp_decline":   lambda ws_id, msg: broadcast_peer(
                         ws_id, pack(type="pvp_decline")),
}

async def handle(ws: WebSocketServerProtocol, path: str = "/"):
    ws_id = new_id()
    conns[ws_id] = {"ws": ws, "room": None, "role": None}
    print(f"[>] {ws_id} 已连接  （在线: {len(conns)}）")

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await send(ws_id, error("无效 JSON"))
                continue

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
            room = rooms.pop(code)
            for ws_id in (room["host_ws_id"], room["guest_ws_id"]):
                if ws_id and ws_id in conns:
                    await send(ws_id, pack(type="room_timeout"))
                    conns[ws_id]["room"] = None
            print(f"[!] 房间 {code} 超时清理")


# ── 启动 ──────────────────────────────────────────────────
async def main():
    port = int(os.environ.get("PORT", 8765))
    print(f"桌面勇者 WebSocket 服务器")
    print(f"监听端口: {port}")
    print(f"本地测试地址: ws://localhost:{port}")
    print("-" * 40)

    async with websockets.serve(handle, "0.0.0.0", port):
        await cleaner()


if __name__ == "__main__":
    asyncio.run(main())
