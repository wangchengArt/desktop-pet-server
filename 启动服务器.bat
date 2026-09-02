@echo off
echo 启动 桌面勇者 WebSocket 服务器...
echo 本地地址: ws://localhost:8765
echo 按 Ctrl+C 停止
echo.
cd /d "%~dp0"
python room_server.py
pause
