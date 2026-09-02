@echo off
cd /d "%~dp0server"
echo ============================================
echo   桌面勇者服务器 - ngrok 公网穿透
echo   让好友通过互联网连接你的本地服务器
echo ============================================
echo.
echo [步骤说明]
echo 1. 先下载 ngrok：https://ngrok.com/download  （免费）
echo 2. 注册账号，复制你的 authtoken
echo 3. 运行一次：ngrok config add-authtoken 你的token
echo 4. 然后双击本脚本启动
echo.

:: 检查 ngrok 是否存在
where ngrok >nul 2>&1
if %errorlevel% neq 0 (
    echo 未找到 ngrok！
    echo 请先下载：https://ngrok.com/download
    echo 下载解压后把 ngrok.exe 放到：C:\Windows 或当前目录
    pause & exit
)

echo [1/2] 启动本地服务器（后台）...
start /b python room_server.py

echo [2/2] 启动 ngrok 穿透...
echo 等待 ngrok 启动后，复制 wss:// 地址写入 config.json
echo.
echo 示例：把 config.json 中的 server_url 改为：
echo   "server_url": "wss://xxxx-xxxx.ngrok-free.app"
echo.
ngrok tcp 8765

pause
