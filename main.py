import asyncio
import websockets
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
import httpx

app = FastAPI()
TARGET_HTTP = "https://holy-silence-fb62.ali-dd6.workers.dev/eyJqdW5rIjoiaE1lcXcwblFiIiwicHJvdG9jb2wiOiJ2bCIsIm1vZGUiOiJwcm94eWlwIiwicGFuZWxJUHMiOlsicHlpcC55Z2tray5kcGRucy5vcmciXX0=?ed=2560"
TARGET_WS   = "wss://holy-silence-fb62.ali-dd6.workers.dev/eyJqdW5rIjoiaE1lcXcwblFiIiwicHJvdG9jb2wiOiJ2bCIsIm1vZGUiOiJwcm94eWlwIiwicGFuZWxJUHMiOlsicHlpcC55Z2tray5kcGRucy5vcmciXX0=?ed=2560"

@app.get("/test-ws")
async def test_ws():
    try:
        async with websockets.connect(TARGET_WS) as ws:
            return {"status": "ok - WebSocket works!"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.api_route("/{path:path}", methods=["GET","POST","PUT","DELETE","PATCH","OPTIONS"])
async def http_proxy(request: Request, path: str):
    headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.request(
            method=request.method,
            url=TARGET_HTTP,
            headers=headers,
            content=await request.body(),
        )
    return StreamingResponse(resp.aiter_bytes(), status_code=resp.status_code,
                             headers=dict(resp.headers))

@app.websocket("/{path:path}")
async def ws_proxy(client_ws: WebSocket, path: str):
    await client_ws.accept()

    skip = {"host","upgrade","connection","sec-websocket-key",
            "sec-websocket-version","sec-websocket-extensions"}
    headers = [(k, v) for k, v in client_ws.headers.items()
               if k.lower() not in skip]

    try:
        async with websockets.connect(TARGET_WS, additional_headers=headers) as server_ws:

            async def c2s():
                try:
                    while True:
                        data = await client_ws.receive_bytes()
                        await server_ws.send(data)
                except Exception:
                    await server_ws.close()

            async def s2c():
                try:
                    async for msg in server_ws:
                        if isinstance(msg, bytes):
                            await client_ws.send_bytes(msg)
                        else:
                            await client_ws.send_text(msg)
                except Exception:
                    pass

            await asyncio.gather(c2s(), s2c())

    except (WebSocketDisconnect, websockets.exceptions.ConnectionClosed):
        pass
    except Exception as e:
        await client_ws.close()
