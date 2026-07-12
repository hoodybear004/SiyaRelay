import asyncio
import json
import logging
from collections import defaultdict

import websockets

HOST = "0.0.0.0"
PORT = 8765

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

clients = defaultdict(set)
hosts = {}
client_info = {}
revoke_acks = {}
locks = defaultdict(asyncio.Lock)


async def send_json(ws, data):
    try:
        await ws.send(json.dumps(data, ensure_ascii=False))
        return True
    except Exception:
        return False


async def broadcast(group, data, exclude=None):
    dead = []
    for ws in list(clients[group]):
        if ws is exclude:
            continue
        if not await send_json(ws, data):
            dead.append(ws)

    for ws in dead:
        clients[group].discard(ws)
        client_info.pop(ws, None)


def get_viewer_count(group):
    return sum(
        1
        for ws in clients.get(group, set())
        if client_info.get(ws, {}).get("role") == "viewer"
    )


async def announce_viewer_count(group):
    host = hosts.get(group)
    if not host:
        return

    await send_json(host, {
        "type": "viewer_count",
        "count": get_viewer_count(group)
    })


async def announce_host(group):
    host = hosts.get(group)
    info = client_info.get(host, {}) if host else {}

    await broadcast(group, {
        "type": "host_status",
        "present": bool(host),
        "name": info.get("name", "") if host else ""
    })

    await announce_viewer_count(group)


async def grant_host(group, ws):
    hosts[group] = ws
    await send_json(ws, {"type": "host_granted"})
    await announce_host(group)

    logging.info(
        "Host granted group=%s name=%s",
        group,
        client_info.get(ws, {}).get("name")
    )


async def claim_host(group, ws):
    async with locks[group]:
        current = hosts.get(group)

        if current is ws:
            await send_json(ws, {"type": "host_granted"})
            await announce_viewer_count(group)
            return

        if current is None or current.closed:
            await grant_host(group, ws)
            return

        ack = asyncio.Event()
        revoke_acks[current] = ack

        await send_json(current, {
            "type": "host_revoke",
            "message": "다른 사용자가 호스트 변경을 요청했습니다."
        })

        try:
            await asyncio.wait_for(ack.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            logging.warning("Old host did not acknowledge within 5 seconds")
        finally:
            revoke_acks.pop(current, None)

        if hosts.get(group) is current:
            hosts.pop(group, None)

        await send_json(current, {"type": "host_replaced"})
        await grant_host(group, ws)


async def handler(ws):
    group = None

    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=10)
        hello = json.loads(raw)

        if hello.get("type") != "hello":
            await send_json(ws, {
                "type": "error",
                "message": "잘못된 접속 요청입니다."
            })
            return

        group = str(hello.get("group", "")).strip()

        if not group:
            await send_json(ws, {
                "type": "error",
                "message": "그룹 키가 없습니다."
            })
            return

        clients[group].add(ws)
        client_info[ws] = {
            "name": str(hello.get("name", "사용자")),
            "role": str(hello.get("role", "viewer")),
            "client_id": str(hello.get("client_id", "")),
            "group": group,
        }

        current = hosts.get(group)
        info = client_info.get(current, {}) if current else {}

        await send_json(ws, {
            "type": "host_status",
            "present": bool(current),
            "name": info.get("name", "") if current else ""
        })

        # 시청자가 들어오면 현재 호스트에게 인원수 즉시 전송
        await announce_viewer_count(group)

        async for raw in ws:
            data = json.loads(raw)
            kind = data.get("type")

            if kind == "claim_host":
                await claim_host(group, ws)

            elif kind == "host_revoke_ack":
                ack = revoke_acks.get(ws)
                if ack:
                    ack.set()

            elif kind == "caption":
                if hosts.get(group) is not ws:
                    await send_json(ws, {
                        "type": "error",
                        "message": "현재 호스트가 아니어서 자막을 전송할 수 없습니다."
                    })
                    continue

                await broadcast(group, {
                    "type": "caption",
                    "speaker": str(data.get("speaker", "")),
                    "text": str(data.get("text", ""))
                }, exclude=ws)

            elif kind == "ping":
                await send_json(ws, {"type": "pong"})

    except Exception as exc:
        logging.info("Client disconnected: %s", exc)

    finally:
        info = client_info.pop(ws, {})

        if group:
            clients[group].discard(ws)

            if hosts.get(group) is ws:
                hosts.pop(group, None)
                await announce_host(group)

                logging.info(
                    "Host disconnected group=%s name=%s",
                    group,
                    info.get("name")
                )
            else:
                # 시청자가 나가면 호스트에게 갱신된 인원수 전송
                await announce_viewer_count(group)

            if not clients[group]:
                clients.pop(group, None)


async def main():
    async with websockets.serve(
        handler,
        HOST,
        PORT,
        ping_interval=15,
        ping_timeout=15
    ):
        logging.info(
            "시아의개고생 중계서버 실행: ws://%s:%s",
            HOST,
            PORT
        )
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
