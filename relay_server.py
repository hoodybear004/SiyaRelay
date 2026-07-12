import asyncio
import json
import logging
import os
from collections import defaultdict

import websockets

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8765"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

clients = defaultdict(set)
hosts = {}
client_info = {}
revoke_acks = {}
locks = defaultdict(asyncio.Lock)

# 동일 viewer가 재접속했을 때 중복 연결 제거
viewer_sessions = {}


async def send_json(ws, data):
    try:
        await ws.send(json.dumps(data, ensure_ascii=False))
        return True
    except Exception:
        return False


async def close_safely(ws, code=1000, reason=""):
    try:
        await ws.close(code=code, reason=reason)
    except Exception:
        pass


async def remove_client(ws):
    info = client_info.pop(ws, {})
    group = info.get("group")

    if group:
        clients[group].discard(ws)

    if info.get("role") == "viewer":
        key = (group, info.get("client_id", ""))
        if viewer_sessions.get(key) is ws:
            viewer_sessions.pop(key, None)

    return info


async def broadcast(group, data, exclude=None):
    dead = []

    for ws in list(clients.get(group, set())):
        if ws is exclude:
            continue

        if getattr(ws, "closed", False):
            dead.append(ws)
            continue

        if not await send_json(ws, data):
            dead.append(ws)

    for ws in dead:
        await remove_client(ws)


def get_viewer_count(group):
    unique_ids = set()
    anonymous_count = 0

    for ws in list(clients.get(group, set())):
        info = client_info.get(ws, {})

        if info.get("role") != "viewer":
            continue

        if getattr(ws, "closed", False):
            continue

        client_id = str(info.get("client_id", "")).strip()

        if client_id:
            unique_ids.add(client_id)
        else:
            anonymous_count += 1

    return len(unique_ids) + anonymous_count


async def announce_viewer_count(group):
    host = hosts.get(group)

    if not host or getattr(host, "closed", False):
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
        "present": bool(host and not getattr(host, "closed", False)),
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

        if current is None or getattr(current, "closed", False):
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


async def register_viewer_session(ws, group, client_id):
    client_id = str(client_id or "").strip()

    if not client_id:
        return

    key = (group, client_id)
    old_ws = viewer_sessions.get(key)

    if old_ws and old_ws is not ws and not getattr(old_ws, "closed", False):
        logging.info(
            "Replacing duplicate viewer session group=%s client_id=%s",
            group,
            client_id
        )
        await close_safely(old_ws, code=4001, reason="duplicate viewer session")
        await remove_client(old_ws)

    viewer_sessions[key] = ws


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

        role = str(hello.get("role", "viewer"))
        client_id = str(hello.get("client_id", ""))

        clients[group].add(ws)
        client_info[ws] = {
            "name": str(hello.get("name", "사용자")),
            "role": role,
            "client_id": client_id,
            "group": group,
        }

        if role == "viewer":
            await register_viewer_session(ws, group, client_id)

        current = hosts.get(group)
        info = client_info.get(current, {}) if current else {}

        await send_json(ws, {
            "type": "host_status",
            "present": bool(current and not getattr(current, "closed", False)),
            "name": info.get("name", "") if current else ""
        })

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
        info = await remove_client(ws)

        if group:
            if hosts.get(group) is ws:
                hosts.pop(group, None)
                await announce_host(group)

                logging.info(
                    "Host disconnected group=%s name=%s",
                    group,
                    info.get("name")
                )
            else:
                await announce_viewer_count(group)

            if not clients.get(group):
                clients.pop(group, None)


async def main():
    async with websockets.serve(
        handler,
        HOST,
        PORT,
        ping_interval=10,
        ping_timeout=20,
        close_timeout=2
    ):
        logging.info(
            "시아의개고생 중계서버 실행: ws://%s:%s",
            HOST,
            PORT
        )
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
