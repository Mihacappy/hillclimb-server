import asyncio
import json
import random
import string
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse


app = FastAPI()

PROTOCOL_VERSION = 1
MAX_PLAYERS = 2


def make_room_code(length=4):
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(random.choice(alphabet) for _ in range(length))


@dataclass
class Player:
    player_id: str
    nickname: str
    websocket: WebSocket
    ready: bool = False
    finished: bool = False
    finish_order: Optional[int] = None
    finish_time: Optional[float] = None
    fuel_used: float = 0.0
    max_horizontal_speed: float = 0.0
    damage_taken: float = 0.0
    eliminated: bool = False
    elimination_reason: str = ""
    collected_fuel: Set[str] = field(default_factory=set)


@dataclass
class Room:
    code: str
    host_id: str
    players: Dict[str, Player] = field(default_factory=dict)
    level: str = ""
    race_started: bool = False
    race_start_server_time: Optional[float] = None
    finish_count: int = 0
    race_over: bool = False


rooms: Dict[str, Room] = {}
rooms_lock = asyncio.Lock()


async def send_json(ws: WebSocket, payload):
    await ws.send_text(json.dumps(payload, separators=(",", ":")))


async def room_broadcast(room: Room, payload, exclude_id=None):
    dead = []

    for pid, player in list(room.players.items()):
        if pid == exclude_id:
            continue

        try:
            await send_json(player.websocket, payload)
        except Exception:
            dead.append(pid)

    for pid in dead:
        room.players.pop(pid, None)


def room_state(room: Room):
    return {
        "type": "room_state",
        "room": room.code,
        "host_id": room.host_id,
        "level": room.level,
        "race_started": room.race_started,
        "players": [
            {
                "player_id": p.player_id,
                "nickname": p.nickname,
                "ready": p.ready,
                "finished": p.finished,
                "eliminated": p.eliminated,
                "elimination_reason": p.elimination_reason,
            }
            for p in room.players.values()
        ],
    }



def reset_player_race_state(player: Player, ready=False):
    player.ready = ready
    player.finished = False
    player.finish_order = None
    player.finish_time = None
    player.fuel_used = 0.0
    player.max_horizontal_speed = 0.0
    player.damage_taken = 0.0
    player.eliminated = False
    player.elimination_reason = ""
    player.collected_fuel.clear()


def player_result(player: Player):
    return {
        "player_id": player.player_id,
        "nickname": player.nickname,
        "finished": player.finished,
        "eliminated": player.eliminated,
        "elimination_reason": player.elimination_reason,
        "finish_order": player.finish_order,
        "finish_time": player.finish_time,
        "fuel_used": player.fuel_used,
        "max_horizontal_speed": player.max_horizontal_speed,
        "damage_taken": player.damage_taken,
    }


async def finalize_race(room: Room, winner_id=None, reason="finished"):
    if room.race_over:
        return

    room.race_over = True
    room.race_started = False

    players = list(room.players.values())

    # Finished players first, then survivors, then eliminated players.
    players.sort(
        key=lambda p: (
            0 if p.finished else (1 if not p.eliminated else 2),
            p.finish_order if p.finish_order is not None else 999,
        )
    )

    await room_broadcast(room, {
        "type": "race_results",
        "winner": winner_id,
        "reason": reason,
        "players": [player_result(p) for p in players],
    })


async def maybe_finish_race(room: Room):
    if room.race_over or not room.players:
        return

    players = list(room.players.values())
    finished = [p for p in players if p.finished]
    eliminated = [p for p in players if p.eliminated]
    active = [
        p for p in players
        if not p.finished and not p.eliminated
    ]

    # Everybody reached a terminal state.
    if not active:
        if finished:
            winner = min(
                finished,
                key=lambda p: p.finish_order or 999,
            )
            await finalize_race(
                room,
                winner.player_id,
                "finish",
            )
        else:
            await finalize_race(
                room,
                None,
                "all_eliminated",
            )
        return



async def maybe_start_countdown(room: Room):
    if room.race_started or room.race_over:
        return

    if len(room.players) != MAX_PLAYERS:
        return

    if not all(p.ready for p in room.players.values()):
        return

    if not room.level:
        return

    await room_broadcast(room, {
        "type": "countdown",
        "seconds": 3,
    })

    # Server owns the start instant.
    start_time = time.time() + 3.0
    room.race_started = True
    room.race_start_server_time = start_time

    await room_broadcast(room, {
        "type": "race_start",
        "server_time": start_time,
        "level": room.level,
    })


async def leave_room(room: Room, player_id: str):
    room.players.pop(player_id, None)

    if not room.players:
        rooms.pop(room.code, None)
        return

    if room.host_id == player_id:
        room.host_id = next(iter(room.players))

    room.race_started = False
    room.race_over = False
    room.race_start_server_time = None
    room.finish_count = 0

    for p in room.players.values():
        reset_player_race_state(p, ready=False)

    await room_broadcast(room, {
        "type": "player_left",
        "player_id": player_id,
    })
    await room_broadcast(room, room_state(room))


@app.get("/")
async def root():
    return PlainTextResponse(
        f"Hill Climb multiplayer server online. Protocol {PROTOCOL_VERSION}"
    )


@app.get("/health")
async def health():
    return {
        "ok": True,
        "protocol": PROTOCOL_VERSION,
        "rooms": len(rooms),
    }


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()

    room: Optional[Room] = None
    player: Optional[Player] = None

    try:
        # First message must identify the client.
        raw = await ws.receive_text()
        hello = json.loads(raw)

        if hello.get("type") != "hello":
            await send_json(ws, {
                "type": "error",
                "message": "First packet must be hello",
            })
            await ws.close()
            return

        if int(hello.get("protocol", -1)) != PROTOCOL_VERSION:
            await send_json(ws, {
                "type": "error",
                "message": "Protocol mismatch",
                "server_protocol": PROTOCOL_VERSION,
            })
            await ws.close()
            return

        nickname = str(hello.get("nickname", "")).strip()[:20]
        if not nickname:
            nickname = "Player"

        player_id = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in range(10)
        )

        player = Player(
            player_id=player_id,
            nickname=nickname,
            websocket=ws,
        )

        await send_json(ws, {
            "type": "hello_ok",
            "player_id": player_id,
            "protocol": PROTOCOL_VERSION,
        })

        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            msg_type = msg.get("type")

            # ---------------------------------------------------------
            # Room creation
            # ---------------------------------------------------------
            if msg_type == "create_room":
                if room is not None:
                    await send_json(ws, {
                        "type": "error",
                        "message": "Already in a room",
                    })
                    continue

                async with rooms_lock:
                    code = make_room_code()
                    while code in rooms:
                        code = make_room_code()

                    room = Room(
                        code=code,
                        host_id=player.player_id,
                    )
                    room.players[player.player_id] = player
                    rooms[code] = room

                await send_json(ws, {
                    "type": "room_created",
                    "room": code,
                })
                await room_broadcast(room, room_state(room))

            # ---------------------------------------------------------
            # Room join
            # ---------------------------------------------------------
            elif msg_type == "join_room":
                if room is not None:
                    await send_json(ws, {
                        "type": "error",
                        "message": "Already in a room",
                    })
                    continue

                code = str(msg.get("room", "")).upper().strip()

                async with rooms_lock:
                    candidate = rooms.get(code)

                    if candidate is None:
                        await send_json(ws, {
                            "type": "error",
                            "message": "Room not found",
                        })
                        continue

                    if len(candidate.players) >= MAX_PLAYERS:
                        await send_json(ws, {
                            "type": "error",
                            "message": "Room is full",
                        })
                        continue

                    room = candidate
                    room.players[player.player_id] = player

                await send_json(ws, {
                    "type": "room_joined",
                    "room": room.code,
                })

                await room_broadcast(room, {
                    "type": "player_joined",
                    "player_id": player.player_id,
                    "nickname": player.nickname,
                })
                await room_broadcast(room, room_state(room))

            # Anything below here requires a room.
            elif room is None:
                await send_json(ws, {
                    "type": "error",
                    "message": "Not in a room",
                })

            # ---------------------------------------------------------
            # Host selects level
            # ---------------------------------------------------------
            elif msg_type == "set_level":
                if room.host_id != player.player_id:
                    await send_json(ws, {
                        "type": "error",
                        "message": "Only host can select level",
                    })
                    continue

                room.level = str(msg.get("level", "")).strip()

                for p in room.players.values():
                    p.ready = False

                await room_broadcast(room, room_state(room))

            # ---------------------------------------------------------
            # Ready
            # ---------------------------------------------------------
            elif msg_type == "ready":
                player.ready = bool(msg.get("ready", True))
                await room_broadcast(room, room_state(room))
                await maybe_start_countdown(room)

            # ---------------------------------------------------------
            # Vehicle snapshots
            # ---------------------------------------------------------
            elif msg_type == "snapshot":
                if not room.race_started:
                    continue

                # Do not trust this for authoritative results; this packet is
                # only for remote vehicle rendering.
                payload = {
                    "type": "snapshot",
                    "player_id": player.player_id,
                    "x": msg.get("x", 0),
                    "y": msg.get("y", 0),
                    "angle": msg.get("angle", 0),
                    "vx": msg.get("vx", 0),
                    "vy": msg.get("vy", 0),
                    "wheel_spin": msg.get("wheel_spin", 0),
                    "wheels": msg.get("wheels", []),
                    "health": msg.get("health", 0),
                    "fuel": msg.get("fuel", 0),
                    "vehicle_index": msg.get("vehicle_index", 0),
                    "rpm": msg.get("rpm", 0),
                    "throttle": msg.get("throttle", 0),
                    "stalled": msg.get("stalled", False),
                    "dead": msg.get("dead", False),
                }

                player.fuel_used = max(
                    player.fuel_used,
                    float(msg.get("fuel_used", player.fuel_used)),
                )
                player.max_horizontal_speed = max(
                    player.max_horizontal_speed,
                    float(msg.get(
                        "max_horizontal_speed",
                        player.max_horizontal_speed,
                    )),
                )
                player.damage_taken = max(
                    player.damage_taken,
                    float(msg.get(
                        "damage_taken",
                        player.damage_taken,
                    )),
                )

                await room_broadcast(
                    room,
                    payload,
                    exclude_id=player.player_id,
                )

            # ---------------------------------------------------------
            # Per-player fuel pickup
            # ---------------------------------------------------------
            elif msg_type == "fuel_collected":
                pickup_id = str(msg.get("pickup_id", ""))

                if pickup_id in player.collected_fuel:
                    continue

                player.collected_fuel.add(pickup_id)

                await send_json(ws, {
                    "type": "fuel_confirmed",
                    "pickup_id": pickup_id,
                })

            # ---------------------------------------------------------
            # Finish
            # ---------------------------------------------------------
            elif msg_type == "finish":
                if player.finished or player.eliminated or room.race_over:
                    continue

                player.finished = True
                room.finish_count += 1
                player.finish_order = room.finish_count

                if room.race_start_server_time is not None:
                    # Client-provided run time is accepted for prototype
                    # precision, but server still owns finish ordering.
                    reported = msg.get("finish_time")
                    if reported is None:
                        player.finish_time = max(
                            0.0,
                            time.time() - room.race_start_server_time,
                        )
                    else:
                        player.finish_time = max(
                            0.0,
                            float(reported),
                        )

                player.fuel_used = max(
                    0.0,
                    float(msg.get("fuel_used", 0)),
                )
                player.max_horizontal_speed = max(
                    0.0,
                    float(msg.get("max_horizontal_speed", 0)),
                )
                player.damage_taken = max(
                    0.0,
                    float(msg.get("damage_taken", 0)),
                )

                await room_broadcast(room, {
                    "type": "player_finished",
                    "player_id": player.player_id,
                    "nickname": player.nickname,
                    "finish_order": player.finish_order,
                    "finish_time": player.finish_time,
                })

                await maybe_finish_race(room)

            # ---------------------------------------------------------
            # Elimination: destroyed or irrecoverably out of fuel
            # ---------------------------------------------------------
            elif msg_type == "eliminated":
                if (
                    player.finished
                    or player.eliminated
                    or room.race_over
                ):
                    continue

                player.eliminated = True
                player.elimination_reason = str(
                    msg.get("reason", "eliminated")
                )[:40]

                player.fuel_used = max(
                    player.fuel_used,
                    float(msg.get("fuel_used", 0)),
                )
                player.max_horizontal_speed = max(
                    player.max_horizontal_speed,
                    float(msg.get("max_horizontal_speed", 0)),
                )
                player.damage_taken = max(
                    player.damage_taken,
                    float(msg.get("damage_taken", 0)),
                )

                await room_broadcast(room, {
                    "type": "player_eliminated",
                    "player_id": player.player_id,
                    "nickname": player.nickname,
                    "reason": player.elimination_reason,
                })

                await maybe_finish_race(room)

            # ---------------------------------------------------------
            # Results -> same map rematch (host only)
            # ---------------------------------------------------------
            elif msg_type == "restart_race":
                if room.host_id != player.player_id:
                    await send_json(ws, {
                        "type": "error",
                        "message": "Only host can restart the race",
                    })
                    continue

                if len(room.players) != MAX_PLAYERS:
                    continue

                room.race_started = False
                room.race_over = False
                room.race_start_server_time = None
                room.finish_count = 0

                for p in room.players.values():
                    reset_player_race_state(p, ready=True)

                await room_broadcast(room, {
                    "type": "race_reset",
                    "mode": "restart",
                    "level": room.level,
                })
                await room_broadcast(room, room_state(room))
                await maybe_start_countdown(room)

            # ---------------------------------------------------------
            # Results -> lobby/map selection (host only)
            # ---------------------------------------------------------
            elif msg_type == "choose_map":
                if room.host_id != player.player_id:
                    await send_json(ws, {
                        "type": "error",
                        "message": "Only host can choose another map",
                    })
                    continue

                room.race_started = False
                room.race_over = False
                room.race_start_server_time = None
                room.finish_count = 0

                for p in room.players.values():
                    reset_player_race_state(p, ready=False)

                await room_broadcast(room, {
                    "type": "race_reset",
                    "mode": "lobby",
                    "level": room.level,
                })
                await room_broadcast(room, room_state(room))

            # ---------------------------------------------------------
            # Leave
            # ---------------------------------------------------------
            elif msg_type == "leave_room":
                await leave_room(room, player.player_id)
                room = None

            elif msg_type == "ping":
                await send_json(ws, {
                    "type": "pong",
                    "server_time": time.time(),
                })

            else:
                await send_json(ws, {
                    "type": "error",
                    "message": f"Unknown packet: {msg_type}",
                })

    except WebSocketDisconnect:
        pass

    except Exception as exc:
        try:
            await send_json(ws, {
                "type": "error",
                "message": str(exc),
            })
        except Exception:
            pass

    finally:
        if room is not None and player is not None:
            await leave_room(room, player.player_id)


if __name__ == "__main__":
    import os
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
    )
