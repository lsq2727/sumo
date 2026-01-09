import sys
import time
import json
import socket
import threading
import queue
import uuid
import asyncio
from dataclasses import dataclass, field
from typing import Dict, Set, List, Tuple, Any
import pymysql
from pymysql.err import OperationalError
from fastapi import FastAPI, WebSocket, Body
from fastapi.responses import HTMLResponse

# 配置参数（按需修改）
UDP_LISTEN_IP = "0.0.0.0"
UDP_LISTEN_PORT = 5005
TCP_LISTEN_IP = "0.0.0.0"
TCP_LISTEN_PORT = 6006
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Zzrcbrx2,Hrdcmxs7.",
    "database": "sumo_data",
    "charset": "utf8mb4"
}
MAX_QUEUE_SIZE = 1000
MAX_POINTS = 200  # 实时滚动图最大点数

# 初始化组件
app = FastAPI()
udp_q = queue.Queue(maxsize=MAX_QUEUE_SIZE)
tcp_q = queue.Queue(maxsize=MAX_QUEUE_SIZE)
last_frame_id = {"A1": -1}  # 去重用
latest_sim_time = {"A1": 0.0}
history_points: Dict[str, List[Dict[str, Any]]] = {}

class DBWriter:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.cur = None
        self.connect()

    def connect(self):
        try:
            self.conn = pymysql.connect(**self.config)
            self.cur = self.conn.cursor()
        except OperationalError as e:
            raise Exception(f"数据库连接失败: {e}")

    def insert_frame(self, frame_rec):
        sql = """
        INSERT INTO frames
          (source, frame_id, sim_time, step_length, sent_ts_ms, raw_json, is_valid, drop_reason, vehicle_count, tls_count)
        VALUES
          (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          recv_ts = CURRENT_TIMESTAMP(3),
          raw_json = VALUES(raw_json),
          is_valid = VALUES(is_valid),
          drop_reason = VALUES(drop_reason),
          vehicle_count = VALUES(vehicle_count),
          tls_count = VALUES(tls_count),
          sim_time = VALUES(sim_time),
          step_length = VALUES(step_length),
          sent_ts_ms = VALUES(sent_ts_ms)
        """
        self.cur.execute(sql, (
            frame_rec["source"], frame_rec["frame_id"], frame_rec["sim_time"],
            frame_rec["step_length"], frame_rec["sent_ts_ms"], frame_rec["raw_json"],
            frame_rec["is_valid"], frame_rec["drop_reason"], frame_rec["vehicle_count"],
            frame_rec["tls_count"]
        ))
        self.conn.commit()

    def upsert_vehicles_latest(self, source, sim_time, frame_id, v):
        sql = """
        INSERT INTO vehicles
          (source, veh_id, last_frame_id, last_sim_time, x, y, speed, lane, road,
           birth_sim_time, last_seen_sim_time, death_sim_time, is_active)
        VALUES
          (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          last_frame_id = VALUES(last_frame_id),
          last_sim_time = VALUES(last_sim_time),
          x = VALUES(x),
          y = VALUES(y),
          speed = VALUES(speed),
          lane = VALUES(lane),
          road = VALUES(road),
          birth_sim_time = LEAST(birth_sim_time, VALUES(birth_sim_time)),
          last_seen_sim_time = VALUES(last_seen_sim_time),
          death_sim_time = NULL,
          is_active = 1
        """
        self.cur.executemany(sql, [(
            source, v["id"], frame_id, sim_time, v["x"], v["y"], v["speed"],
            v["lane"], v["road"], sim_time, sim_time, None, 1
        )])
        self.conn.commit()

    def upsert_tls_latest(self, source, sim_time, frame_id, t):
        sql = """
        INSERT INTO tls
          (source, tls_id, last_frame_id, last_sim_time, phase, state)
        VALUES
          (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          last_frame_id = VALUES(last_frame_id),
          last_sim_time = VALUES(last_sim_time),
          phase = VALUES(phase),
          state = VALUES(state)
        """
        self.cur.executemany(sql, [(
            source, t["tls_id"], frame_id, sim_time, t["phase"], t["state"]
        )])
        self.conn.commit()

    def insert_tls_history(self, source, frame_id, sim_time, t):
        sql = """
        INSERT INTO tls_history
          (source, frame_id, sim_time, tls_id, phase, state, phase_remaining)
        VALUES
          (%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          sim_time=VALUES(sim_time),
          phase=VALUES(phase),
          state=VALUES(state),
          phase_remaining=VALUES(phase_remaining)
        """
        self.cur.executemany(sql, [(
            source, frame_id, sim_time, t["tls_id"], t["phase"],
            t["state"], t["phase_remaining"]
        )])
        self.conn.commit()

    def insert_tls_lane_state(self, source, frame_id, sim_time, tls_id, tls_state, phase_remaining):
        """
        根据 tls.state (如 'GrGr') 自动拆分为 lane 级灯态
        每帧 / 每 tls / 每 lane 插一条记录
        """
        if not tls_state:
            return

        rows = []
        for idx, color in enumerate(tls_state):
            rows.append((
                source,
                frame_id,
                sim_time,
                tls_id,
                f"lane_{idx}",
                color,
                phase_remaining
            ))

        sql = """
        INSERT INTO tls_lane_state
          (source, frame_id, sim_time, tls_id, from_lane, lane_color, phase_remaining)
        VALUES
          (%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          lane_color = VALUES(lane_color),
          phase_remaining = VALUES(phase_remaining),
          sim_time = VALUES(sim_time)
        """
        self.cur.executemany(sql, rows)
        self.conn.commit()


db_writer = DBWriter(DB_CONFIG)

class UDPServer:
    def __init__(self):
        self.stop_event = threading.Event()
        self.udp_q = udp_q

    def udp_receiver_thread(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((UDP_LISTEN_IP, UDP_LISTEN_PORT))
        s.settimeout(1.0)
        cnt = 0
        last_ts = time.time()
        while not self.stop_event.is_set():
            try:
                data, addr = s.recvfrom(65535)
                a_ip = addr[0]
                msg = data.decode("utf-8", errors="ignore")
                now = time.time()
                cnt += 1
                if now - last_ts >= 1.0:
                    self.udp_q.put(("rate", cnt))
                    cnt = 0
                    last_ts = now
                self.udp_q.put(("msg", a_ip, msg, now))
            except socket.timeout:
                continue
            except Exception as e:
                self.udp_q.put(("err", str(e), time.time()))
                time.sleep(0.2)
        s.close()

    def start(self):
        threading.Thread(target=self.udp_receiver_thread, daemon=True).start()

class TCPServer:
    def __init__(self):
        self.stop_event = threading.Event()
        self.tcp_q = tcp_q
        self.tcp_conn = None
        self.conn_lock = threading.Lock()

    def tcp_server_thread(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((TCP_LISTEN_IP, TCP_LISTEN_PORT))
        srv.listen(1)
        srv.settimeout(1.0)
        while not self.stop_event.is_set():
            try:
                conn, addr = srv.accept()
                conn.settimeout(1.0)
                conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                with self.conn_lock:
                    if self.tcp_conn:
                        try:
                            self.tcp_conn.close()
                        except Exception:
                            pass
                    self.tcp_conn = conn
                self.tcp_q.put(("connected", addr, time.time()))
                threading.Thread(target=self.tcp_reader_thread, args=(conn,), daemon=True).start()
            except socket.timeout:
                continue
            except Exception as e:
                self.tcp_q.put(("err", str(e), time.time()))
                time.sleep(0.2)
        srv.close()

    def tcp_reader_thread(self, conn: socket.socket):
        buf = b""
        while not self.stop_event.is_set():
            try:
                data = conn.recv(4996)
                if not data:
                    self.tcp_q.put(("disconnected", time.time()))
                    break
                buf += data
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.strip()
                    if line:
                        self.tcp_q.put(("rx", line.decode("utf-8", errors="ignore"), time.time()))
                        # 转发ACK到WebSocket
                        ws_mgr.publish_threadsafe("control/log", {
                            "level": "info",
                            "text": f"A端回复: {line.decode('utf-8', errors='ignore')}",
                            "ts_ms": int(time.time() * 1000)
                        })
            except socket.timeout:
                continue
            except Exception:
                self.tcp_q.put(("disconnected", time.time()))
                break
        with self.conn_lock:
            if self.tcp_conn is conn:
                self.tcp_conn = None

    def send_command(self, cmd: dict) -> bool:
        """发送控制指令到A端"""
        if not self.tcp_conn:
            return False
        msg_str = json.dumps(cmd) + "\n"
        try:
            self.tcp_conn.sendall(msg_str.encode("utf-8"))
            return True
        except Exception:
            return False

    def start(self):
        threading.Thread(target=self.tcp_server_thread, daemon=True).start()

@dataclass
class Client:
    ws: WebSocket
    topics: Set[str] = field(default_factory=set)

class WSManager:
    def __init__(self):
        self.clients: Dict[WebSocket, Client] = {}
        self._lock = asyncio.Lock()
        self._loop = None

    def init_loop(self, loop):
        self._loop = loop

    async def handle_ws(self, ws: WebSocket):
        await ws.accept()
        client = Client(ws=ws, topics=set())
        self.clients[ws] = client
        try:
            while True:
                text = await ws.receive_text()
                cmd = json.loads(text)
                op = cmd.get("op") or cmd.get("type")
                topics = cmd.get("topics", [])
                if isinstance(topics, str):
                    topics = [topics]
                if op == "subscribe":
                    client.topics.update(topics)
                elif op == "unsubscribe":
                    for t in topics:
                        client.topics.discard(t)
        finally:
            self.clients.pop(ws, None)

    async def publish(self, topic: str, payload: Dict[str, Any]):
        msg = json.dumps({"type": "event", "topic": topic, "payload": payload}, ensure_ascii=False)
        async with self._lock:
            targets = [c for c in self.clients.values() if topic in c.topics]
        for c in targets:
            await c.ws.send_text(msg)

    def publish_threadsafe(self, topic: str, payload: Dict[str, Any]):
        if self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(self.publish(topic, payload), self._loop)

ws_mgr = WSManager()

def data_processor():
    """处理UDP接收的数据，解析后入库并发布到WebSocket"""
    while True:
        try:
            item = udp_q.get(timeout=1.0)
            if item[0] == "msg":
                a_ip, raw_msg, now = item[1], item[2], item[3]
                # 解析JSON
                try:
                    msg = json.loads(raw_msg)
                except json.JSONDecodeError:
                    print(f"[B] JSON解析失败: {raw_msg}")
                    continue
                # 协议校验
                frame_rec = {
                    "source": msg.get("source", "A1"),
                    "frame_id": msg.get("frame_id", 0),
                    "sim_time": msg.get("sim_time", 0.0),
                    "step_length": msg.get("step_length"),
                    "sent_ts_ms": msg.get("sent_ts_ms"),
                    "raw_json": raw_msg,
                    "is_valid": 1,
                    "drop_reason": None,
                    "vehicle_count": len(msg.get("vehicles", [])),
                    "tls_count": len(msg.get("tls", []))
                }
                if msg.get("type") != "sumo_snapshot":
                    frame_rec["is_valid"] = 0
                    frame_rec["drop_reason"] = "type.not_sumo_frame"
                    db_writer.insert_frame(frame_rec)
                    continue
                # 去重/乱序处理
                source = frame_rec["source"]
                frame_id = frame_rec["frame_id"]
                last_id = last_frame_id.get(source, -1)
                if frame_id <= last_id:
                    frame_rec["is_valid"] = 0
                    frame_rec["drop_reason"] = f"duplicate_or_out_of_order(last={last_id})"
                    db_writer.insert_frame(frame_rec)
                    continue
                last_frame_id[source] = frame_id
                latest_sim_time[source] = frame_rec["sim_time"]
                # 入库
                db_writer.insert_frame(frame_rec)
                vehicles = msg.get("vehicles", [])
                for v in vehicles:
                    db_writer.upsert_vehicles_latest(source, frame_rec["sim_time"], frame_id, v)
                tls_list = msg.get("tls", [])
                for t in tls_list:
                    db_writer.upsert_tls_latest(source, frame_rec["sim_time"], frame_id, t)
                    db_writer.insert_tls_history(source, frame_id, frame_rec["sim_time"], t)
                    db_writer.insert_tls_lane_state(
                        source=source,
                        frame_id=frame_id,
                        sim_time=frame_rec["sim_time"],
                        tls_id=t["tls_id"],
                        tls_state=t["state"],
                        phase_remaining=t.get("phase_remaining")
                    )
                # 发布到WebSocket
                if vehicles:
                    avg_speed = sum(v["speed"] for v in vehicles) / len(vehicles)
                else:
                    avg_speed = 0.0
                history = history_points.setdefault(source, [])
                history.append({
                    "sim_time": frame_rec["sim_time"],
                    "vehicle_count": len(vehicles),
                    "avg_speed": round(avg_speed, 2)
                })
                if len(history) > MAX_POINTS:
                    history[:] = history[-MAX_POINTS:]
                # 发布实时主题
                ws_mgr.publish_threadsafe("realtime/summary", {
                    "source": source,
                    "frame_id": frame_id,
                    "sim_time": frame_rec["sim_time"],
                    "vehicle_count": len(vehicles),
                    "avg_speed": round(avg_speed, 2),
                    "ts_ms": int(time.time() * 1000)
                })
                ws_mgr.publish_threadsafe("realtime/vehicles", {
                    "source": source,
                    "frame_id": frame_id,
                    "sim_time": frame_rec["sim_time"],
                    "vehicles": vehicles
                })
                ws_mgr.publish_threadsafe("realtime/tls_lanes", {
                    "source": source,
                    "frame_id": frame_id,
                    "sim_time": frame_rec["sim_time"],
                    "tls": [{"tls_id": t["tls_id"],
                             "phase_remaining": t.get("phase_remaining"),
                             "state": t["state"],
                             }for t in tls_list
                            ]
                })
                ws_mgr.publish_threadsafe("realtime/ingest_status", {
                    "ok": True,
                    "source": source,
                    "frame_id": frame_id,
                    "ts_ms": int(time.time() * 1000)
                })
                ws_mgr.publish_threadsafe("history/trend", {
                    "source": source,
                    "points": history
                })
        except queue.Empty:
            continue
        except Exception as e:
            print(f"[B] 数据处理错误: {e}")

# 前端HTML（直接复用给定文件）
@app.get("/", response_class=HTMLResponse)
async def get_index():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

# WebSocket接口
@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await ws_mgr.handle_ws(websocket)

# 控制指令接口
@app.post("/api/control/tls")
async def control_tls(
    tls_id: str = Body(..., embed=True),
    phase: int = Body(..., embed=True)
):
    cmd = {"type": "set_phase", "tls_id": tls_id, "phase_index": phase}
    ok = tcp_server.send_command(cmd)
    if ok:
        ws_mgr.publish_threadsafe("control/log", {
            "level": "info",
            "text": f"已发送指令：切换{tls_id}相位至{phase}",
            "ts_ms": int(time.time() * 1000)
        })
        return {"status": "ok", "msg": f"Sent phase {phase} to {tls_id}"}
    else:
        return {"status": "error", "msg": "A-side not connected"}

# 历史回放接口
@app.get("/api/history/playback")
def api_history_playback(source="A1", t0=None, t1=None, stride=2, max_frames=600):
    stride = max(int(stride), 1)
    max_frames = max(int(max_frames), 1)
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        sql = """
            SELECT frame_id, sim_time, raw_json
            FROM frames
            WHERE source=%s
            ORDER BY frame_id DESC
            LIMIT %s
        """
        cur.execute(sql, (source, max_frames * stride))
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    rows = list(reversed(rows))
    frames = []
    for idx, row in enumerate(rows):
        if idx % stride != 0:
            continue
        frame_id, sim_time, raw_json = row
        try:
            raw = json.loads(raw_json)
        except json.JSONDecodeError:
            continue
        vehicles = [
            {"id": v.get("id"), "x": v.get("x", 0), "y": v.get("y", 0)}
            for v in raw.get("vehicles", [])
        ]
        frames.append({
            "frame_id": frame_id,
            "sim_time": sim_time,
            "vehicles": vehicles
        })
    return {"source": source, "t0": t0, "t1": t1, "count": len(frames), "frames": frames}

@app.get("/api/net")
def api_net():
    return {"polylines": [], "bounds":[0,0,1000,1000]}

@app.on_event("startup")
async def on_startup():
    ws_mgr.init_loop(asyncio.get_running_loop())

def main():
    # 启动基础服务
    udp_server = UDPServer()
    udp_server.start()
    global tcp_server
    tcp_server = TCPServer()
    tcp_server.start()
    # 启动数据处理线程
    threading.Thread(target=data_processor, daemon=True).start()
    # 启动FastAPI
    import uvicorn
    """loop = asyncio.get_event_loop()
    ws_mgr.init_loop(loop)"""
    print("[B] 所有服务启动成功，访问 http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()
