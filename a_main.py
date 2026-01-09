import sys
import os
import time
import json
import socket
import threading
from pathlib import Path
import traci

# 配置参数（按需修改）
B_IP = "192.168.10.4"  # B端IP
B_UDP_PORT = 5005      # B端UDP接收端口
B_TCP_PORT = 6006      # B端TCP服务端口
SEND_INTERVAL_SEC = 1  # UDP上报周期（秒）
SUMO_CFG_PATH = "D:/ZuoYe/sumo/111.sumocfg"  # 本地SUMO配置文件路径

# 环境变量配置
sumo_home = os.environ.get("SUMO_HOME")
if not sumo_home:
    raise EnvironmentError("请配置SUMO_HOME环境变量")
tools = Path(sumo_home) / "tools"
if not tools.exists():
    raise EnvironmentError(f"SUMO tools路径不存在：{tools}")
sys.path.append(str(tools))

def udp_sender(stop_event: threading.Event):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    target = (B_IP, B_UDP_PORT)
    i = 0
    print(f"[A][UDP] sending -> {target} every {SEND_INTERVAL_SEC}s")
    try:
        while not stop_event.is_set():
            try:
                # 采集SUMO数据并封装为JSON
                sim_time = traci.simulation.getTime()
                vehicles = []
                for veh_id in traci.vehicle.getIDList():
                    try:
                        x, y = traci.vehicle.getPosition(veh_id)
                        vehicles.append({
                            "id": veh_id,
                            "x": x,
                            "y": y,
                            "speed": traci.vehicle.getSpeed(veh_id),
                            "lane": traci.vehicle.getLaneID(veh_id),
                            "road": traci.vehicle.getRoadID(veh_id)
                        })
                    except traci.exceptions.TraciExceotion:
                        continue
                tls_list = []
                for tls_id in traci.trafficlight.getIDList():
                    phase = traci.trafficlight.getPhase(tls_id)
                    state = traci.trafficlight.getRedYellowGreenState(tls_id)
                    next_switch = traci.trafficlight.getNextSwitch(tls_id)
                    phase_remaining = next_switch - sim_time
                    tls_list.append({
                        "tls_id": tls_id,
                        "phase": phase,
                        "state": state,
                        "phase_remaining": phase_remaining,
                        "lane_lights": []  # 简化处理，按需可扩展
                    })
            # 封装JSON消息
                msg = json.dumps({
                    "type": "sumo_snapshot",
                    "sim_time": sim_time,
                    "sent_ts_ms": int(time.time() * 1000),
                    "frame_id": i,
                    "source": "A1",
                    "step_length": traci.simulation.getDeltaT(),
                    "vehicles": vehicles,
                    "tls": tls_list
                })
                s.sendto(msg.encode("utf-8"), target)
                i += 1
                time.sleep(SEND_INTERVAL_SEC)
            except Exception as e:
                time.sleep(0.2)
    finally:
        s.close()

def tcp_client_recv(stop_event):
    while not stop_event.is_set():
        try:
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.connect((B_IP, B_TCP_PORT))
            conn.settimeout(1.0)
            conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            print(f"[A][TCP] connected to B ({B_IP}:{B_TCP_PORT}, waiting commands...)")
            buf = b""
            while not stop_event.is_set():
                data = conn.recv(4996)
                if not data:
                    print("[A][TCP] disconnected by B, will reconnect...")
                    break
                buf += data
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.strip()
                    if line:
                        cmd = json.loads(line.decode("utf-8", errors="ignore"))
                        print(f"[A][TCP] <- 执行指令: {cmd}")
                        # 执行信号灯控制指令
                        if cmd.get("type") == "set_phase" and "tls_id" in cmd and "phase_index" in cmd:
                            traci.trafficlight.setPhase(cmd["tls_id"], cmd["phase_index"])
                            # 回复ACK
                            ack = json.dumps({"type": "ack", "ok": True, "msg": f"相位切换至{cmd['phase_index']}"}) + "\n"
                            conn.send(ack.encode("utf-8"))
        except socket.timeout:
            continue
        except Exception as e:
            print(f"[A][TCP] 错误: {e}")
            time.sleep(2)


def main():
    # 启动SUMO
    sumo_cmd = ["sumo-gui", "-c", SUMO_CFG_PATH]
    traci.start(sumo_cmd)
    print("[A] SUMO仿真启动成功")

    # 启动线程
    stop_event = threading.Event()
    threading.Thread(target=udp_sender, args=(stop_event,), daemon=True).start()
    threading.Thread(target=tcp_client_recv, args=(stop_event,), daemon=True).start()

    # 保持运行
    try:
        while traci.simulation.getMinExpectedNumber() > 0:
            traci.simulationStep()
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("[A] 正在退出...")
    finally:
        stop_event.set()
        traci.close()


if __name__ == "__main__":
    main()