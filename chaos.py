"""chaos: fleet benchmark under injected failures.

Starts a controller + N simulated boards in-process, submits a mixed workload
(evals / sims / rollouts across two hardware generations, some boards on old
firmware), injects board hangs at a given rate, and reports fleet uptime,
utilization, job success, MTTR and queue latency — with self-healing ON vs OFF.

    python3 chaos.py --boards 64 --jobs 600 --fault-rate 0.002 --duration 40
"""
import argparse, json, logging, random, threading, time

import grpc
import fleet_pb2 as pb
import fleet_pb2_grpc as rpc
from agent import BoardAgent
from controller import Controller, serve


def run(boards, jobs, fault_rate, duration, self_heal, port, seed=1):
    rng = random.Random(seed)
    agents = {}
    ctrl = Controller(heartbeat_timeout=1.0, self_heal=self_heal,
                      oob_power_cycle=lambda bid: agents[bid].power_cycle())
    server = serve(ctrl, port)
    for i in range(boards):
        bid = f"board-{i:03d}"
        agents[bid] = BoardAgent(
            bid, f"localhost:{port}", hw_gen="ai5" if i % 3 else "ai6",
            firmware="2.1.0" if rng.random() > 0.15 else "2.0.0",   # 15% need onboarding flash
            slots=2, speed=rng.uniform(80, 120), fault_rate=fault_rate, seed=seed * 1000 + i)
        agents[bid].start()
    time.sleep(1.0)  # let boards register / flash

    stub = rpc.FleetStub(grpc.insecure_channel(f"localhost:{port}"))
    kinds = ["eval", "sim", "rollout"]
    t0 = time.monotonic()
    for i in range(jobs):
        stub.SubmitJob(pb.JobSpec(
            kind=rng.choice(kinds), artifact=f"sha256:{rng.getrandbits(64):016x}",
            hw_gen=rng.choice(["", "ai5", "ai6"]), priority=rng.randint(0, 3),
            work_units=rng.randint(30, 150)))
        time.sleep(duration * 0.5 / jobs)   # spread submissions over the first half
    while time.monotonic() - t0 < duration:
        time.sleep(0.5)

    s = stub.FleetStats(pb.Empty())
    server.stop(0); ctrl.stop()
    for a in agents.values():
        a.stop()
    return {
        "self_heal": self_heal, "boards": boards, "jobs": jobs, "fault_rate": fault_rate,
        "uptime_pct": round(s.uptime_pct, 1), "utilization_pct": round(s.utilization_pct, 1),
        "done": s.jobs_done, "failed": s.jobs_failed, "still_queued": s.jobs_queued + s.jobs_running,
        "heals": s.heals, "requeues": s.requeues, "mttr_s": round(s.mttr_s, 2),
        "p50_queue_s": round(s.p50_queue_s, 2), "p99_queue_s": round(s.p99_queue_s, 2),
        "boards_healthy_at_end": s.boards_healthy,
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--boards", type=int, default=64)
    ap.add_argument("--jobs", type=int, default=600)
    ap.add_argument("--fault-rate", type=float, default=0.002, help="P(board hangs) per heartbeat (5/s)")
    ap.add_argument("--duration", type=float, default=40)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.ERROR)
    results = [run(a.boards, a.jobs, a.fault_rate, a.duration, sh, 50100 + i) for i, sh in enumerate((False, True))]
    if a.json:
        print(json.dumps(results, indent=2))
    else:
        keys = ["uptime_pct", "utilization_pct", "done", "failed", "still_queued", "heals", "requeues", "mttr_s", "p50_queue_s", "p99_queue_s", "boards_healthy_at_end"]
        print(f"{'metric':<22}{'self-heal OFF':>15}{'self-heal ON':>15}")
        for k in keys:
            print(f"{k:<22}{results[0][k]:>15}{results[1][k]:>15}")
