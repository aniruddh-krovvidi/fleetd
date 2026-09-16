"""fleetd controller: the control plane for a fleet of AI accelerator boards.

Owns board inventory + onboarding (firmware flashing), job scheduling, health
monitoring, and self-healing. Boards talk to it over gRPC; clients use the same
service to submit jobs and read state. Prometheus metrics on an HTTP port.

    python3 controller.py --port 50051 --metrics-port 9100
"""
import argparse, heapq, itertools, logging, statistics, threading, time
from concurrent import futures
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import grpc
import fleet_pb2 as pb
import fleet_pb2_grpc as rpc

log = logging.getLogger("fleetd")

HEALTHY, THROTTLED, UNHEALTHY, PROVISIONING, DRAINING = "HEALTHY", "THROTTLED", "UNHEALTHY", "PROVISIONING", "DRAINING"
QUEUED, RUNNING, DONE, FAILED = "QUEUED", "RUNNING", "DONE", "FAILED"


class Board:
    def __init__(self, info):
        self.id, self.hw_gen, self.firmware, self.slots = info.id, info.hw_gen, info.firmware, info.slots
        self.state = PROVISIONING
        self.temp_c = 0.0
        self.running = set()
        self.last_seen = time.monotonic()
        self.pending = []          # commands delivered on next heartbeat
        self.down_since = None     # for MTTR


class Job:
    def __init__(self, spec):
        self.spec = spec
        self.state = QUEUED
        self.board_id = ""
        self.attempts = 0
        self.submitted = time.monotonic()
        self.started = None
        self.finished = None
        self.error = ""


class Controller(rpc.FleetServicer):
    def __init__(self, target_firmware="2.1.0", heartbeat_timeout=1.0, throttle_temp=90.0,
                 max_attempts=3, self_heal=True, oob_power_cycle=None):
        self.target_fw = target_firmware
        self.hb_timeout = heartbeat_timeout
        self.throttle_temp = throttle_temp
        self.max_attempts = max_attempts
        self.self_heal = self_heal
        # In a real fleet this is an IPMI/Redfish/PDU call; the sim injects a callable.
        self.oob_power_cycle = oob_power_cycle

        self.lock = threading.Lock()
        self.boards: dict[str, Board] = {}
        self.jobs: dict[str, Job] = {}
        self.queue = []            # heap of (-priority, seq, job_id)
        self.seq = itertools.count()

        # time-weighted accumulators for uptime / utilization
        self.t0 = time.monotonic()
        self.last_tick = self.t0
        self.healthy_secs = 0.0    # sum(healthy_boards * dt)
        self.board_secs = 0.0      # sum(total_boards * dt)
        self.busy_slot_secs = 0.0
        self.slot_secs = 0.0
        self.mttr_samples = []
        self.heals = 0
        self.requeues = 0

        self._stop = threading.Event()
        threading.Thread(target=self._monitor, daemon=True).start()

    # ---------- board-side RPCs ----------

    def Register(self, info, ctx):
        with self.lock:
            b = self.boards.get(info.id)
            if b is None:
                b = self.boards[info.id] = Board(info)
                log.info("onboarding board %s (%s fw=%s)", info.id, info.hw_gen, info.firmware)
            b.firmware, b.hw_gen, b.slots = info.firmware, info.hw_gen, info.slots
            b.last_seen = time.monotonic()
            b.running.clear()
            if b.state == UNHEALTHY and b.down_since is not None:
                self.mttr_samples.append(time.monotonic() - b.down_since)
                self.heals += 1
                b.down_since = None
            if info.firmware != self.target_fw:
                b.state = PROVISIONING
                b.pending.append(pb.Command(type="FLASH", firmware=self.target_fw))
                log.info("board %s fw %s != target %s: flashing", info.id, info.firmware, self.target_fw)
            elif b.state != DRAINING:
                b.state = HEALTHY
        return pb.RegisterReply(accepted=True, target_firmware=self.target_fw)

    def Heartbeat(self, req, ctx):
        with self.lock:
            b = self.boards.get(req.board_id)
            if b is None:
                return pb.HeartbeatReply(commands=[pb.Command(type="REBOOT")])  # unknown board: re-register
            b.last_seen = time.monotonic()
            b.temp_c, b.firmware = req.temp_c, req.firmware
            b.running = set(req.running_jobs)
            if b.state == UNHEALTHY:           # came back on its own
                if b.down_since is not None:
                    self.mttr_samples.append(time.monotonic() - b.down_since)
                    self.heals += 1
                    b.down_since = None
                b.state = HEALTHY
            if b.state in (HEALTHY, THROTTLED):
                b.state = THROTTLED if req.temp_c >= self.throttle_temp else HEALTHY
            cmds, b.pending = b.pending, []
            if b.state == HEALTHY and req.state == "READY":
                cmds += self._assign(b)
            return pb.HeartbeatReply(commands=cmds)

    def ReportJob(self, res, ctx):
        with self.lock:
            j = self.jobs.get(res.job_id)
            b = self.boards.get(res.board_id)
            if b:
                b.running.discard(res.job_id)
            if j is None or j.state != RUNNING:
                return pb.Ack(ok=False, msg="unknown or not running")
            j.finished = time.monotonic()
            if res.success:
                j.state = DONE
            else:
                j.error = res.error
                self._requeue_or_fail(j)
        return pb.Ack(ok=True)

    # ---------- client-side RPCs ----------

    def SubmitJob(self, spec, ctx):
        with self.lock:
            if not spec.id:
                spec.id = f"job-{next(self.seq)}"
            j = self.jobs[spec.id] = Job(spec)
            self._enqueue(j)
        return pb.JobHandle(id=spec.id)

    def GetJob(self, h, ctx):
        with self.lock:
            j = self.jobs.get(h.id)
            if j is None:
                ctx.abort(grpc.StatusCode.NOT_FOUND, h.id)
            return self._job_status(j)

    def ListJobs(self, _, ctx):
        with self.lock:
            return pb.JobList(jobs=[self._job_status(j) for j in self.jobs.values()])

    def ListBoards(self, _, ctx):
        now = time.monotonic()
        with self.lock:
            return pb.BoardList(boards=[pb.Board(
                id=b.id, hw_gen=b.hw_gen, firmware=b.firmware, state=b.state, temp_c=b.temp_c,
                running_jobs=sorted(b.running), last_seen_ago_s=now - b.last_seen, slots=b.slots,
            ) for b in self.boards.values()])

    def DrainBoard(self, bid, ctx):
        with self.lock:
            b = self.boards.get(bid.id)
            if b is None:
                return pb.Ack(ok=False, msg="no such board")
            b.state = DRAINING
            b.pending.append(pb.Command(type="DRAIN"))
        return pb.Ack(ok=True)

    def FleetStats(self, _, ctx):
        with self.lock:
            self._integrate()
            states = [j.state for j in self.jobs.values()]
            queued = [(j.started - j.submitted) for j in self.jobs.values() if j.started]
            queued.sort()
            q = lambda p: queued[int(p * (len(queued) - 1))] if queued else 0.0
            return pb.Stats(
                boards_total=len(self.boards),
                boards_healthy=sum(b.state in (HEALTHY, THROTTLED) for b in self.boards.values()),
                uptime_pct=100 * self.healthy_secs / self.board_secs if self.board_secs else 0,
                utilization_pct=100 * self.busy_slot_secs / self.slot_secs if self.slot_secs else 0,
                jobs_queued=states.count(QUEUED), jobs_running=states.count(RUNNING),
                jobs_done=states.count(DONE), jobs_failed=states.count(FAILED),
                mttr_s=statistics.mean(self.mttr_samples) if self.mttr_samples else 0,
                heals=self.heals, requeues=self.requeues, p50_queue_s=q(0.5), p99_queue_s=q(0.99),
            )

    # ---------- internals (call with lock held) ----------

    def _enqueue(self, j):
        j.state = QUEUED
        j.board_id = ""
        heapq.heappush(self.queue, (-j.spec.priority, next(self.seq), j.spec.id))

    def _assign(self, b):
        """Pop queued jobs this board can run, up to its free slots."""
        cmds, skipped = [], []
        while self.queue and len(b.running) + len(cmds) < b.slots:
            item = heapq.heappop(self.queue)
            j = self.jobs[item[2]]
            if j.state != QUEUED:
                continue
            if j.spec.hw_gen and j.spec.hw_gen != b.hw_gen:
                skipped.append(item)
                continue
            j.state, j.board_id, j.attempts = RUNNING, b.id, j.attempts + 1
            j.started = j.started or time.monotonic()
            b.running.add(j.spec.id)
            cmds.append(pb.Command(type="ASSIGN", job=j.spec))
        for item in skipped:
            heapq.heappush(self.queue, item)
        return cmds

    def _requeue_or_fail(self, j):
        if j.attempts < self.max_attempts:
            self.requeues += 1
            self._enqueue(j)
        else:
            j.state = FAILED

    def _integrate(self):
        now = time.monotonic()
        dt = now - self.last_tick
        self.last_tick = now
        n = len(self.boards)
        healthy = sum(b.state in (HEALTHY, THROTTLED) for b in self.boards.values())
        self.healthy_secs += healthy * dt
        self.board_secs += n * dt
        self.slot_secs += sum(b.slots for b in self.boards.values()) * dt
        self.busy_slot_secs += sum(len(b.running) for b in self.boards.values()) * dt

    def _monitor(self):
        """Health monitor: detect dead boards, rescue their jobs, trigger self-healing."""
        while not self._stop.is_set():
            time.sleep(0.1)
            with self.lock:
                self._integrate()
                now = time.monotonic()
                for b in self.boards.values():
                    if b.state == UNHEALTHY or now - b.last_seen < self.hb_timeout:
                        continue
                    log.warning("board %s missed heartbeats (%.1fs): UNHEALTHY", b.id, now - b.last_seen)
                    b.state, b.down_since = UNHEALTHY, now
                    for jid in list(b.running):       # rescue jobs
                        j = self.jobs.get(jid)
                        if j and j.state == RUNNING:
                            j.error = f"board {b.id} died"
                            self._requeue_or_fail(j)
                    b.running.clear()
                    if self.self_heal and self.oob_power_cycle:
                        b.pending = [pb.Command(type="REBOOT")]
                        self.oob_power_cycle(b.id)

    def _job_status(self, j):
        return pb.JobStatus(
            id=j.spec.id, kind=j.spec.kind, state=j.state, board_id=j.board_id, attempts=j.attempts,
            queued_s=(j.started - j.submitted) if j.started else 0,
            duration_s=(j.finished - j.started) if j.started and j.finished else 0, error=j.error,
        )

    def stop(self):
        self._stop.set()


# ---------- Prometheus metrics ----------

def metrics_server(ctrl, port):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a): pass
        def do_GET(self):
            s = ctrl.FleetStats(pb.Empty(), None)
            body = "".join(f"fleetd_{k} {v}\n" for k, v in (
                ("boards_total", s.boards_total), ("boards_healthy", s.boards_healthy),
                ("uptime_pct", s.uptime_pct), ("utilization_pct", s.utilization_pct),
                ("jobs_queued", s.jobs_queued), ("jobs_running", s.jobs_running),
                ("jobs_done_total", s.jobs_done), ("jobs_failed_total", s.jobs_failed),
                ("heals_total", s.heals), ("requeues_total", s.requeues), ("mttr_seconds", s.mttr_s),
            )).encode()
            self.send_response(200); self.send_header("Content-Type", "text/plain"); self.end_headers()
            self.wfile.write(body)
    srv = ThreadingHTTPServer(("", port), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def serve(ctrl, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    rpc.add_FleetServicer_to_server(ctrl, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    return server


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=50051)
    ap.add_argument("--metrics-port", type=int, default=9100)
    ap.add_argument("--target-firmware", default="2.1.0")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ctrl = Controller(target_firmware=a.target_firmware)
    metrics_server(ctrl, a.metrics_port)
    log.info("fleetd controller on :%d, metrics on :%d", a.port, a.metrics_port)
    serve(ctrl, a.port).wait_for_termination()
