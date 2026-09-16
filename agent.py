"""fleetd board agent: runs on (or, here, simulates) one accelerator board.

Registers with the controller, heartbeats, executes assigned jobs, applies
FLASH / REBOOT / DRAIN commands. The simulation injects the failure modes a
real fleet sees: boards that hang (stop heartbeating), thermal throttling,
and out-of-date firmware that must be flashed before the board takes work.

    python3 agent.py --id board-01 --hw-gen ai5 --firmware 2.0.0
"""
import argparse, logging, random, threading, time

import grpc
import fleet_pb2 as pb
import fleet_pb2_grpc as rpc

log = logging.getLogger("agent")


class BoardAgent(threading.Thread):
    def __init__(self, board_id, addr="localhost:50051", hw_gen="ai5", firmware="2.1.0", slots=2,
                 speed=100.0, fault_rate=0.0, heartbeat_s=0.2, seed=None):
        super().__init__(daemon=True, name=board_id)
        self.id, self.addr, self.hw_gen, self.firmware, self.slots = board_id, addr, hw_gen, firmware, slots
        self.speed = speed                  # work_units per second
        self.fault_rate = fault_rate        # P(hang) per heartbeat
        self.heartbeat_s = heartbeat_s
        self.rng = random.Random(seed)
        self.running = {}                   # job_id -> thread
        self.lock = threading.Lock()
        self.state = "READY"
        self.hung = False                   # simulated hang: no heartbeats until power-cycled
        self.power_cycled = threading.Event()
        self.draining = False
        self._stop = threading.Event()
        self.stub = rpc.FleetStub(grpc.insecure_channel(addr))

    # --- out-of-band control, as a BMC/PDU would provide ---
    def power_cycle(self):
        self.power_cycled.set()

    def stop(self):
        self._stop.set()

    # --- main loop ---
    def run(self):
        self._register()
        while not self._stop.is_set():
            if self.hung:
                if self.power_cycled.wait(timeout=0.1):   # waits for the controller's OOB reboot
                    self._reboot()
                continue
            if self.fault_rate and self.rng.random() < self.fault_rate:
                log.warning("%s: hang (simulated)", self.id)
                self.hung = True
                with self.lock:
                    self.running.clear()            # jobs on a hung board are lost
                continue
            try:
                reply = self.stub.Heartbeat(pb.HeartbeatRequest(
                    board_id=self.id, state=self.state, temp_c=self._temp(),
                    firmware=self.firmware, running_jobs=list(self.running)))
                for cmd in reply.commands:
                    self._handle(cmd)
            except grpc.RpcError as e:
                log.warning("%s: heartbeat failed: %s", self.id, e.code())
            time.sleep(self.heartbeat_s)

    def _register(self):
        for _ in range(50):
            try:
                self.stub.Register(pb.BoardInfo(id=self.id, hw_gen=self.hw_gen, firmware=self.firmware, slots=self.slots))
                return
            except grpc.RpcError:
                time.sleep(0.1)

    def _reboot(self):
        self.power_cycled.clear()
        self.hung = False
        with self.lock:
            self.running.clear()
        time.sleep(0.3)                                  # POST + agent start
        self.state = "READY"
        self._register()

    def _temp(self):
        # base + load; occasional spike so THROTTLED shows up in the sim
        load = len(self.running) / max(self.slots, 1)
        spike = 35 if self.rng.random() < 0.01 else 0
        return 55 + 20 * load + self.rng.uniform(-3, 3) + spike

    def _handle(self, cmd):
        if cmd.type == "ASSIGN":
            if self.draining:
                self._report(cmd.job.id, False, "board draining", 0)
                return
            t = threading.Thread(target=self._run_job, args=(cmd.job,), daemon=True)
            with self.lock:
                self.running[cmd.job.id] = t
            t.start()
        elif cmd.type == "FLASH":
            log.info("%s: flashing %s -> %s", self.id, self.firmware, cmd.firmware)
            self.state = "FLASHING"
            time.sleep(0.5)
            self.firmware = cmd.firmware
            self.state = "READY"
            self._register()
        elif cmd.type == "REBOOT":
            self._reboot()
        elif cmd.type == "DRAIN":
            self.draining = True
            self.state = "DRAINING"

    def _run_job(self, job):
        t0 = time.monotonic()
        time.sleep(job.work_units / self.speed)
        if self.hung:
            return                                       # result never reported; controller rescues it
        with self.lock:
            self.running.pop(job.id, None)
        self._report(job.id, True, "", time.monotonic() - t0)

    def _report(self, job_id, ok, err, dur):
        try:
            self.stub.ReportJob(pb.JobResult(job_id=job_id, board_id=self.id, success=ok, error=err, duration_s=dur))
        except grpc.RpcError as e:
            log.warning("%s: report failed: %s", self.id, e.code())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--addr", default="localhost:50051")
    ap.add_argument("--hw-gen", default="ai5")
    ap.add_argument("--firmware", default="2.1.0")
    ap.add_argument("--slots", type=int, default=2)
    ap.add_argument("--fault-rate", type=float, default=0.0)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ag = BoardAgent(a.id, a.addr, a.hw_gen, a.firmware, a.slots, fault_rate=a.fault_rate)
    ag.start()
    ag.join()
