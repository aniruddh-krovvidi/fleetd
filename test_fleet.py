"""End-to-end tests: real gRPC server, real agents, in-process.

    .venv/bin/python -m pytest -q      (or: .venv/bin/python test_fleet.py)
"""
import logging, time, unittest

import grpc
import fleet_pb2 as pb
import fleet_pb2_grpc as rpc
from agent import BoardAgent
from controller import Controller, serve

PORT = 50990


def wait_for(pred, timeout=10):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if pred():
            return True
        time.sleep(0.05)
    return False


class FleetTest(unittest.TestCase):
    def setUp(self):
        self.agents = {}
        self.ctrl = Controller(heartbeat_timeout=0.6, oob_power_cycle=lambda b: self.agents[b].power_cycle())
        self.server = serve(self.ctrl, PORT)
        self.stub = rpc.FleetStub(grpc.insecure_channel(f"localhost:{PORT}"))

    def tearDown(self):
        for a in self.agents.values():
            a.stop()
        self.server.stop(0)
        self.ctrl.stop()

    def board(self, bid, **kw):
        a = self.agents[bid] = BoardAgent(bid, f"localhost:{PORT}", speed=400, **kw)
        a.start()
        return a

    def submit(self, **kw):
        spec = {"kind": "eval", "artifact": "sha256:x", "work_units": 40, **kw}
        return self.stub.SubmitJob(pb.JobSpec(**spec)).id

    def state(self, jid):
        return self.stub.GetJob(pb.JobHandle(id=jid)).state

    def test_jobs_run_and_finish(self):
        self.board("b1"); self.board("b2")
        ids = [self.submit() for _ in range(6)]
        self.assertTrue(wait_for(lambda: all(self.state(i) == "DONE" for i in ids)))
        boards = {self.stub.GetJob(pb.JobHandle(id=i)).board_id for i in ids}
        self.assertEqual(boards, {"b1", "b2"}, "work should spread across boards")

    def test_hw_gen_constraint(self):
        self.board("ai5-board", hw_gen="ai5"); self.board("ai6-board", hw_gen="ai6")
        jid = self.submit(hw_gen="ai6")
        self.assertTrue(wait_for(lambda: self.state(jid) == "DONE"))
        self.assertEqual(self.stub.GetJob(pb.JobHandle(id=jid)).board_id, "ai6-board")

    def test_onboarding_flashes_old_firmware(self):
        self.board("old", firmware="1.9.0")
        self.assertTrue(wait_for(lambda: any(
            b.id == "old" and b.firmware == "2.1.0" and b.state == "HEALTHY"
            for b in self.stub.ListBoards(pb.Empty()).boards)))
        jid = self.submit()
        self.assertTrue(wait_for(lambda: self.state(jid) == "DONE"))

    def test_dead_board_job_rescued_and_board_healed(self):
        a = self.board("flaky", slots=1)
        self.board("steady", slots=1)
        # a slow job lands on some board; then hang that board
        jid = self.submit(work_units=800)
        self.assertTrue(wait_for(lambda: self.state(jid) == "RUNNING"))
        victim = self.stub.GetJob(pb.JobHandle(id=jid)).board_id
        self.agents[victim].hung = True
        self.agents[victim].running.clear()
        # controller must notice, requeue the job elsewhere, and power-cycle the board
        self.assertTrue(wait_for(lambda: self.state(jid) == "DONE", timeout=15))
        st = self.stub.GetJob(pb.JobHandle(id=jid))
        self.assertEqual(st.attempts, 2)
        self.assertNotEqual(st.board_id, victim)
        stats = self.stub.FleetStats(pb.Empty())
        self.assertGreaterEqual(stats.requeues, 1)
        self.assertTrue(wait_for(lambda: self.stub.FleetStats(pb.Empty()).heals >= 1))
        self.assertTrue(wait_for(lambda: all(b.state == "HEALTHY" for b in self.stub.ListBoards(pb.Empty()).boards)))

    def test_drain_stops_new_work(self):
        self.board("d1"); self.board("d2")
        self.assertTrue(wait_for(lambda: len(self.stub.ListBoards(pb.Empty()).boards) == 2))
        self.assertTrue(self.stub.DrainBoard(pb.BoardId(id="d1")).ok)
        self.assertTrue(wait_for(lambda: any(b.id == "d1" and b.state == "DRAINING" for b in self.stub.ListBoards(pb.Empty()).boards)))
        ids = [self.submit() for _ in range(4)]
        self.assertTrue(wait_for(lambda: all(self.state(i) == "DONE" for i in ids)))
        self.assertTrue(all(self.stub.GetJob(pb.JobHandle(id=i)).board_id == "d2" for i in ids))


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    unittest.main()
