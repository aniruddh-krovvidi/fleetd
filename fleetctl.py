"""fleetctl: operator CLI for the fleetd controller.

    python3 fleetctl.py status
    python3 fleetctl.py boards
    python3 fleetctl.py jobs [--state RUNNING]
    python3 fleetctl.py submit --kind eval --artifact sha256:abc --hw-gen ai5 --priority 2 --work 100
    python3 fleetctl.py drain board-007
"""
import argparse, time

import grpc
import fleet_pb2 as pb
import fleet_pb2_grpc as rpc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", default="localhost:50051")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    sub.add_parser("boards")
    j = sub.add_parser("jobs"); j.add_argument("--state")
    s = sub.add_parser("submit")
    s.add_argument("--kind", default="eval"); s.add_argument("--artifact", required=True)
    s.add_argument("--hw-gen", default=""); s.add_argument("--priority", type=int, default=0)
    s.add_argument("--work", type=int, default=100); s.add_argument("--wait", action="store_true")
    d = sub.add_parser("drain"); d.add_argument("board_id")
    a = ap.parse_args()
    stub = rpc.FleetStub(grpc.insecure_channel(a.addr))

    if a.cmd == "status":
        st = stub.FleetStats(pb.Empty())
        print(f"boards   {st.boards_healthy}/{st.boards_total} healthy   uptime {st.uptime_pct:.1f}%   utilization {st.utilization_pct:.1f}%")
        print(f"jobs     queued {st.jobs_queued}  running {st.jobs_running}  done {st.jobs_done}  failed {st.jobs_failed}")
        print(f"healing  heals {st.heals}  requeues {st.requeues}  MTTR {st.mttr_s:.2f}s   queue p50 {st.p50_queue_s:.2f}s p99 {st.p99_queue_s:.2f}s")
    elif a.cmd == "boards":
        print(f"{'id':<12}{'gen':<5}{'fw':<8}{'state':<14}{'temp':<7}{'jobs':<6}last_seen")
        for b in stub.ListBoards(pb.Empty()).boards:
            print(f"{b.id:<12}{b.hw_gen:<5}{b.firmware:<8}{b.state:<14}{b.temp_c:<7.0f}{len(b.running_jobs)}/{b.slots:<4}{b.last_seen_ago_s:.1f}s")
    elif a.cmd == "jobs":
        print(f"{'id':<10}{'kind':<9}{'state':<9}{'board':<12}{'tries':<6}{'queued':<8}dur")
        for jb in stub.ListJobs(pb.Empty()).jobs:
            if a.state and jb.state != a.state:
                continue
            print(f"{jb.id:<10}{jb.kind:<9}{jb.state:<9}{jb.board_id:<12}{jb.attempts:<6}{jb.queued_s:<8.2f}{jb.duration_s:.2f}")
    elif a.cmd == "submit":
        h = stub.SubmitJob(pb.JobSpec(kind=a.kind, artifact=a.artifact, hw_gen=a.hw_gen, priority=a.priority, work_units=a.work))
        print(h.id)
        while a.wait:
            jb = stub.GetJob(h)
            if jb.state in ("DONE", "FAILED"):
                print(f"{jb.state} on {jb.board_id} after {jb.attempts} attempt(s), {jb.duration_s:.2f}s {jb.error}")
                break
            time.sleep(0.2)
    elif a.cmd == "drain":
        r = stub.DrainBoard(pb.BoardId(id=a.board_id))
        print("ok" if r.ok else r.msg)


if __name__ == "__main__":
    main()
