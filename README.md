# fleetd

A control plane for a **fleet of AI accelerator boards**: inventory and
firmware onboarding, job scheduling, health monitoring, and self-healing —
the layer that turns thousands of boards into one reliable inference cluster.
Python + gRPC, no other dependencies.

```
 fleetctl / clients ──gRPC──►  controller  ◄──gRPC (heartbeat/register/report)── board agents
                                │  ├─ inventory + onboarding (flash to target firmware)
                                │  ├─ scheduler (priority queue, hw-gen constraints, slots)
                                │  ├─ health monitor (missed heartbeats → UNHEALTHY)
                                │  ├─ self-healing (rescue jobs, out-of-band power-cycle)
                                │  └─ thermal throttling (no new work above 90°C)
                                └─ /metrics (Prometheus)
```

Jobs reference **compiler-produced artifacts** by hash and target a hardware
generation (`ai5`, `ai6`); the scheduler places them on healthy boards with free
slots, highest priority first. A board that stops heartbeating is marked
`UNHEALTHY` within one timeout: its running jobs are requeued elsewhere and the
controller power-cycles it out-of-band (a BMC/PDU call in production; the sim
injects the hook). Boards that register with old firmware are flashed before
they take work. Draining a board finishes its jobs and stops new assignments.

## Chaos benchmark

64 simulated boards (2 hardware generations, 15% arriving on old firmware),
600 mixed eval / sim / rollout jobs, each board hanging with probability 0.2%
per heartbeat (≈1 hang/board/100s). 40 seconds, same seed both runs:

| metric                 | self-heal OFF | self-heal ON |
|------------------------|--------------:|-------------:|
| fleet uptime           |        77.0 % |   **99.5 %** |
| boards healthy at end  |         37/64 |    **64/64** |
| boards auto-recovered  |             0 |           33 |
| mean time to recover   |             — |      **0.3 s** |
| jobs rescued off dead boards |       7 |            7 |
| jobs done / failed     |       600 / 0 |      600 / 0 |
| queue latency p50 / p99|  0.03 / 0.14 s| 0.02 / 0.11 s|

Without self-healing the fleet bleeds capacity (27 of 64 boards dead after
40 s); with it, hangs are a 0.3-second blip and uptime stays above the 99%
line. Job rescue works in both modes, which is why nothing fails either way —
what self-healing buys is *capacity*, not correctness.

```bash
python3 chaos.py --boards 64 --jobs 600 --fault-rate 0.002 --duration 40
```

## Run it

```bash
python3 -m venv .venv && .venv/bin/pip install grpcio grpcio-tools
.venv/bin/python controller.py                       # :50051 gRPC, :9100 metrics
.venv/bin/python agent.py --id board-01 --hw-gen ai5 --firmware 2.0.0   # gets flashed to 2.1.0
.venv/bin/python fleetctl.py submit --artifact sha256:abc --hw-gen ai5 --wait
.venv/bin/python fleetctl.py status
.venv/bin/python fleetctl.py boards
.venv/bin/python test_fleet.py                       # 5 end-to-end tests over real gRPC
```

Regenerate stubs after editing `fleet.proto`:
`.venv/bin/python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. fleet.proto`

## Files

- `fleet.proto` — the API: board-side (Register / Heartbeat / ReportJob) and client-side (SubmitJob / GetJob / ListBoards / DrainBoard / FleetStats)
- `controller.py` — control plane: inventory, scheduler, health monitor, self-healing, metrics
- `agent.py` — board agent; simulates hangs, thermal spikes, flashing, reboot
- `fleetctl.py` — operator CLI
- `chaos.py` — the benchmark above
- `test_fleet.py` — end-to-end tests

## Deliberate simplifications

- State is in-memory. Persisting boards/jobs to SQLite so the controller survives restarts is the obvious next step.
- Single-board jobs only. Gang scheduling (a job spanning N boards) is a queue-reservation change in `_assign`.
- Time-weighted uptime/utilization are computed in the controller; a real fleet would scrape `/metrics` into Prometheus and compute them there.
