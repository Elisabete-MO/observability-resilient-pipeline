"""Benchmark HTTP do Producer usando somente a biblioteca padrao."""

import argparse
import json
import math
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("Informe um inteiro positivo.")
    return number


def positive_float(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("Informe um numero positivo finito.")
    return number


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return round(ordered[index] * 1000, 3)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenario",
        choices=["load", "stress", "spike"],
        default="load",
    )
    parser.add_argument("--url", default="http://localhost:8000/events")
    parser.add_argument("--rate", type=positive_int, default=5)
    parser.add_argument("--duration", type=positive_int, default=60)
    parser.add_argument("--workers", type=positive_int, default=10)
    parser.add_argument("--timeout", type=positive_float, default=15)
    args = parser.parse_args()

    # Cada fase dura --duration segundos.
    if args.scenario == "load":
        phases = [("steady", args.rate)]
    elif args.scenario == "stress":
        phases = [
            ("step_1", args.rate),
            ("step_2", args.rate * 2),
            ("step_3", args.rate * 4),
        ]
    else:
        phases = [
            ("baseline", args.rate),
            ("peak", args.rate * 5),
            ("recovery", args.rate),
        ]

    run_id = str(uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    results = Counter()
    all_latencies = []
    confirmed_latencies = []
    lock = threading.Lock()

    # Limita requisicoes em andamento e evita fila ilimitada no gerador.
    slots = threading.BoundedSemaphore(args.workers)

    def send_event(phase, index):
        started = time.perf_counter()
        outcome = "unexpected_response"

        try:
            event = {
                "event_id": str(uuid4()),
                "event_type": "benchmark.event",
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "payload": {
                    "run_id": run_id,
                    "scenario": args.scenario,
                    "phase": phase,
                    "index": index,
                },
            }

            request = Request(
                args.url,
                data=json.dumps(event).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )

            with urlopen(request, timeout=args.timeout) as response:
                body = json.load(response)
                if (
                    response.status == 202
                    and isinstance(body, dict)
                    and body.get("status") == "published"
                ):
                    outcome = "confirmed"

        except HTTPError as error:
            outcome = f"http_{error.code}"
            error.close()
        except (URLError, TimeoutError, OSError):
            # A publicacao pode ter ocorrido mesmo sem resposta ao cliente.
            outcome = "connection_error"
        except (ValueError, UnicodeError):
            outcome = "invalid_response"
        except Exception:
            outcome = "client_error"
        finally:
            elapsed = time.perf_counter() - started
            with lock:
                results[outcome] += 1
                all_latencies.append(elapsed)
                if outcome == "confirmed":
                    confirmed_latencies.append(elapsed)
            slots.release()

    phase_reports = []
    started = time.monotonic()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        for phase_name, rate in phases:
            phase_started = time.monotonic()
            scheduled_count = rate * args.duration
            submitted = 0
            skipped_busy = 0
            skipped_late = 0

            print(
                f"Fase {phase_name}: {rate} req/s por {args.duration}s",
                file=sys.stderr,
                flush=True,
            )

            for index in range(scheduled_count):
                due = phase_started + index / rate
                time.sleep(max(0, due - time.monotonic()))

                # Nao cria uma rajada para compensar atraso do gerador.
                if time.monotonic() - due > 1 / rate:
                    skipped_late += 1
                    continue

                if not slots.acquire(blocking=False):
                    skipped_busy += 1
                    continue

                executor.submit(send_event, phase_name, index)
                submitted += 1

            phase_end = phase_started + args.duration
            time.sleep(max(0, phase_end - time.monotonic()))

            phase_reports.append({
                "phase": phase_name,
                "target_rate_per_second": rate,
                "scheduled": scheduled_count,
                "submitted": submitted,
                "skipped_busy": skipped_busy,
                "skipped_late": skipped_late,
            })

        sending_seconds = time.monotonic() - started
        # Ao sair do bloco, aguarda as requisicoes em andamento terminarem.

    total_seconds = time.monotonic() - started
    attempted = sum(results.values())
    confirmed = results["confirmed"]

    report = {
        "run_id": run_id,
        "started_at_utc": started_at,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "scenario": args.scenario,
        "url": args.url,
        "workers": args.workers,
        "timeout_seconds": args.timeout,
        "phase_duration_seconds": args.duration,
        "sending_seconds": round(sending_seconds, 3),
        "total_seconds_including_drain": round(total_seconds, 3),
        "phases": phase_reports,
        "attempted": attempted,
        "confirmed": confirmed,
        "unconfirmed": attempted - confirmed,
        "results": dict(results),
        "confirmed_per_second_including_drain": round(
            confirmed / total_seconds, 3
        ),
        "unconfirmed_percent": round(
            100 * (attempted - confirmed) / attempted, 3
        ) if attempted else None,
        "http_latency_ms_all_attempts": {
            "p50": percentile(all_latencies, 0.50),
            "p95": percentile(all_latencies, 0.95),
            "p99": percentile(all_latencies, 0.99),
        },
        "http_latency_ms_confirmed": {
            "p50": percentile(confirmed_latencies, 0.50),
            "p95": percentile(confirmed_latencies, 0.95),
            "p99": percentile(confirmed_latencies, 0.99),
        },
    }

    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(
        f"Concluido: {confirmed}/{attempted} tentativas confirmadas.",
        file=sys.stderr,
        flush=True,
    )

    return 0 if attempted > 0 and confirmed == attempted else 1


if __name__ == "__main__":
    raise SystemExit(main())
