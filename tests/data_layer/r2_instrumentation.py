"""Keep the two-second application response gate separate from coverage process teardown."""

import json
import selectors
import subprocess
import time
from pathlib import Path

from coverage import Coverage


def run_fifo_cli(command):
    if Coverage.current() is None:
        return subprocess.run(command, capture_output=True, text=True, timeout=2, check=False)
    started = time.monotonic()
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    assert process.stdout is not None
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        if not selector.select(timeout=2):
            process.kill()
            process.communicate()
            raise subprocess.TimeoutExpired(command, 2)
        first = process.stdout.readline()
    response_elapsed = time.monotonic() - started
    assert response_elapsed < 2
    assert json.loads(first)["status"] == "REJECTED"
    rest, errors = process.communicate(timeout=30)
    total_elapsed = time.monotonic() - started
    evidence = Path(__file__).resolve().parents[2] / ".tmp-data-layer/coverage-timing"
    evidence.mkdir(parents=True, exist_ok=True)
    with (evidence / "FIFO_COVERAGE_TIMING.jsonl").open("a") as stream:
        stream.write(
            json.dumps(
                {
                    "response_deadline_seconds": 2,
                    "response_seconds": response_elapsed,
                    "process_seconds": total_elapsed,
                    "returncode": process.returncode,
                    "coverage_active": True,
                    "teardown_deadline_seconds": 30,
                }
            )
            + "\n"
        )
    return subprocess.CompletedProcess(command, process.returncode, first + rest, errors)
