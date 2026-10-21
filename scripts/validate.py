"""Run the documented offline checks in an already prepared isolated environment.

Usage: python scripts/validate.py --evidence /absolute/local/evidence-directory
This script never installs packages or downloads market data.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update({"PIP_NO_INDEX": "1", "PIP_DISABLE_PIP_VERSION_CHECK": "1",
                "PYTHONDONTWRITEBYTECODE": "1"})
    steps = []
    for name, command in [
        ("dependencies", [sys.executable, "-m", "pip", "check"]),
        ("tests", [sys.executable, "-B", "-m", "pytest", "-q", "--durations=20",
                   "--junitxml=" + str(evidence / "tests.xml")]),
        ("build", [sys.executable, "-m", "build", "--no-isolation", "--outdir",
                   str(evidence / "dist")]),
    ]:
        started = time.monotonic()
        with (evidence / (name + ".log")).open("w") as log:
            result = subprocess.run(command, cwd=root, env=env, stdout=log,
                                    stderr=subprocess.STDOUT, check=False)
        steps.append({"step": name, "returncode": result.returncode,
                      "seconds": time.monotonic() - started})
        (evidence / "steps.json").write_text(json.dumps(steps, indent=2) + "\n")
        print(name, "exit", result.returncode, flush=True)
        if result.returncode:
            return result.returncode
    suites = ET.parse(evidence / "tests.xml").getroot()
    fields = ("tests", "failures", "errors", "skipped")
    counts = {k: sum(int(s.get(k, 0)) for s in suites.iter("testsuite")) for k in fields}
    (evidence / "counts.json").write_text(json.dumps(counts, indent=2) + "\n")
    print(json.dumps(counts), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
