"""Run the four specified experiments and an extra failure experiment, using real HTTP."""

import json
import os
import platform
import subprocess
import sys
from pathlib import Path


def main():
    target = Path(sys.argv[1] if len(sys.argv) > 1 else "benchmarks")
    target.mkdir(parents=True, exist_ok=True)
    experiments = [
        (
            "A_cache_disabled",
            ["--cache-capacity", "0"],
            ["--requests", "10000", "--concurrency", "100"],
        ),
        (
            "B_cache_enabled",
            ["--cache-capacity", "512"],
            ["--requests", "10000", "--concurrency", "100"],
        ),
        ("C_reservations", [], ["--mode", "reserve", "--requests", "100", "--concurrency", "100"]),
        (
            "D_websocket_fanout",
            [],
            ["--mode", "fanout", "--requests", "100", "--concurrency", "100"],
        ),
        (
            "E_injected_failures",
            [],
            [
                "--mode",
                "reserve",
                "--requests",
                "100",
                "--concurrency",
                "100",
                "--failure-rate",
                "0.05",
            ],
        ),
    ]
    for name, config, flags in experiments:
        cmd = [sys.executable, "-m", "eventvault.cli", *config, "benchmark", *flags]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env={**os.environ, "LOG_LEVEL": "ERROR"},
            check=True,
        )
        data = json.loads(result.stdout)
        data["command"] = "eventvault " + " ".join([*config, "benchmark", *flags])
        data["python"] = platform.python_version()
        (target / f"{name}.json").write_text(json.dumps(data, indent=2) + "\n")
        print(name, "completed", flush=True)


if __name__ == "__main__":
    main()
