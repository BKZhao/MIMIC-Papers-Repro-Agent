from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path


def main() -> int:
    if importlib.util.find_spec("streamlit") is None:
        print(
            "Streamlit is not installed. Please install dashboard extras first:\n"
            "  pip install -e '.[dashboard]'",
            file=sys.stderr,
        )
        return 2

    app_path = Path(__file__).resolve().with_name("dashboard_app.py")
    cmd = [sys.executable, "-m", "streamlit", "run", str(app_path), *sys.argv[1:]]
    completed = subprocess.run(cmd, check=False)
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
