"""Both SDKs/modes: stable step keys, saved payments and call/commit-gap recovery."""
import argparse
import os
from pathlib import Path
import sys

root = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(root / "examples"), str(root / "clients/python")]
os.environ["PYTHONPATH"] = str(root / "clients/python") + os.pathsep + os.environ.get("PYTHONPATH", "")
from payment_resume_demo import main

main(argparse.Namespace(server=Path(os.environ.get("ENGINE_BINARY", root / "engine/target/release/deoos-server")),
                        node_sdk=root / "clients/typescript/dist/index.js", key_window_seconds=86400))
