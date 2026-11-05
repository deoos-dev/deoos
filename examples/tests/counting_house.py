"""Counting House refund saga on the real engine: approval, crash, rejection, compensation, evals."""
import argparse
import os
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root / "python" / "counting-house"))
from demo import main

main(argparse.Namespace(
    endpoint=os.environ.get("DEOOS_ENDPOINT", "http://127.0.0.1:19000"),
    directory=root / "outputs" / "counting-house",
))
