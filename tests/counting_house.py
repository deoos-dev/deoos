"""Counting House refund saga on the real engine: approval, crash, rejection, compensation, evals."""
import argparse
import os
from pathlib import Path
import sys

root = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(root / "examples" / "counting_house"), str(root / "clients/python")]
os.environ["PYTHONPATH"] = str(root / "clients/python") + os.pathsep + os.environ.get("PYTHONPATH", "")
from demo import main

main(argparse.Namespace(
    endpoint=os.environ.get("COUNTING_HOUSE_ENDPOINT", "http://127.0.0.1:19000"),
    directory=root / "outputs" / "counting-house",
))
