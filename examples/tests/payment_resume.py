"""Both SDKs and modes: stable step keys, saved payments, and the call/commit gap."""
import argparse
import os
from pathlib import Path

import paths
from payment_resume_demo import main

main(argparse.Namespace(
    server=Path(os.environ["ENGINE_BINARY"]),
    node_sdk=Path(os.environ["DEOOS_NODE_SDK"]) if os.environ.get("DEOOS_NODE_SDK") else None,
    key_window_seconds=86400,
    directory=paths.ROOT / "outputs" / "payment-resume",
    endpoint=os.environ.get("DEOOS_ENDPOINT", "http://127.0.0.1:19000"),
))
