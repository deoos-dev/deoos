"""Print engine health as one JSON object. Exit 0 when protocol_version is 3.

Usage: python agents/scripts/check_engine.py
Requires ENGINE_URL for server mode, or DEOOS_STORAGE_BUCKET for library mode.
"""
import json
import sys

from deoos import client_from_environment


def main():
    client = client_from_environment()
    try:
        info = client.request("/info")
    finally:
        client.close()
    protocol = info.get("protocol_version") if isinstance(info, dict) else None
    print(json.dumps({"ok": protocol == 3, "protocol_version": protocol}, sort_keys=True))
    return 0 if protocol == 3 else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
