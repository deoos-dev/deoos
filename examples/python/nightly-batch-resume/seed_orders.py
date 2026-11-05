"""Write the seeded order file the nightly batch reads.

The sequence is fixed. A rerun of the same path produces the same bytes.
"""
import argparse
import json
import random
from pathlib import Path

ORDER_COUNT = 10_000


def write_orders(path, count=ORDER_COUNT):
    rng = random.Random(20261028)
    lines = []
    for number in range(1, count + 1):
        order = {
            "cents": 100 + rng.randrange(9_000),
            "email": f"buyer-{number:05d}@orders.example",
            "id": f"o{number:05d}",
            "sku": f"sku-{rng.randrange(1_000):04d}",
        }
        lines.append(json.dumps(order, sort_keys=True, separators=(",", ":")))
    Path(path).write_text("\n".join(lines) + "\n")
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("--count", type=int, default=ORDER_COUNT)
    args = parser.parse_args()
    write_orders(args.path, args.count)
    print(f"Wrote {args.count} orders to {args.path}")


if __name__ == "__main__":
    main()
