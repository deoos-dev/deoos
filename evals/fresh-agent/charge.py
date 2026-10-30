"""Flaky charge. A crash or a timeout loses the attempt."""
import random
import sys


def charge(order_id):
    if random.random() < 0.7:
        raise RuntimeError("upstream timeout")
    return {"order_id": order_id, "status": "charged"}


def main():
    order_id = sys.argv[1] if len(sys.argv) > 1 else "order-1001"
    print(charge(order_id))


if __name__ == "__main__":
    main()
