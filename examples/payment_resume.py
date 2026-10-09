"""Charge a fake payment API, await approval, and fulfil using stable request keys.

Run with HANDLERS and payment_inputs(service_url). The API must enforce request
idempotency; saved charge checkpoints avoid calling it again after a resume.
"""
import json
import urllib.parse
import urllib.request

HANDLER = "payment.resume.v1"


def payment_inputs(service_url, amount_cents=1000):
    if not isinstance(service_url, str):
        raise ValueError("service_url must be an HTTP(S) base URL")
    url = urllib.parse.urlsplit(service_url)
    if (url.scheme not in ("http", "https") or not url.hostname or url.username
            or url.password or url.query or url.fragment):
        raise ValueError("service_url must be an HTTP(S) base URL without credentials, query, or fragment")
    if (isinstance(amount_cents, bool) or not isinstance(amount_cents, int)
            or not 1 <= amount_cents <= 9_007_199_254_740_991):
        raise ValueError("amount_cents must be a positive safe integer")
    return {"service_url": service_url.rstrip("/"), "amount_cents": amount_cents}


def post(service_url, path, key, body):
    body = {**body, "idempotency_key": key}
    request = urllib.request.Request(service_url + path, data=json.dumps(body).encode("utf-8"),
                                     headers={"Content-Type": "application/json", "Idempotency-Key": key})
    with urllib.request.urlopen(request, timeout=10) as response:
        result = json.load(response)
    if not isinstance(result, dict):
        raise ValueError("payment API response must be an object")
    return result


def payment(ctx, inputs):
    inputs = payment_inputs(**inputs)
    charge = ctx.step("charge", lambda: post(inputs["service_url"], "/charge",
                      ctx.idempotency_key("charge"), {"amount_cents": inputs["amount_cents"]}))
    approval = ctx.wait_signal("approval")
    if not isinstance(approval, dict) or not isinstance(approval.get("approved"), bool):
        raise ValueError("approval signal must contain a boolean 'approved' field")
    if not approval["approved"]:
        return {"status": "declined", "charge": charge}
    return ctx.step("fulfil", lambda: post(inputs["service_url"], "/fulfil",
                    ctx.idempotency_key("fulfil"), {"charge": charge, "approval": approval}))


HANDLERS = {HANDLER: payment}
