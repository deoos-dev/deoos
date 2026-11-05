"""Counting House refund: the model proposes, the workflow performs the saga.

A write intent names the refund. This workflow re-checks the live ledger, waits
for a person to approve, then posts the change to the billing ledger, the CRM,
and the notification log. Every write uses an idempotency key. Every write is
read back. If a later write fails, earlier writes are compensated in reverse.

The legacy systems are JSON files in an office directory. Nothing in this
module opens a network connection. Swap a proposer in PROPOSERS, or set
proposer to "command" and COUNTING_HOUSE_PROPOSER to a local program.

Run a worker with HANDLERS. Submit the dict returned by case_inputs.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path

from deoos import Client

HANDLER = "counting-house.refund.v1"
ALLOWED_REASONS = ("duplicate", "defective", "goodwill")
CUSTOMER_ID = "cust-100"
INVOICE_ID = "inv-42"
INVOICE_CENTS = 4200
SYSTEMS = ("ledger", "crm", "notify")


class OfficeFault(Exception):
    """A legacy system rejected a write or a read-back did not match."""

    def __init__(self, step, code, message):
        self.step = step
        self.code = code
        super().__init__(f"{step}: {code}: {message}")


def opening_state():
    """One paid invoice for Avery Chen. Each case gets its own copy."""
    return {
        "ledger": {
            "customers": {CUSTOMER_ID: {"name": "Avery Chen"}},
            "invoices": {INVOICE_ID: {
                "customer_id": CUSTOMER_ID,
                "amount_cents": INVOICE_CENTS,
                "status": "paid",
            }},
            "entries": [],
        },
        "crm": {CUSTOMER_ID: {"name": "Avery Chen", "notes": []}},
        "notify": {"messages": []},
        "effects": {},
        "requests": [],
    }


def refunded_cents(state, invoice_id):
    return sum(
        entry["amount_cents"] for entry in state["ledger"]["entries"]
        if entry["invoice_id"] == invoice_id and entry["kind"] == "refund" and not entry["reversed"]
    )


def _atomic_json(path, value):
    path = Path(path)
    payload = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    temporary = path.with_name(path.name + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(temporary, path)
    os.chmod(path, 0o600)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _find(entries, entry_id):
    return next((entry for entry in entries if entry["id"] == entry_id), None)


class Office:
    """Billing ledger, CRM, and notification log stored as one JSON file."""

    def __init__(self, directory):
        self.root = Path(directory)
        self._state_path = self.root / "office.json"

    def seed(self):
        self.root.mkdir(parents=True, exist_ok=True)
        if not self._state_path.exists():
            _atomic_json(self._state_path, opening_state())
        return self

    def read(self):
        with self._lock():
            return self._load()

    def snapshot(self, customer_id, invoice_id):
        state = self.read()
        return {
            "refunded_cents": refunded_cents(state, invoice_id),
            "entries": state["ledger"]["entries"],
            "notes": state["crm"][customer_id]["notes"],
            "messages": [message for message in state["notify"]["messages"]
                         if message["customer_id"] == customer_id],
        }

    def write_audit(self, document):
        _atomic_json(self.root / "audit.json", document)
        return document

    def eligibility(self, request, proposal):
        """Re-read the ledger. The proposal is not evidence that the invoice is payable."""
        state = self.read()
        invoice = state["ledger"]["invoices"].get(proposal.get("invoice_id"))
        amount = invoice["amount_cents"] if invoice else None

        def decision(eligible, code):
            return {"eligible": eligible, "code": code, "invoice_amount_cents": amount}

        if proposal.get("action") != "refund":
            return decision(False, "unsupported-action")
        if (proposal.get("customer_id") != request["customer_id"]
                or proposal.get("invoice_id") != request["invoice_id"]):
            return decision(False, "subject-mismatch")
        if proposal.get("reason") not in ALLOWED_REASONS:
            return decision(False, "reason-not-allowed")
        if invoice is None:
            return decision(False, "unknown-invoice")
        if invoice["customer_id"] != proposal["customer_id"]:
            return decision(False, "wrong-customer")
        if invoice["status"] != "paid":
            return decision(False, "not-paid")
        cents = proposal.get("amount_cents")
        if isinstance(cents, bool) or not isinstance(cents, int) or cents < 1:
            return decision(False, "amount")
        if cents > invoice["amount_cents"]:
            return decision(False, "above-invoice")
        return decision(True, "ok")

    def refund(self, key, customer_id, invoice_id, amount_cents, reason):
        body = {"op": "refund", "customer_id": customer_id, "invoice_id": invoice_id,
                "amount_cents": amount_cents, "reason": reason}

        def run(state):
            self._remember(state, "ledger", "refund", key)
            saved = self._replay(state, key, body, "ledger.apply")
            if saved is not None:
                return saved
            invoice = state["ledger"]["invoices"].get(invoice_id)
            if invoice is None or invoice["customer_id"] != customer_id:
                raise OfficeFault("ledger.apply", "rejected", "invoice is not on this customer")
            if amount_cents > invoice["amount_cents"]:
                raise OfficeFault("ledger.apply", "rejected", "refund exceeds the invoice")
            entry_id = f"led-{len(state['ledger']['entries']) + 1}"
            state["ledger"]["entries"].append({
                "id": entry_id, "kind": "refund", "customer_id": customer_id,
                "invoice_id": invoice_id, "amount_cents": amount_cents, "reason": reason,
                "idempotency_key": key, "reversed": False,
            })
            result = {"entry_id": entry_id, "amount_cents": amount_cents, "invoice_id": invoice_id,
                      "customer_id": customer_id, "idempotency_key": key,
                      "refunded_cents": refunded_cents(state, invoice_id)}
            state["effects"][key] = {"body": body, "result": result}
            return result

        return self._update(run)

    def read_refund(self, effect):
        state = self.read()
        entry = _find(state["ledger"]["entries"], effect["entry_id"])
        posted = refunded_cents(state, effect["invoice_id"])
        if (entry is None or entry["kind"] != "refund" or entry["reversed"]
                or entry["amount_cents"] != effect["amount_cents"]
                or entry["customer_id"] != effect["customer_id"]
                or entry["invoice_id"] != effect["invoice_id"]
                or posted != effect["amount_cents"]):
            raise OfficeFault("ledger.verify", "read-back", "ledger refund did not land")
        return {"ok": True, "entry_id": entry["id"], "refunded_cents": posted}

    def reverse_refund(self, key, effect):
        body = {"op": "reversal", "entry_id": effect["entry_id"], "invoice_id": effect["invoice_id"],
                "amount_cents": effect["amount_cents"]}

        def run(state):
            self._remember(state, "ledger", "reversal", key)
            saved = self._replay(state, key, body, "ledger.compensate")
            if saved is not None:
                return saved
            entry = _find(state["ledger"]["entries"], effect["entry_id"])
            if entry is None or entry["kind"] != "refund":
                raise OfficeFault("ledger.compensate", "rejected", "refund entry is missing")
            entry["reversed"] = True
            reversal_id = f"led-{len(state['ledger']['entries']) + 1}"
            state["ledger"]["entries"].append({
                "id": reversal_id, "kind": "reversal", "reverses": entry["id"],
                "customer_id": entry["customer_id"], "invoice_id": entry["invoice_id"],
                "amount_cents": entry["amount_cents"], "idempotency_key": key,
            })
            result = {"entry_id": reversal_id, "reverses": entry["id"], "invoice_id": entry["invoice_id"],
                      "amount_cents": entry["amount_cents"], "idempotency_key": key,
                      "refunded_cents": refunded_cents(state, entry["invoice_id"])}
            state["effects"][key] = {"body": body, "result": result}
            return result

        return self._update(run)

    def read_reversal(self, effect):
        state = self.read()
        reversal = _find(state["ledger"]["entries"], effect["entry_id"])
        original = _find(state["ledger"]["entries"], effect["reverses"])
        posted = refunded_cents(state, effect["invoice_id"])
        if (reversal is None or reversal["kind"] != "reversal" or original is None
                or not original["reversed"] or posted != 0):
            raise OfficeFault("ledger.compensate.verify", "read-back", "ledger reversal did not land")
        return {"ok": True, "entry_id": reversal["id"], "refunded_cents": posted}

    def add_note(self, key, customer_id, invoice_id, text, entry_id):
        body = {"op": "note", "customer_id": customer_id, "invoice_id": invoice_id,
                "text": text, "entry_id": entry_id}

        def run(state):
            self._remember(state, "crm", "note", key)
            saved = self._replay(state, key, body, "crm.apply")
            if saved is not None:
                return saved
            record = state["crm"].get(customer_id)
            if record is None:
                raise OfficeFault("crm.apply", "rejected", "customer is not in the CRM")
            note_id = f"note-{len(record['notes']) + 1}"
            record["notes"].append({
                "id": note_id, "text": text, "invoice_id": invoice_id, "entry_id": entry_id,
                "void": False, "idempotency_key": key,
            })
            result = {"note_id": note_id, "customer_id": customer_id, "text": text,
                      "entry_id": entry_id, "idempotency_key": key}
            state["effects"][key] = {"body": body, "result": result}
            return result

        return self._update(run)

    def read_note(self, effect):
        state = self.read()
        notes = state["crm"][effect["customer_id"]]["notes"]
        note = _find(notes, effect["note_id"])
        if note is None or note["void"] or note["text"] != effect["text"] or note["entry_id"] != effect["entry_id"]:
            raise OfficeFault("crm.verify", "read-back", "CRM note did not land")
        return {"ok": True, "note_id": note["id"]}

    def void_note(self, key, effect):
        body = {"op": "void", "customer_id": effect["customer_id"], "note_id": effect["note_id"]}

        def run(state):
            self._remember(state, "crm", "void", key)
            saved = self._replay(state, key, body, "crm.compensate")
            if saved is not None:
                return saved
            note = _find(state["crm"][effect["customer_id"]]["notes"], effect["note_id"])
            if note is None:
                raise OfficeFault("crm.compensate", "rejected", "CRM note is missing")
            note["void"] = True
            result = {"note_id": note["id"], "customer_id": effect["customer_id"], "void": True,
                      "idempotency_key": key}
            state["effects"][key] = {"body": body, "result": result}
            return result

        return self._update(run)

    def read_void(self, effect):
        state = self.read()
        note = _find(state["crm"][effect["customer_id"]]["notes"], effect["note_id"])
        if note is None or not note["void"]:
            raise OfficeFault("crm.compensate.verify", "read-back", "CRM note is still active")
        return {"ok": True, "note_id": note["id"], "void": True}

    def send_notice(self, key, customer_id, invoice_id, text):
        body = {"op": "send", "customer_id": customer_id, "invoice_id": invoice_id, "text": text}

        def run(state):
            self._remember(state, "notify", "send", key)
            saved = self._replay(state, key, body, "notify.apply")
            if saved is not None:
                return saved
            if customer_id not in state["crm"]:
                raise OfficeFault("notify.apply", "rejected", "customer is unknown")
            message_id = f"msg-{len(state['notify']['messages']) + 1}"
            state["notify"]["messages"].append({
                "id": message_id, "customer_id": customer_id, "invoice_id": invoice_id,
                "text": text, "status": "sent", "idempotency_key": key,
            })
            result = {"message_id": message_id, "customer_id": customer_id, "text": text,
                      "status": "sent", "idempotency_key": key}
            state["effects"][key] = {"body": body, "result": result}
            return result

        return self._update(run)

    def read_notice(self, effect):
        state = self.read()
        message = _find(state["notify"]["messages"], effect["message_id"])
        if (message is None or message["status"] != "sent" or message["text"] != effect["text"]
                or message["customer_id"] != effect["customer_id"]):
            raise OfficeFault("notify.verify", "read-back", "notification did not land")
        return {"ok": True, "message_id": message["id"], "status": "sent"}

    def recall_notice(self, key, effect):
        body = {"op": "recall", "message_id": effect["message_id"], "customer_id": effect["customer_id"]}

        def run(state):
            self._remember(state, "notify", "recall", key)
            saved = self._replay(state, key, body, "notify.compensate")
            if saved is not None:
                return saved
            message = _find(state["notify"]["messages"], effect["message_id"])
            if message is None:
                raise OfficeFault("notify.compensate", "rejected", "notification is missing")
            message["status"] = "recalled"
            result = {"message_id": message["id"], "customer_id": effect["customer_id"],
                      "status": "recalled", "idempotency_key": key}
            state["effects"][key] = {"body": body, "result": result}
            return result

        return self._update(run)

    def read_recall(self, effect):
        state = self.read()
        message = _find(state["notify"]["messages"], effect["message_id"])
        if message is None or message["status"] != "recalled":
            raise OfficeFault("notify.compensate.verify", "read-back", "notification was not recalled")
        return {"ok": True, "message_id": message["id"], "status": "recalled"}

    def _lock(self):
        self.root.mkdir(parents=True, exist_ok=True)
        lock = open(self.root / "lock", "a+")
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        except BaseException:
            lock.close()
            raise
        return _Unlock(lock)

    def _load(self):
        if not self._state_path.exists():
            raise OfficeFault("office", "missing-office", "office directory has no ledger")
        return json.loads(self._state_path.read_text())

    def _update(self, function):
        with self._lock():
            state = self._load()
            result = function(state)
            _atomic_json(self._state_path, state)
            return result

    def _remember(self, state, system, op, key):
        state["requests"].append({"system": system, "op": op, "key": key, "replay": key in state["effects"]})

    def _replay(self, state, key, body, step):
        existing = state["effects"].get(key)
        if existing is None:
            return None
        if existing["body"] != body:
            raise OfficeFault(step, "conflict", "idempotency key reused with a different body")
        return existing["result"]


class _Unlock:
    def __init__(self, lock):
        self._lock = lock

    def __enter__(self):
        return self._lock

    def __exit__(self, *unused):
        self._lock.close()


def stub_propose(request):
    """Deterministic stand-in for a model. It sees the request and nothing else."""
    amount = request["amount_cents"]
    invoice = request["invoice_id"]
    reason = request["reason"]
    return {
        "action": "refund",
        "amount_cents": amount,
        "customer_id": request["customer_id"],
        "invoice_id": invoice,
        "reason": reason,
        "crm_note": f"Refund {amount} cents on {invoice} ({reason}).",
        "customer_message": f"We refunded {amount} cents for invoice {invoice}.",
        "proposer": "stub",
    }


def over_refund_propose(request):
    """A stub that asks for one cent more than the customer did. Eligibility must refuse it."""
    proposal = stub_propose(request)
    amount = request["amount_cents"] + 1
    proposal["amount_cents"] = amount
    proposal["proposer"] = "over-refund"
    proposal["crm_note"] = f"Refund {amount} cents on {request['invoice_id']} ({request['reason']})."
    proposal["customer_message"] = f"We refunded {amount} cents for invoice {request['invoice_id']}."
    return proposal


# Add a local model here. It must return a write intent and must not touch the network.
PROPOSERS = {"stub": stub_propose, "over-refund": over_refund_propose}


def command_propose(request):
    """Run a local program: request JSON on stdin, proposal JSON on stdout."""
    command = os.environ.get("COUNTING_HOUSE_PROPOSER")
    if not command:
        raise ValueError("COUNTING_HOUSE_PROPOSER must name a local program")
    import subprocess
    completed = subprocess.run(
        [command], input=json.dumps(request).encode(), stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, timeout=30, check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", "replace")[:500]
        raise ValueError(detail or "proposer command failed")
    if len(completed.stdout) > 1_048_576:
        raise ValueError("proposer output is too large")
    proposal = json.loads(completed.stdout)
    if not isinstance(proposal, dict):
        raise ValueError("proposer output must be a JSON object")
    proposal.setdefault("proposer", "command")
    return proposal


def propose_refund(request):
    """Write intent. Called once inside the propose step; the engine saves the result."""
    name = request.get("proposer") or "stub"
    if name == "command":
        return command_propose(request)
    proposer = PROPOSERS.get(name)
    if proposer is None:
        known = ", ".join(sorted([*PROPOSERS, "command"]))
        raise ValueError(f"unknown proposer {name!r}; expected one of: {known}")
    return proposer(request)


def proposal_matches_policy(request, proposal, invoice_amount_cents):
    """Gold label for the eval export. Independent of whether a person approved.

    A refund proposal is correct when it names the requested customer, invoice,
    and reason, the reason is allowed, and the amount is the requested amount
    and does not exceed the paid invoice.
    """
    if not isinstance(invoice_amount_cents, int):
        return False
    amount = proposal.get("amount_cents")
    return (
        proposal.get("action") == "refund"
        and proposal.get("customer_id") == request["customer_id"]
        and proposal.get("invoice_id") == request["invoice_id"]
        and proposal.get("reason") == request["reason"]
        and request["reason"] in ALLOWED_REASONS
        and isinstance(amount, int) and not isinstance(amount, bool)
        and amount == request["amount_cents"]
        and 1 <= amount <= invoice_amount_cents
    )


def _identifier(value, name, limit=64):
    if (not isinstance(value, str) or not 1 <= len(value) <= limit or not value.isascii()
            or any(not (char.isalnum() or char in "_.-") for char in value)):
        raise ValueError(f"{name} must be 1-{limit} ASCII letters, digits, dot, underscore, or hyphen")
    return value


def _amount(value):
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 9_007_199_254_740_991:
        raise ValueError("amount_cents must be a positive safe integer")
    return value


def validate_inputs(inputs):
    if not isinstance(inputs, dict):
        raise ValueError("inputs must be an object")
    office = inputs.get("office_dir")
    if not isinstance(office, str) or not Path(office).is_absolute():
        raise ValueError("office_dir must be an absolute path")
    fail_at = inputs.get("fail_at")
    if fail_at is not None and fail_at not in SYSTEMS:
        raise ValueError("fail_at must be ledger, crm, or notify")
    request = inputs.get("request")
    if not isinstance(request, dict):
        raise ValueError("request must be an object")
    reason = request.get("reason")
    if not isinstance(reason, str) or not 1 <= len(reason) <= 32 or any(not (char.islower() or char == "-") for char in reason):
        raise ValueError("reason must be a short lowercase word")
    note = request.get("note", "")
    if not isinstance(note, str) or len(note) > 500:
        raise ValueError("note must be a string of at most 500 characters")
    proposer = request.get("proposer") or "stub"
    if proposer != "command" and proposer not in PROPOSERS:
        raise ValueError("proposer must be a PROPOSERS name or 'command'")
    return {
        "office_dir": office,
        "fail_at": fail_at,
        "request": {
            "customer_id": _identifier(request.get("customer_id"), "customer_id"),
            "invoice_id": _identifier(request.get("invoice_id"), "invoice_id"),
            "amount_cents": _amount(request.get("amount_cents")),
            "reason": reason,
            "note": note,
            "proposer": proposer,
        },
    }


def validate_proposal(proposal):
    if not isinstance(proposal, dict):
        raise ValueError("proposal must be an object")
    for name in ("action", "customer_id", "invoice_id", "reason", "crm_note", "customer_message", "proposer"):
        if not isinstance(proposal.get(name), str) or not proposal[name] or len(proposal[name]) > 500:
            raise ValueError(f"proposal.{name} must be a non-empty string of at most 500 characters")
    if proposal["action"] == "refund":
        _amount(proposal.get("amount_cents"))
    return proposal


def _validate_approval(approval):
    if not isinstance(approval, dict) or not isinstance(approval.get("approved"), bool):
        raise ValueError("approval signal must contain a boolean 'approved' field")
    who = approval.get("by")
    if not isinstance(who, str) or not who.strip() or len(who) > 128:
        raise ValueError("approval signal must name who decided")
    note = approval.get("note", "")
    if not isinstance(note, str) or len(note) > 500:
        raise ValueError("approval note must be a string of at most 500 characters")
    return approval


def _refuse(spec, system, step):
    if spec["fail_at"] == system:
        raise OfficeFault(step, "system-down", f"{system} rejected the write")


def _effect_id(effect):
    for key in ("entry_id", "note_id", "message_id"):
        if key in effect:
            return effect[key]
    return None


def _finish(ctx, office, spec, proposal, eligibility, approval, applied, verified, compensations,
            outcome, failed_step, error):
    request = spec["request"]
    events = [
        {"step": "propose", "proposer": proposal["proposer"], "action": proposal["action"],
         "amount_cents": proposal.get("amount_cents")},
        {"step": "eligibility", "eligible": eligibility["eligible"], "code": eligibility["code"]},
    ]
    if approval is not None:
        events.append({"step": "approval", "approved": approval["approved"], "by": approval["by"]})
    for name in SYSTEMS:
        if name in applied:
            events.append({"step": f"{name}.apply", "id": _effect_id(applied[name])})
        if name in verified:
            events.append({"step": f"{name}.verify", "ok": True})
    for name in reversed(SYSTEMS):
        if name in compensations:
            events.append({"step": f"{name}.compensate", "id": _effect_id(compensations[name])})
    document = {
        "task_id": ctx.task["id"],
        "outcome": outcome,
        "request": request,
        "proposal": proposal,
        "eligibility": eligibility,
        "approval": approval,
        "failed_step": failed_step,
        "error": error,
        "effects": applied,
        "verifications": verified,
        "compensations": compensations,
        "events": events,
    }

    def save():
        document["systems"] = office.snapshot(request["customer_id"], request["invoice_id"])
        office.write_audit(document)
        ctx.log(f"outcome {outcome} {ctx.task['id']}")
        return document

    return ctx.step("audit", save)


def refund(ctx, inputs):
    """Saga for one refund case. Restarting a worker resumes committed steps."""
    spec = validate_inputs(inputs)
    request = spec["request"]
    office = Office(spec["office_dir"])
    proposal = ctx.step("propose", lambda: validate_proposal(propose_refund(request)))
    eligibility = ctx.step("eligibility", lambda: office.eligibility(request, proposal))
    if not eligibility["eligible"]:
        return _finish(ctx, office, spec, proposal, eligibility, None, {}, {}, {},
                       "ineligible", None, eligibility["code"])

    approval = _validate_approval(ctx.wait_signal("approval"))
    if not approval["approved"]:
        return _finish(ctx, office, spec, proposal, eligibility, approval, {}, {}, {},
                       "rejected", None, None)

    applied, verified = {}, {}
    try:
        def apply_ledger():
            _refuse(spec, "ledger", "ledger.apply")
            return office.refund(
                ctx.idempotency_key("ledger.apply"), proposal["customer_id"], proposal["invoice_id"],
                proposal["amount_cents"], proposal["reason"])

        def apply_crm():
            _refuse(spec, "crm", "crm.apply")
            return office.add_note(
                ctx.idempotency_key("crm.apply"), proposal["customer_id"], proposal["invoice_id"],
                proposal["crm_note"], applied["ledger"]["entry_id"])

        def apply_notify():
            _refuse(spec, "notify", "notify.apply")
            return office.send_notice(
                ctx.idempotency_key("notify.apply"), proposal["customer_id"], proposal["invoice_id"],
                proposal["customer_message"])

        applied["ledger"] = ctx.step("ledger.apply", apply_ledger)
        verified["ledger"] = ctx.step("ledger.verify", lambda: office.read_refund(applied["ledger"]))
        applied["crm"] = ctx.step("crm.apply", apply_crm)
        verified["crm"] = ctx.step("crm.verify", lambda: office.read_note(applied["crm"]))
        applied["notify"] = ctx.step("notify.apply", apply_notify)
        verified["notify"] = ctx.step("notify.verify", lambda: office.read_notice(applied["notify"]))
    except OfficeFault as failure:
        compensations = {}
        # Later systems first, so a customer is not left with a notice for a refund we undid.
        if "notify" in applied:
            compensations["notify"] = ctx.step("notify.compensate", lambda: office.recall_notice(
                ctx.idempotency_key("notify.compensate"), applied["notify"]))
            ctx.step("notify.compensate.verify", lambda: office.read_recall(compensations["notify"]))
        if "crm" in applied:
            compensations["crm"] = ctx.step("crm.compensate", lambda: office.void_note(
                ctx.idempotency_key("crm.compensate"), applied["crm"]))
            ctx.step("crm.compensate.verify", lambda: office.read_void(compensations["crm"]))
        if "ledger" in applied:
            compensations["ledger"] = ctx.step("ledger.compensate", lambda: office.reverse_refund(
                ctx.idempotency_key("ledger.compensate"), applied["ledger"]))
            ctx.step("ledger.compensate.verify", lambda: office.read_reversal(compensations["ledger"]))
        return _finish(ctx, office, spec, proposal, eligibility, approval, applied, verified,
                       compensations, "compensated", failure.step, str(failure))
    return _finish(ctx, office, spec, proposal, eligibility, approval, applied, verified, {},
                   "completed", None, None)


HANDLERS = {HANDLER: refund}


def case_inputs(office_dir, *, amount_cents=INVOICE_CENTS, reason="duplicate",
                note="Charged twice on Tuesday.", proposer="stub", fail_at=None,
                customer_id=CUSTOMER_ID, invoice_id=INVOICE_ID):
    """Inputs for one case. Seeds the office directory when it is empty."""
    office = Office(Path(office_dir).expanduser().resolve()).seed()
    return validate_inputs({
        "office_dir": str(office.root),
        "fail_at": fail_at,
        "request": {
            "customer_id": customer_id,
            "invoice_id": invoice_id,
            "amount_cents": amount_cents,
            "reason": reason,
            "note": note,
            "proposer": proposer,
        },
    })


def send_decision(client, task_id, approved, by, note):
    """Deliver the human approval signal. One value per task; retries send the same value."""
    _identifier(task_id, "task id", 128)
    if not isinstance(approved, bool):
        raise ValueError("approved must be a boolean")
    if not isinstance(by, str) or not by.strip():
        raise ValueError("approval needs the name of the person who decided")
    return client.signal(task_id, "approval", {"approved": approved, "by": by, "note": note or ""})


def export_runs(client, destination):
    """Write evals.jsonl and one audit-plus-history file per completed refund."""
    destination = Path(destination)
    listing = client.list_tasks()
    if listing.get("truncated"):
        raise RuntimeError("the engine returned only the first 100 tasks; refusing a partial eval export")
    tasks = [task for task in listing["tasks"]
             if task.get("handler") == HANDLER and task.get("status") == "completed"]
    tasks.sort(key=lambda task: task["id"])
    runs = destination / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    rows = []
    for task in tasks:
        _identifier(task["id"], "task id", 128)
        view = client.view(task["id"])
        output = view["output"]
        history = client.history(task["id"])
        row = {
            "task_id": task["id"],
            "proposal": output["proposal"],
            "approval": output["approval"],
            "outcome": output["outcome"],
            "correct": proposal_matches_policy(
                output["request"], output["proposal"], output["eligibility"]["invoice_amount_cents"]),
        }
        rows.append(row)
        (runs / f"{task['id']}.json").write_text(json.dumps(
            {"task_id": task["id"], "audit": output, "history": history}, indent=2, sort_keys=True) + "\n")
    (destination / "evals.jsonl").write_text("".join(
        json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows))
    return rows


def client_from_env():
    mode = os.environ.get("DEOOS_MODE", "library")
    if mode == "server":
        url = os.environ.get("ENGINE_URL")
        if not url:
            raise ValueError("ENGINE_URL is required when DEOOS_MODE=server")
        return Client.remote(url, token=os.environ.get("ENGINE_TOKEN"))
    if mode != "library":
        raise ValueError("DEOOS_MODE must be library or server")
    return Client()


def _print(value):
    print(json.dumps(value, indent=2, sort_keys=True))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    submit = commands.add_parser("submit", help="seed the local office and submit one refund case")
    submit.add_argument("--id", required=True)
    submit.add_argument("--office", type=Path, required=True)
    submit.add_argument("--amount-cents", type=int, default=INVOICE_CENTS)
    submit.add_argument("--reason", default="duplicate")
    submit.add_argument("--note", default="Charged twice on Tuesday.")
    submit.add_argument("--proposer", default="stub")
    submit.add_argument("--fail-at", choices=SYSTEMS)
    submit.add_argument("--customer", default=CUSTOMER_ID)
    submit.add_argument("--invoice", default=INVOICE_ID)
    commands.add_parser("work", help="run a worker until interrupted")
    approve = commands.add_parser("approve", help="approve a waiting refund")
    approve.add_argument("--id", required=True)
    approve.add_argument("--by", required=True)
    approve.add_argument("--note", default="Matches the refund policy.")
    reject = commands.add_parser("reject", help="reject a waiting refund")
    reject.add_argument("--id", required=True)
    reject.add_argument("--by", required=True)
    reject.add_argument("--note", default="Rejected.")
    export = commands.add_parser("export", help="write evals.jsonl and per-run audit history")
    export.add_argument("--out", type=Path, required=True)
    inspect = commands.add_parser("inspect", help="show the task view and recent engine history")
    inspect.add_argument("--id", required=True)

    args = parser.parse_args(argv)
    client = client_from_env()
    try:
        if args.command == "submit":
            inputs = case_inputs(
                args.office, amount_cents=args.amount_cents, reason=args.reason, note=args.note,
                proposer=args.proposer, fail_at=args.fail_at, customer_id=args.customer, invoice_id=args.invoice)
            _print(client.submit(_identifier(args.id, "task id", 128), HANDLER, inputs, max_attempts=5))
        elif args.command == "work":
            import signal
            import threading
            stop = threading.Event()

            def stop_on_signal(signum, frame):
                stop.set()

            signal.signal(signal.SIGINT, stop_on_signal)
            signal.signal(signal.SIGTERM, stop_on_signal)
            client.run_worker(HANDLERS, stop_event=stop)
        elif args.command == "approve":
            _print(send_decision(client, args.id, True, args.by, args.note))
        elif args.command == "reject":
            _print(send_decision(client, args.id, False, args.by, args.note))
        elif args.command == "export":
            rows = export_runs(client, args.out)
            print(f"wrote {len(rows)} eval rows to {args.out / 'evals.jsonl'}")
        else:
            _print({"view": client.view(args.id), "history": client.history(args.id)})
    finally:
        client.close()


if __name__ == "__main__":
    main()
