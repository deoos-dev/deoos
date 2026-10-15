"""Index eight local documents, kill twice, and recover across Python/TypeScript."""
import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.request
import uuid

import boto3
from botocore.exceptions import ClientError
from document_index import HANDLER, index_documents, index_inputs, search
from devto_etl_demo import check, client_for, run_task, start_server

STORAGE = dict(provider="s3", region="us-east-1", endpoint="http://127.0.0.1:19000",
               allow_http=True, access_key_id="local-development",
               secret_access_key="local-development-only-secret", lease_ms=2000)
# Self-authored fixtures: CRLF, BOM, Unicode, an empty document, repeated terms,
# numeric object keys, and a token that is also a JavaScript property name.
TEXTS = (
    "Orion rover maps solar dunes.\r\nMaps repeat; maps repeat.\r\n",
    "\ufeffLuna studies lunar water and ice.\nA rover carries cameras. 🌙\n",
    'The café archive stores a résumé.\nİstanbul notes: naïve sketches, "quiet" rooms.\n',
    "",
    "A river flows past the quiet library.\nReaders search old maps.\n",
    "Robots sort 42 copper bolts and 2 steel gears.\nConstructor constructor 10 robots.\n",
    "A garden grows basil, mint, and beans.\nRain waters the garden.\n",
    "Résumé notes: café rain and solar maps.\nOrion watches stars; repeated repeated.\n",
)
QUERIES = {"solar maps": [1, 8], "rover": [1, 2], "lunar water": [2],
           "café résumé": [3, 8], "constructor 42": [6], "garden rain": [7],
           "quiet": [3, 5], "İstanbul": [3], "": [], "unfindable": []}


def private_json(path, value):
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    path.chmod(0o600)


def record_callback(config, name):
    with os.fdopen(os.open(config["ledger"], os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600), "a") as stream:
        stream.write(json.dumps({"step": name, "language": "python", "stage": config["stage"]}) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def python_worker(config):
    if "url" in config["client"]:
        for key in list(os.environ):
            if key.startswith(("AWS_", "DEOOS_", "GOOGLE_", "AZURE_")):
                os.environ.pop(key)

    def pause():
        Path(config["ready"]).write_text(config["stage"])
        while True:
            time.sleep(1)

    def interrupted(ctx, inputs):
        original = ctx.step

        def step(name, callback):
            def execute():
                result = callback()
                record_callback(config, name)
                if config["stage"] == "publish-gap" and name == "publish":
                    pause()  # Published bytes exist; their checkpoint does not.
                return result
            result = original(name, execute)
            if config["stage"] == "read-four" and name == "read-4":
                pause()  # The complete fourth source text is already durable.
            return result

        ctx.step = step
        return index_documents(ctx, inputs)

    with client_for(config["client"]) as client:
        run_task(client, config["task_id"], {HANDLER: interrupted})


NODE_WORKER = """import {readFileSync,writeFileSync,openSync,writeSync,fsyncSync,closeSync} from 'node:fs';
import {setTimeout as delay} from 'node:timers/promises';
const config=JSON.parse(readFileSync(process.argv[1],'utf8'));
const {Client}=await import(config.sdk);const {HANDLER,indexDocuments}=await import(config.example);
const client=config.client.url?Client.remote(config.client.url):new Client(config.client);
async function handler(ctx,inputs){
 const original=ctx.step.bind(ctx);
 async function pause(){writeFileSync(config.ready,config.stage);while(true)await delay(1000);}
 ctx.step=async(name,fn)=>{
  const result=await original(name,async()=>{const result=await fn();
   const fd=openSync(config.ledger,'a',0o600);
   try{writeSync(fd,JSON.stringify({step:name,language:'typescript',stage:config.stage})+'\\n');fsyncSync(fd);}finally{closeSync(fd);}
   if(config.stage==='publish-gap'&&name==='publish')await pause();return result;});
  if(config.stage==='read-four'&&name==='read-4')await pause();return result;};
 return indexDocuments(ctx,inputs);
}
const deadline=Date.now()+120000;
while(Date.now()<deadline){const state=await client.inspect(config.task_id);
 if(state.status==='completed')process.exit(0);
 if(['failed','cancelled'].includes(state.status))throw new Error(JSON.stringify(state));
 await client.runOnce({[HANDLER]:handler});await delay(100);}
throw new Error('Document indexing timed out');
"""
NODE_SEARCH = """import {readFileSync} from 'node:fs';
const {search}=await import(process.argv[2]);
const index=JSON.parse(readFileSync(process.argv[1],'utf8'));
console.log(JSON.stringify(JSON.parse(process.argv[3]).map(query=>search(index,query))));
"""


def run_worker(config, task_id, stage, language, sdk, directory):
    ready = directory / f"{stage}-ready"
    private = directory / f"{stage}-worker.json"
    private_json(private, dict(client=config, task_id=task_id, stage=stage, ready=str(ready),
        ledger=str(directory / "callbacks.jsonl"), sdk=sdk.resolve().as_uri() if sdk else None,
        example=Path(__file__).with_name("document_index.mjs").as_uri()))
    command = ([sys.executable, __file__, "--worker", str(private)] if language == "python"
               else ["node", "--input-type=module", "-e", NODE_WORKER, str(private)])
    env = dict(os.environ)
    if "url" in config:
        env = {key: value for key, value in env.items()
               if not key.startswith(("AWS_", "DEOOS_", "GOOGLE_", "AZURE_"))}
    log_path = directory / f"{stage}-worker.log"
    with os.fdopen(os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as log:
        process = subprocess.Popen(command, env=env, stdout=log, stderr=log)
        try:
            if stage == "finish":
                check(process.wait(timeout=120) == 0, f"Recovery worker failed; inspect {log_path}")
                return
            deadline = time.monotonic() + 120
            while not ready.exists():
                check(process.poll() is None, f"Worker exited before {stage}; inspect {log_path}")
                check(time.monotonic() < deadline, f"Worker did not reach {stage}")
                time.sleep(.1)
            check(ready.read_text() == stage, "Unexpected recovery barrier")
            process.kill()
            process.wait(timeout=10)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)


def expected_index(paths):
    documents, terms = [], {}
    for number, (path, text) in enumerate(zip(paths, TEXTS), 1):
        tokens = sorted({token.lower() for token in re.findall(r"[A-Za-z0-9]+", text)})
        document = {"id": f"doc-{number}", "path": str(path), "text": text, "tokens": tokens}
        documents.append(document)
        for token in tokens:
            terms.setdefault(token, []).append(document["id"])
    return {"documents": documents, "terms": terms}


def callbacks(directory):
    return collections.Counter(row["step"] for row in
        (json.loads(line) for line in (directory / "callbacks.jsonl").read_text().splitlines()))


def stop(process):
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def main(args):
    directory = args.directory.resolve() / uuid.uuid4().hex[:12]
    directory.mkdir(parents=True)
    bucket = "deoos-documents-" + uuid.uuid4().hex[:20]
    report = {"bucket": bucket, "prefix": "document-index/", "endpoint": args.endpoint,
              "cases": [], "cleaned": False, "fixture": "Eight self-authored UTF-8 local text documents; ASCII token search."}
    evidence = directory / "result.json"
    private_json(evidence, report)  # Record identity before attempting to create storage.
    s3, create_intent, failed = None, False, False
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                with urllib.request.urlopen(args.endpoint + "/health", timeout=2) as response:
                    if response.status == 200:
                        break
            except OSError:
                pass
            check(time.monotonic() < deadline, "RustFS did not become ready")
            time.sleep(.5)
        s3 = boto3.client("s3", endpoint_url=args.endpoint, region_name=STORAGE["region"],
                          aws_access_key_id=STORAGE["access_key_id"], aws_secret_access_key=STORAGE["secret_access_key"])
        create_intent = True
        s3.create_bucket(Bucket=bucket)
        for mode in ("library", "server"):
            for language in (("python", "typescript") if args.node_sdk else ("python",)):
                other = "typescript" if language == "python" and args.node_sdk else "python"
                label = f"{mode}-{language}"
                case = directory / label
                case.mkdir()
                corpus = case / "documents"
                corpus.mkdir()
                paths = [corpus / f"document-{number}.txt" for number in range(1, 9)]
                for path, text in zip(paths, TEXTS):
                    path.write_bytes(text.encode("utf-8"))
                expected = expected_index(paths)
                output = case / "index.json"
                prefix, task_id = report["prefix"] + label, "documents-001"
                config = dict(STORAGE, endpoint=args.endpoint, bucket=bucket, prefix=prefix)
                server = None
                with (case / "server.log").open("w") as log:
                    try:
                        if mode == "server":
                            server, config = start_server(args.server, bucket, prefix, log, endpoint=args.endpoint)
                        with client_for(config) as client:
                            client.submit(task_id, HANDLER, index_inputs([str(path) for path in paths], str(output)), max_attempts=5)
                            run_worker(config, task_id, "read-four", language, args.node_sdk, case)
                            first = client.inspect(task_id)
                            check(first["status"] == "running" and first["attempts"] == 1, "Wrong first crash state")
                            first_names = {f"read-{n}" for n in range(1, 5)} | {f"index-{n}" for n in range(1, 4)}
                            check(set(first["steps"]) == first_names, "Midway checkpoint boundary differs")
                            check(callbacks(case) == {name: 1 for name in first_names}, "Unexpected initial callback counts")
                            check(not output.exists(), "Index published before all documents were extracted")
                            for path in paths[:4]:
                                path.write_bytes(b"Changed after the saved read; recovery must use its checkpoint.\n")
                            run_worker(config, task_id, "publish-gap", other, args.node_sdk, case)
                            gap = client.inspect(task_id)
                            names = {f"{step}-{n}" for step in ("read", "index") for n in range(1, 9)}
                            check(gap["status"] == "running" and gap["attempts"] == 2, "Wrong publication crash state")
                            check(set(gap["steps"]) == names, "Publish checkpoint already exists or extraction missing")
                            check(callbacks(case) == dict.fromkeys(names | {"publish"}, 1), "Completed extraction callbacks ran again")
                            written = output.read_bytes()
                            canonical = (json.dumps(expected, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
                            check(written == canonical, "Published index differs from full original documents")
                            run_worker(config, task_id, "finish", language, args.node_sdk, case)
                            final = client.inspect(task_id)
                            check(final["status"] == "completed" and final["attempts"] == 3, "Workflow did not recover from both hard kills")
                            expected_counts = dict.fromkeys(names, 1) | {"publish": 2}
                            check(callbacks(case) == expected_counts, "Extraction repeated or publication was not replayed")
                            check(output.read_bytes() == written, "Publication replay changed index bytes")
                            for earlier in (first, gap):
                                check(all(final["steps"].get(name) == pointer for name, pointer in earlier["steps"].items()), "Saved checkpoint pointers changed")
                            check(all(gap["steps"].get(name) == pointer for name, pointer in first["steps"].items()), "Pointers changed across first recovery")
                            for number, text in enumerate(TEXTS, 1):
                                check(client.request(f"/tasks/{task_id}/steps/read-{number}") == text, "Saved source text changed")
                                check(client.request(f"/tasks/{task_id}/steps/index-{number}") == expected["documents"][number-1], "Saved indexed document changed")
                            result = {"output_path": str(output), "documents": 8, "terms": len(expected["terms"])}
                            check(final["output"] == result and client.request(f"/tasks/{task_id}/steps/publish") == result, "Saved publication result differs")
                            actual = json.loads(written)
                            hits = [[expected["documents"][n-1] for n in numbers] for numbers in QUERIES.values()]
                            check([search(actual, query) for query in QUERIES] == hits, "Python search returned incorrect content")
                            if args.node_sdk:
                                node_hits = json.loads(subprocess.check_output(["node", "--input-type=module", "-e", NODE_SEARCH,
                                    str(output), Path(__file__).with_name("document_index.mjs").as_uri(), json.dumps(list(QUERIES))], timeout=30))
                                check(node_hits == hits, "TypeScript search returned incorrect content")
                            output.chmod(0o644)
                            private_json(case / "states.json", {"read_four": first, "publication_gap": gap, "final": final})
                            report["cases"].append({"mode": mode, "language": language, "recovery_languages": [other, language],
                                "passed": True, "attempts": 3, "documents": 8, "terms": len(expected["terms"]),
                                "callback_counts": expected_counts, "saved_payloads_verified": True, "checkpoint_pointers_preserved": True,
                                "changed_sources_not_reread": True, "publication_replayed_identically": True,
                                "search_hits": {query: [row["id"] for row in match] for query, match in zip(QUERIES, hits)},
                                "index_sha256": hashlib.sha256(written).hexdigest(), "index": str(output)})
                            private_json(evidence, report)
                            print(f"{label}: killed twice; {language} → {other} → {language}; 8 documents extracted once, publication replayed, {len(QUERIES)} searches verified.", flush=True)
                    finally:
                        stop(server)
    except BaseException as error:
        failed = True
        report["error"] = repr(error)
        raise
    finally:
        cleanup_error = None
        try:
            if create_intent:
                try:
                    s3.head_bucket(Bucket=bucket)
                except ClientError as error:
                    if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                        raise
                else:
                    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                        objects = [{"Key": row["Key"]} for row in page.get("Contents", [])]
                        check(all(row["Key"].startswith(report["prefix"]) for row in objects), "Unexpected object blocks cleanup")
                        if objects:
                            check(not s3.delete_objects(Bucket=bucket, Delete={"Objects": objects}).get("Errors"), "Storage cleanup failed")
                    s3.delete_bucket(Bucket=bucket)
                    try:
                        s3.head_bucket(Bucket=bucket)
                    except ClientError as error:
                        if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                            raise
                    else:
                        raise RuntimeError("Temporary bucket still exists")
            report["cleaned"] = True
        except BaseException as error:
            cleanup_error = error
            report["cleanup_error"] = repr(error)
        finally:
            private_json(evidence, report)
        if cleanup_error is not None and not failed:
            raise RuntimeError(f"Storage cleanup failed; inspect {evidence}") from cleanup_error
    print(f"Search indexes and recovery evidence: {directory}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default=STORAGE["endpoint"])
    parser.add_argument("--directory", type=Path, default=Path(__file__).resolve().parents[1] / "outputs" / "document-index")
    parser.add_argument("--server", type=Path)
    parser.add_argument("--node-sdk", type=Path, help="Verify both SDKs with cross-language recovery using this SDK index.js")
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        python_worker(json.loads(args.worker.read_text()))
    else:
        if not args.server:
            parser.error("--server is required")
        main(args)
