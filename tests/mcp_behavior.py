"""Official MCP client against real DEOOS subprocesses and owned storage.

Install tests/requirements.txt in a separate test venv, then install the DEOOS
wheel in the interpreter selected by --python (which may be that same venv).
Example: python tests/mcp_behavior.py --binary <deoos-server> --backend filesystem
Run --backend rustfs against an existing local RustFS via AWS_ENDPOINT. Each run
owns a fresh bucket/directory and dynamically binds its shared engine server.
--faults additionally compiles the existing macOS test-only DYLD interposer.

The pinned official SDK 2.3.0 is used in its public mode='legacy', exercising
initialize/initialized with negotiated MCP 2025-11-25, rather than the 2026 wire.
No MCP package is required by the DEOOS runtime. Cancellation suppresses the MCP
response; an already published engine mutation can still commit.
"""
import argparse
from contextlib import asynccontextmanager
import hashlib
import importlib.metadata
import json
import os
import pathlib
import platform
import select
import signal
import subprocess
import sys
import tempfile
import uuid

import anyio
from jsonschema import Draft202012Validator, validate
from mcp import Client, StdioServerParameters, stdio_client
from mcp.shared.exceptions import MCPError

from local_faults import ROOT, Server, wait_marker

READ_TOOLS = {'task_list', 'task_summary', 'task_history'}
ACTION_TOOLS = {'task_cancel', 'task_retry', 'task_signal'}
METADATA = {'id', 'handler', 'status', 'attempts', 'max_attempts', 'revision'}
SENTINELS = ['MCP-INPUT-PRIVATE', 'MCP-CHECKPOINT-PRIVATE', 'MCP-OUTPUT-PRIVATE']
WRAPPER = """import os,pathlib,sys
pathlib.Path(os.environ['MCP_TEST_PID_FILE']).write_text(str(os.getpid()))
os.execv(sys.executable, [sys.executable, '-m', 'deoos', 'mcp', *sys.argv[1:]])
"""
CLIENT_SETUP = """import json, os
from deoos import Client
c = Client.remote(os.environ['ENGINE_URL'], os.environ.get('ENGINE_TOKEN')) if os.environ.get('ENGINE_URL') else Client()
try:
"""


def sdk(python, env, code):
    source = CLIENT_SETUP + '\n'.join('    ' + line for line in code.splitlines()) + '\nfinally:\n    c.close()\n'
    result = subprocess.run([python, '-c', source], env=env, text=True,
                            capture_output=True, timeout=90)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def clean_env():
    return {key: value for key, value in os.environ.items()
            if not key.startswith(('AWS_', 'DEOOS_STORAGE_', 'DEOOS_FAULT_', 'DYLD_', 'MCP_TEST_'))
            and key not in {'ENGINE_URL', 'ENGINE_TOKEN', 'ENGINE_BIND', 'DEOOS_MODE',
                            'EXECUTION_PREFIX', 'LEASE_MS', 'PYTHONPATH',
                            'DEOOS_NATIVE_LIBRARY', 'DEOOS_NODE_LIBRARY'}}


def remote_env(env, server):
    return {**{key: value for key, value in env.items()
               if not key.startswith(('AWS_', 'DEOOS_STORAGE_', 'DEOOS_FAULT_', 'DYLD_'))
               and key not in {'EXECUTION_PREFIX', 'LEASE_MS'}},
            'ENGINE_URL': server.url}


@asynccontextmanager
async def connection(python, env, work, *, actions=False):
    pid_file = work / ('mcp-' + uuid.uuid4().hex + '.pid')
    parameters = StdioServerParameters(command=python, args=['-c', WRAPPER] + (
        ['--allow-actions'] if actions else []), env=dict(env, MCP_TEST_PID_FILE=str(pid_file)))
    with (work / (pid_file.stem + '.stderr')).open('w') as stderr:
        async with Client(stdio_client(parameters, errlog=stderr), mode='legacy',
                          cache=None, read_timeout_seconds=30) as client:
            assert client.protocol_version == '2025-11-25', client.protocol_version
            assert client.server_info.name == 'deoos'
            assert client.server_capabilities.tools is not None
            assert 'does not roll back' in client.instructions
            yield client
    # The official transport owns shutdown/reaping. Check its actual subprocess
    # disappeared; the separate EOF check below also requires orderly exit 0.
    pid = int(pid_file.read_text())
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        pass
    else:
        raise AssertionError(f'MCP subprocess survived connection close: {pid}')


async def definitions(client, actions):
    result = await client.list_tools()
    tools = {tool.name: tool.model_dump(by_alias=True, exclude_none=True) for tool in result.tools}
    assert set(tools) == READ_TOOLS | (ACTION_TOOLS if actions else set()), tools
    required = {'task_list': set(), 'task_summary': {'id'}, 'task_history': {'id'},
                'task_cancel': {'id'}, 'task_retry': {'id', 'expected_revision', 'operation_id'},
                'task_signal': {'id', 'name', 'value', 'operation_id'}}
    for name, tool in tools.items():
        schema = tool['inputSchema']
        Draft202012Validator.check_schema(schema)
        assert schema['type'] == 'object' and schema['additionalProperties'] is False
        assert set(schema.get('required', [])) == set(schema['properties']) == required[name]
        assert tool['annotations']['readOnlyHint'] == (name in READ_TOOLS)
        if name in ACTION_TOOLS:
            assert tool['annotations']['destructiveHint'] is True
        if tool.get('outputSchema') is not None:
            Draft202012Validator.check_schema(tool['outputSchema'])
    return tools


async def call(client, tools, name, arguments, *, status=None, invalid=False):
    if not invalid:
        validate(arguments, tools[name]['inputSchema'])
    result = await client.call_tool(name, arguments)
    value = result.structured_content
    assert isinstance(value, dict), result
    assert len(result.content) == 1 and result.content[0].type == 'text', result
    assert json.loads(result.content[0].text) == value
    assert bool(result.is_error) == (status is not None or invalid), result
    if status is not None:
        assert value['status'] == status and isinstance(value['error'], str), value
    elif invalid:
        assert isinstance(value['error'], str), value
    elif tools[name].get('outputSchema') is not None:
        validate(value, tools[name]['outputSchema'])
    return value


def metadata(value):
    assert set(value) == METADATA, value
    validate(value, {'type': 'object', 'required': sorted(METADATA), 'properties': {
        'id': {'type': 'string'}, 'handler': {'type': 'string'}, 'status': {'type': 'string'},
        'attempts': {'type': 'integer', 'minimum': 0}, 'max_attempts': {'type': 'integer', 'minimum': 1},
        'revision': {'type': 'string', 'minLength': 1}}, 'additionalProperties': False})


def eof(python, env):
    process = subprocess.Popen([python, '-m', 'deoos', 'mcp'], env=env, stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, encoding='utf-8')
    messages = [dict(jsonrpc='2.0', id=1, method='initialize', params={
        'protocolVersion': '2025-11-25', 'capabilities': {},
        'clientInfo': {'name': 'deoos-eof-test', 'version': '1'}}),
        dict(jsonrpc='2.0', method='notifications/initialized')]

    def exchange(raw):
        process.stdin.write(raw + '\n')
        process.stdin.flush()
        assert select.select([process.stdout], [], [], 30)[0], 'wire response timed out'
        line = process.stdout.readline()
        assert len(line.encode('utf-8')) < 2048, 'invalid request produced an unbounded reply'
        return json.loads(line)

    try:
        # This small wire check supplements the official SDK: wait for a real
        # native/remote task read so EOF must release an initialized SDK handle.
        for message in messages:
            process.stdin.write(json.dumps(message) + '\n')
            process.stdin.flush()
            if 'id' in message:
                assert select.select([process.stdout], [], [], 30)[0], 'EOF handshake/read timed out'
                response = json.loads(process.stdout.readline())
                assert response['id'] == message['id'] and 'result' in response, response
        # These intentionally malformed frames cannot be represented as valid
        # official-SDK requests. They must receive a bounded id:null error,
        # rather than echoing the oversized ID or stranding the stdio session.
        invalid_id = json.dumps(dict(jsonrpc='2.0', id='\U0001f642' * 257, method='ping'),
                                ensure_ascii=False)
        huge_integer = '{"jsonrpc":"2.0","id":' + '9' * 1025 + ',"method":"ping"}'
        for number, raw, code in ((2, invalid_id, -32600), (3, huge_integer, -32700)):
            error = exchange(raw)
            assert error['id'] is None and error['error']['code'] == code, error
            pong = exchange(json.dumps(dict(jsonrpc='2.0', id=number, method='ping')))
            assert pong == {'jsonrpc': '2.0', 'id': number, 'result': {}}, pong
        read = exchange(json.dumps(dict(jsonrpc='2.0', id=4, method='tools/call', params={
            'name': 'task_summary', 'arguments': {'id': 'completed'}})))
        assert read['id'] == 4 and read['result']['structuredContent']['status'] == 'completed', read
        process.stdin.close()
        assert process.wait(timeout=10) == 0, process.stderr.read()
        assert process.stdout.read() == '', 'unsolicited stdout after EOF'
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)
        for stream in (process.stdin, process.stdout, process.stderr):
            stream.close()


async def behavior(python, env, work, mode):
    seeded = await anyio.to_thread.run_sync(sdk, python, env, """c.submit('completed', 'complete.v1', {'secret':'MCP-INPUT-PRIVATE'})
def handler(ctx, inputs):
    ctx.step('persisted', lambda: 'MCP-CHECKPOINT-PRIVATE')
    for number in range(35):
        ctx.log('retained-message' if number == 34 else 'message-%02d' % number)
    return 'MCP-OUTPUT-PRIVATE'
assert c.run_once({'complete.v1': handler})
c.submit('pending', 'pending.v1', {})
print(json.dumps(c.inspect('completed')))
""")
    async with connection(python, env, work) as client:
        tools = await definitions(client, False)
        listing = await call(client, tools, 'task_list', {})
        assert listing['truncated'] is False and len(listing['tasks']) == 2
        for task in listing['tasks']:
            metadata(task)
        summary = await call(client, tools, 'task_summary', {'id': 'completed'})
        assert summary['id'] == 'completed' and summary['status'] == 'completed'
        assert summary['summary_version'] == 1 and summary['completed_steps'] == ['persisted']
        assert not {'inputs', 'output', 'owner', 'token', 'revision', 'generation', 'history',
                    'definitions', 'timers', 'signals', 'last_operation'}.intersection(summary)
        assert not any(secret in json.dumps([listing, summary]) for secret in SENTINELS)
        history = await call(client, tools, 'task_history', {'id': 'completed'})
        assert set(history) == {'id', 'status', 'revision', 'history', 'bounded'}
        assert history['revision'] == seeded['revision'] and history['status'] == 'completed'
        assert history['bounded'] is True and len(history['history']) == 32
        assert history['history'] == seeded['history'][-32:]
        assert any(event.get('detail') == 'retained-message' for event in history['history'])
        for name in ACTION_TOOLS:
            try:
                await client.call_tool(name, {'id': 'pending'})
            except MCPError as error:
                assert error.code == -32602, error
            else:
                raise AssertionError(f'disabled action dispatched: {name}')
        await call(client, tools, 'task_summary', {'id': 'missing'}, status=404)
        await call(client, tools, 'task_summary', {'id': '../unsafe'}, invalid=True)
        await call(client, tools, 'task_summary', {'id': 'pending', 'extra': True}, invalid=True)
        await call(client, tools, 'task_summary', {}, invalid=True)
    async with connection(python, env, work, actions=True) as client:
        tools = await definitions(client, True)
        signalled = await call(client, tools, 'task_signal', {
            'id': 'pending', 'name': 'approved', 'value': {'approved': True}, 'operation_id': 'stable-signal'})
        metadata(signalled)
        assert await call(client, tools, 'task_signal', {
            'id': 'pending', 'name': 'approved', 'value': {'approved': True},
            'operation_id': 'stable-signal'}) == signalled
        await call(client, tools, 'task_signal', {'id': 'pending', 'name': 'approved',
                   'value': False, 'operation_id': 'stable-signal'}, status=409)
        cancelled = await call(client, tools, 'task_cancel', {'id': 'pending'})
        assert cancelled['status'] == 'cancelled'
        metadata(cancelled)
        args = {'id': 'pending', 'expected_revision': signalled['revision'], 'operation_id': 'stale-retry'}
        await call(client, tools, 'task_retry', args, status=409)
        args.update(expected_revision=cancelled['revision'], operation_id='stable-retry')
        retried = await call(client, tools, 'task_retry', args)
        metadata(retried)
        assert retried['status'] == 'queued' and retried['revision'] != cancelled['revision']
        assert await call(client, tools, 'task_retry', args) == retried
        await call(client, tools, 'task_retry', {**args, 'operation_id': 'different-retry'}, status=409)
        await call(client, tools, 'task_retry', {'id': 'pending'}, invalid=True)
        await call(client, tools, 'task_signal', {'id': 'pending', 'name': 'x', 'value': 1}, invalid=True)
        await call(client, tools, 'task_signal', {'id': 'pending', 'name': 'x', 'value': 1,
                   'operation_id': '../unsafe'}, invalid=True)
        final = await call(client, tools, 'task_history', {'id': 'pending'})
        assert final['revision'] == retried['revision']
        assert sum(event['event'] == 'signal' for event in final['history']) == 1
        assert sum(event['event'] == 'retry' for event in final['history']) == 1
    # The engine listing is explicitly bounded. Read a known task beyond that
    # bound, without interpreting truncated=true as an empty or complete list.
    await anyio.to_thread.run_sync(sdk, python, env, """for number in range(101):
    c.submit('tail-%03d' % number, 'tail.v1', {})
print(json.dumps(True))
""")
    async with connection(python, env, work) as client:
        tools = await definitions(client, False)
        listing = await call(client, tools, 'task_list', {})
        assert listing['truncated'] is True and len(listing['tasks']) == 100
        for task in listing['tasks']:
            metadata(task)
        assert 'tail-100' not in {task['id'] for task in listing['tasks']}
        assert (await call(client, tools, 'task_summary', {'id': 'tail-100'}))['status'] == 'queued'
    await anyio.to_thread.run_sync(eof, python, env)
    if mode == 'server':
        async with connection(python, dict(env, ENGINE_TOKEN='wrong-test-token'), work) as client:
            tools = await definitions(client, False)
            await call(client, tools, 'task_summary', {'id': 'completed'}, status=401)
    return {'mode': mode, 'protocol': '2025-11-25', 'sdk': 'mcp==2.3.0',
            'metadata_and_progress': True, 'bounded_listing_and_history': True,
            'action_listing_and_dispatch_gated': True, 'revision_and_signal_conflicts': True,
            'wrong_token': mode == 'server', 'official_transport_reaped': True,
            'bounded_invalid_request_ids': True, 'eof_exit': 0}


async def fault_case(binary, python, base, work, library, mode):
    work.mkdir()
    prefix = 'mcp-fault-' + uuid.uuid4().hex
    storage, arm, marker = work / 'storage', work / 'arm', work / 'marker'
    target = storage / 'objects' / hashlib.sha256(f'{prefix}/tasks/pending/state.json'.encode()).hexdigest()
    env = dict(base, DEOOS_STORAGE_PROVIDER='filesystem', DEOOS_STORAGE_DIRECTORY=str(storage),
               EXECUTION_PREFIX=prefix, DYLD_INSERT_LIBRARIES=str(library),
               DEOOS_FAULT_TARGET=str(target), DEOOS_FAULT_ARM=str(arm),
               DEOOS_FAULT_MARKER=str(marker),
               DEOOS_FAULT_MODE='after-rename' if mode.startswith('closed-output') else mode)
    server = await anyio.to_thread.run_sync(Server, binary, env, work / 'engine.log')
    client_env = remote_env(env, server)
    try:
        await anyio.to_thread.run_sync(sdk, python, client_env,
                                      "c.submit('pending', 'pending.v1', {}); print(json.dumps(True))")
        if mode.startswith('closed-output'):
            return await anyio.to_thread.run_sync(closed_output, python, client_env, arm, marker,
                                                 server, mode == 'closed-output')
        async with connection(python, client_env, work, actions=True) as client:
            tools = await definitions(client, True)
            arm.write_text('armed')
            if mode == 'fail-sync':
                failure = await call(client, tools, 'task_cancel', {'id': 'pending'}, status=507)
                assert 'durability' in failure['error'] and 'uncertain' in failure['error']
                assert marker.read_text() == 'after-rename-directory-fsync-EIO'
            else:
                completed = anyio.Event()
                scope = anyio.CancelScope()

                async def blocked():
                    with scope:
                        await client.call_tool('task_cancel', {'id': 'pending'})
                        raise AssertionError('write was expected to be blocked at rename')
                    completed.set()

                async with anyio.create_task_group() as group:
                    group.start_soon(blocked)
                    assert await anyio.to_thread.run_sync(wait_marker, marker, server) == 'after-rename'
                    scope.cancel()
                    with anyio.fail_after(5):
                        await completed.wait()
                    # SDK 2.3 sends courtesy notifications/cancelled before its
                    # caller cancellation propagates. Ping's response proves the
                    # adapter stdin loop is still responsive while the real
                    # engine write is stopped. No rollback is assumed.
                    await client.send_ping()
                    os.kill(server.process.pid, signal.SIGCONT)
            history = await call(client, tools, 'task_history', {'id': 'pending'})
            assert history['status'] == 'cancelled', history
            recovered = await call(client, tools, 'task_retry', {'id': 'pending',
                                   'expected_revision': history['revision'], 'operation_id': 'recover'})
            assert recovered['status'] == 'queued'
        return {'fault': mode, 'persisted_after_error_or_cancellation': 'cancelled',
                'adapter_remained_usable': True, 'rolled_back': False}
    finally:
        # SIGCONT is harmless for a running server and keeps cleanup bounded.
        if server.process.poll() is None:
            os.kill(server.process.pid, signal.SIGCONT)
        await anyio.to_thread.run_sync(server.close)


def closed_output(python, env, arm, marker, server, leave_stdin_open):
    """A disconnected stdout and queued work must not strand EOF cleanup."""
    process = subprocess.Popen([python, '-m', 'deoos', 'mcp', '--allow-actions'], env=env,
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)

    def send(message):
        process.stdin.write(json.dumps(dict(jsonrpc='2.0', **message)) + '\n')
        process.stdin.flush()

    try:
        send(dict(id=1, method='initialize', params={'protocolVersion': '2025-11-25',
                  'capabilities': {}, 'clientInfo': {'name': 'closed-output-test', 'version': '1'}}))
        assert select.select([process.stdout], [], [], 30)[0], 'initialization timed out'
        assert json.loads(process.stdout.readline())['result']['protocolVersion'] == '2025-11-25'
        send(dict(method='notifications/initialized'))
        arm.write_text('armed')
        send(dict(id=2, method='tools/call', params={'name': 'task_cancel', 'arguments': {'id': 'pending'}}))
        assert wait_marker(marker, server) == 'after-rename'
        # Close the child's output receiver, never the test driver's stdout.
        # Queue pressure exercises disconnect during both dispatch and response;
        # counts deliberately exceed the adapter's documented bounded backlog.
        process.stdout.close()
        writes_completed = 0
        try:
            for number in range(32):
                send(dict(id=number + 3, method='tools/call', params={
                    'name': 'task_summary', 'arguments': {'id': 'pending'}}))
                writes_completed += 1
        except BrokenPipeError:
            pass  # The adapter may already have recognized the disconnect.
        if not leave_stdin_open:
            try:
                process.stdin.close()
            except BrokenPipeError:
                pass
        os.kill(server.process.pid, signal.SIGCONT)
        if leave_stdin_open:
            assert not process.stdin.closed
        exit_status = process.wait(timeout=15)
        # A deliberately broken stdout is a failed transport, not orderly EOF.
        # CPython may replace the adapter's exit1 with exit120 when its final
        # buffered stdout flush also sees EPIPE. Both must terminate and release
        # the subprocess; normal EOF is separately required to exit0 above.
        assert exit_status in (1, 120), (exit_status, process.stderr.read())
        observed = sdk(python, env, "print(json.dumps(c.inspect('pending')))")
        assert observed['status'] == 'cancelled', observed
        return {'fault': 'closed-output' if leave_stdin_open else 'closed-output-eof',
                'queue_pressure_pipe_writes_completed': writes_completed, 'exit': exit_status,
                'stdin_left_open_until_exit': leave_stdin_open,
                'persisted_after_disconnect': 'cancelled', 'rolled_back': False}
    finally:
        if server.process.poll() is None:
            os.kill(server.process.pid, signal.SIGCONT)
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)
        for stream in (process.stdin, process.stdout, process.stderr):
            try:
                stream.close()
            except BrokenPipeError:
                pass


async def run(args, report):
    assert importlib.metadata.version('mcp') == '2.3.0', 'install the pinned official test SDK'
    base = clean_env()
    s3, bucket_created = None, False
    with tempfile.TemporaryDirectory(prefix='deoos-mcp-behavior-') as temporary:
        work = pathlib.Path(temporary).resolve()
        env = dict(base)
        bucket = None
        if args.backend == 'filesystem':
            env.update(DEOOS_STORAGE_PROVIDER='filesystem', DEOOS_STORAGE_DIRECTORY=str(work / 'storage'))
        else:
            import boto3
            from botocore.exceptions import ClientError
            bucket = 'deoos-mcp-' + uuid.uuid4().hex
            endpoint = os.environ.get('AWS_ENDPOINT', 'http://127.0.0.1:19000')
            assert endpoint.startswith('http://127.0.0.1:') or endpoint.startswith('http://localhost:'), \
                'RustFS test endpoint must be local'
            env.update(AWS_ENDPOINT=endpoint, AWS_ALLOW_HTTP='true', AWS_REGION='us-east-1',
                       AWS_ACCESS_KEY_ID='local-development',
                       AWS_SECRET_ACCESS_KEY='local-development-only-secret', AWS_BUCKET=bucket)
            s3 = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                              aws_access_key_id=env['AWS_ACCESS_KEY_ID'],
                              aws_secret_access_key=env['AWS_SECRET_ACCESS_KEY'])
        report.update(bucket=bucket, storage_directory=env.get('DEOOS_STORAGE_DIRECTORY'))
        try:
            if s3 is not None:
                await anyio.to_thread.run_sync(lambda: s3.create_bucket(Bucket=bucket))
                bucket_created = True
            for mode in ('library', 'server'):
                cell = work / mode
                cell.mkdir()
                storage_env = dict(env, EXECUTION_PREFIX='mcp-' + uuid.uuid4().hex,
                                   ENGINE_TOKEN=uuid.uuid4().hex)
                server = None
                try:
                    if mode == 'server':
                        server = await anyio.to_thread.run_sync(Server, args.binary, storage_env, cell / 'engine.log')
                        client_env = remote_env(storage_env, server)
                    else:
                        client_env = storage_env
                    report['cells'].append(await behavior(args.python, client_env, cell, mode))
                finally:
                    if server is not None:
                        await anyio.to_thread.run_sync(server.close)
            if args.faults:
                assert args.backend == 'filesystem' and platform.system() == 'Darwin' \
                    and platform.machine() == 'arm64', '--faults requires macOS ARM64 filesystem'
                library = work / 'faults.dylib'
                subprocess.run(['clang', '-dynamiclib', '-Wall', '-Wextra', '-Werror', '-o',
                                str(library), str(ROOT / 'tests/local_faults.c')], check=True)
                for mode in ('fail-sync', 'after-rename', 'closed-output', 'closed-output-eof'):
                    report['faults'].append(await fault_case(args.binary, args.python, base,
                                           work / mode, library, mode))
        finally:
            if bucket_created:
                for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket):
                    objects = [{'Key': item['Key']} for item in page.get('Contents', [])]
                    if objects:
                        deleted = s3.delete_objects(Bucket=bucket, Delete={'Objects': objects})
                        assert not deleted.get('Errors'), deleted
                s3.delete_bucket(Bucket=bucket)
                try:
                    s3.head_bucket(Bucket=bucket)
                except ClientError as error:
                    assert str(error.response['Error']['Code']) in ('404', 'NoSuchBucket', 'NotFound'), error
                else:
                    raise AssertionError('owned MCP test bucket survived cleanup')
    assert not pathlib.Path(temporary).exists()
    report['cleaned'] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=pathlib.Path, required=True)
    parser.add_argument('--python', default=sys.executable, help='interpreter with the DEOOS wheel installed')
    parser.add_argument('--backend', choices=('filesystem', 'rustfs'), default='filesystem')
    parser.add_argument('--faults', action='store_true')
    parser.add_argument('--report', type=pathlib.Path,
                        default=ROOT.parent / 'outputs/evidence/mcp-behavior.json')
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    report = {'backend': args.backend, 'binary': str(args.binary),
              'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              'official_sdk': 'mcp==2.3.0', 'cells': [], 'faults': [], 'cleaned': False}
    try:
        anyio.run(run, args, report)
    except BaseException as error:
        report['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
