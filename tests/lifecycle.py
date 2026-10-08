# Additional real-process/storage behavioral suite; current acceptance is tests/two_modes.py.
"""Real-process lifecycle suite. Endpoint/bucket/credentials supplied by run_tests.py."""
import concurrent.futures as cf
import json, os, pathlib, subprocess, sys, tempfile, time, urllib.request, urllib.error, uuid
ROOT = pathlib.Path(__file__).resolve().parents[1]
ENGINE = pathlib.Path(os.environ.get('ENGINE_BINARY', str(ROOT / 'engine/target/debug/deoos-server'))).resolve()
class API:
    def __init__(self, port): self.url = f'http://127.0.0.1:{port}'
    def req(self, path, data=None):
        if data is not None:
            info=self.req('/info')
            assert info.get('protocol_version')==3, info
            if not isinstance(data,dict): raise TypeError('mutation data must be an object')
            data={**data,'protocol_version':3}
        req=urllib.request.Request(self.url+path,data=None if data is None else json.dumps(data).encode(),headers={'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=30) as r: return json.load(r)
    def submit(self,id,handler='raw',**extra): return self.req('/tasks',dict(id=id,handler=handler,inputs={},max_attempts=5,**extra))
    def claim(self,handler='raw'): return self.req('/claim',dict(worker=str(uuid.uuid4()),handlers=[handler]))['task']
    def mutate(self,t,action,value=None):
        if action.startswith('steps/'):
            name=action.split('/',1)[1]
            self.req(f'/tasks/{t["id"]}/definitions/{name}',dict(
                token=t['token'],operation_id=str(uuid.uuid4()),value={'kind':'step','revision':'1'}))
        return self.req(f'/tasks/{t["id"]}/{action}',dict(token=t['token'],operation_id=str(uuid.uuid4()),value=value))
    def get(self,id): return self.req('/tasks/'+id)
def expect_conflict(fn):
    try: fn()
    except urllib.error.HTTPError as e: assert e.code==409, e.code
    else: raise AssertionError('stale write accepted')
def wait(fn,timeout=30):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        try:
            value=fn()
            if value:return value
        except (OSError,urllib.error.URLError):pass
        time.sleep(.1)
    raise AssertionError('wait timed out')
def run():
    results=[]; procs=[]; proxy=None; external=None
    with tempfile.TemporaryDirectory() as scratch:
        def start(port):
            env=dict(os.environ,ENGINE_BIND=f'127.0.0.1:{port}',LEASE_MS='6000')
            log=open(pathlib.Path(scratch)/f'{port}.log','a')
            p=subprocess.Popen([str(ENGINE)],env=env,cwd=scratch,stdout=log,stderr=log);procs.append(p)
            def healthy():
                if p.poll() is not None: raise RuntimeError(pathlib.Path(log.name).read_text())
                return urllib.request.urlopen(f'http://127.0.0.1:{port}/health',timeout=1).status==200
            wait(healthy); return API(port)
        try:
            a,b=start(17331),start(17332)
            with cf.ThreadPoolExecutor(max_workers=12) as pool:
                states=list(pool.map(lambda _:a.submit('race'),range(24)))
            assert all(t['id']=='race' for t in states)
            with cf.ThreadPoolExecutor(max_workers=12) as pool:
                claims=list(pool.map(lambda n:(a if n%2 else b).claim(),range(24)))
            winner=[t for t in claims if t is not None];assert len(winner)==1,winner
            t=winner[0];a.mutate(t,'complete',{'ok':True})
            results.append('concurrent submission and cross-engine claim: one winner')

            a.submit('stale');old=a.claim();time.sleep(6.3)
            expect_conflict(lambda:a.mutate(old,'renew'))
            new=b.claim()
            assert new['id']=='stale' and new['token']!=old['token']
            for action in ['renew','steps/late','complete','fail']:
                expect_conflict(lambda action=action:a.mutate(old,action,{'stale':True}))
            b.mutate(new,'complete',{'new':True});results.append('stale owner rejected for heartbeat, checkpoint, completion and failure')

            a.submit('retry',retry_ms=500);t=a.claim();a.mutate(t,'steps/saved',{'persisted':True});a.mutate(t,'fail','transient')
            assert b.claim() is None
            time.sleep(.6);t=b.claim();assert t['attempts']==2 and 'saved' in t['steps'];b.mutate(t,'complete',{'ok':True})
            a.req('/tasks',dict(id='exhaust',handler='raw',inputs={},max_attempts=1));t=a.claim();a.mutate(t,'fail','permanent')
            assert a.get('exhaust')['status']=='failed' and b.claim() is None
            results.append('retry backoff, persisted checkpoint and terminal attempt budget')

            marker=pathlib.Path(scratch)/'effects';ready=pathlib.Path(scratch)/'ready'
            a.req('/tasks',dict(id='recovery',handler='example',inputs=dict(marker=str(marker),ready=str(ready),number=20,pause_seconds=120),max_attempts=5))
            env=dict(os.environ,ENGINE_URL=a.url,PYTHONPATH=str(ROOT/'clients/python'))
            p=subprocess.Popen([sys.executable,str(ROOT/'examples/python_worker.py')],env=env);procs.append(p)
            wait(ready.exists);assert 'first' in a.get('recovery')['steps']
            p.kill();p.wait();procs[0].kill();procs[0].wait()
            # Fresh engine processes have no local task database or checkpoint files.
            a=start(17331);time.sleep(6.3)
            env=dict(os.environ,ENGINE_URL=b.url)
            subprocess.run(['node',str(ROOT/'examples/typescript_worker.mjs')],env=env,check=True,timeout=30)
            state=a.get('recovery');assert state['status']=='completed' and state['output']=={'number':42},state
            assert marker.read_text()=='first-executed\n'
            results.append('killed Python worker and engine; TypeScript resumed from S3 and skipped committed step')
            a.submit('definition')
            expect_conflict(lambda:a.req('/tasks',dict(id='definition',handler='raw',inputs={'changed':True},max_attempts=5)))
            t=a.claim();a.mutate(t,'complete',None)
            results.append('task ID rejects changed immutable definition')
            for language,command in [('python',[sys.executable,str(ROOT/'examples/basic_python.py'),'basic-python']),('typescript',['node',str(ROOT/'examples/basic_typescript.mjs'),'basic-typescript'])]:
                completed=subprocess.run(command,env=dict(os.environ,ENGINE_URL=a.url,PYTHONPATH=str(ROOT/'clients/python')),capture_output=True,text=True,check=True,timeout=30)
                assert a.get('basic-'+language)['output']=={'words':4,'text':'durable tasks survive crashes'}
            results.append('standalone Python and TypeScript basic examples')

            # Verify actual SDK renewal while a handler exceeds the lease duration.
            long_marker=pathlib.Path(scratch)/'long-effects';long_ready=pathlib.Path(scratch)/'long-ready'
            a.req('/tasks',dict(id='heartbeat',handler='example',inputs=dict(marker=str(long_marker),ready=str(long_ready),number=20,pause_seconds=8),max_attempts=5))
            env=dict(os.environ,ENGINE_URL=a.url,PYTHONPATH=str(ROOT/'clients/python'))
            subprocess.run([sys.executable,str(ROOT/'examples/python_worker.py')],env=env,check=True,timeout=30)
            assert a.get('heartbeat')['status']=='completed' and a.get('heartbeat')['attempts']==1
            # The TypeScript worker renews during a long asynchronous callback too.
            a.req('/tasks',dict(id='ts-heartbeat',handler='ts-long',inputs={},max_attempts=3))
            script="import {Client} from './clients/typescript/dist/index.js';const c=new Client(process.env.ENGINE_URL);await c.runOnce({'ts-long':async(ctx)=>ctx.step('slow',async()=>{await new Promise(r=>setTimeout(r,8000));return 42;})});"
            subprocess.run(['node','--input-type=module','-e',script],cwd=ROOT,env=dict(os.environ,ENGINE_URL=b.url),check=True,timeout=30)
            assert a.get('ts-heartbeat')['status']=='completed' and a.get('ts-heartbeat')['attempts']==1
            results.append('Python and TypeScript SDK heartbeats keep long handlers owned beyond initial lease')

            from external_service import ExternalService
            external=ExternalService();effect_ready=pathlib.Path(scratch)/'effect-ready'
            a.req('/tasks',dict(id='effect',handler='effect',inputs=dict(url=external.url,ready=str(effect_ready)),max_attempts=5))
            p=subprocess.Popen([sys.executable,str(ROOT/'examples/idempotent_python.py')],env=dict(os.environ,ENGINE_URL=a.url,PYTHONPATH=str(ROOT/'clients/python')));procs.append(p)
            wait(effect_ready.exists);assert not a.get('effect')['steps'];p.kill();p.wait();time.sleep(6.3)
            subprocess.run(['node',str(ROOT/'examples/idempotent_typescript.mjs')],env=dict(os.environ,ENGINE_URL=b.url),check=True,timeout=30)
            assert external.requests==2 and external.effects==1
            assert a.get('effect')['status']=='completed'
            results.append('crash after external success before checkpoint: two requests, one effect with stable idempotency key')

            if os.environ.get('AWS_ENDPOINT') == 'http://127.0.0.1:19000':
                from fault_proxy import FaultProxy
                import boto3
                proxy=FaultProxy();proxy.start()
                os.environ['AWS_ENDPOINT']='http://127.0.0.1:19002'
                fault=start(17333)
                a.submit('uncertain',handler='fault');t=fault.claim('fault')
                proxy.arm('lost-state-response')
                fault.mutate(t,'complete',{'committed':True})
                assert proxy.triggered==1 and a.get('uncertain')['status']=='completed'
                results.append('storage accepted completion but response lost: exact revision reconciled')

                a.submit('orphan',handler='fault');t=fault.claim('fault')
                proxy.arm('pause-result-response')
                with cf.ThreadPoolExecutor(max_workers=1) as pool:
                    future=pool.submit(lambda:fault.mutate(t,'steps/uncommitted',{'result':True}))
                    assert proxy.ready.wait(15),'result upload fault did not trigger'
                    procs[-1].kill();procs[-1].wait();proxy.release.set()
                    try:future.result(timeout=15)
                    except (OSError,urllib.error.URLError):pass
                assert 'uncommitted' not in a.get('orphan')['steps']
                s3=boto3.client('s3',endpoint_url='http://127.0.0.1:19000',region_name='us-east-1')
                objects=s3.list_objects_v2(Bucket=os.environ['AWS_BUCKET'],Prefix=os.environ['EXECUTION_PREFIX']+'/tasks/orphan/results/')['Contents']
                assert len(objects)==1
                time.sleep(6.3);t=b.claim('fault');assert t['id']=='orphan'
                b.mutate(t,'steps/uncommitted',{'retried':True});b.mutate(t,'complete',{'ok':True})
                assert b.req('/tasks/orphan/steps/uncommitted')=={'retried':True}
                results.append('engine killed after immutable upload: orphan ignored; retry committed new result')
            return results
        finally:
            if proxy:proxy.close()
            if external:external.close()
            for p in procs:
                if p.poll() is None:p.kill();p.wait()
if __name__=='__main__':print(json.dumps(run(),indent=2))
