"""Acceptance matrix: both real SDK modes, both storage providers, same Rust core."""
import concurrent.futures as cf,hashlib,http.server,json,os,pathlib,platform,subprocess,sys,tempfile,threading,time,urllib.error,urllib.request,uuid
(pathlib.Path(__file__).resolve().parents[2]/'outputs/evidence').mkdir(parents=True,exist_ok=True)
import boto3
from botocore.exceptions import ClientError
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'clients/python'))
from deoos import Client,EngineError
backend=sys.argv[1]
report={'backend':backend,'modes':{},'cleaned':False}
bucket='deoos-engine-test-'+uuid.uuid4().hex[:20]
if backend=='aws':
 session=boto3.Session(profile_name=os.environ.get('AWS_PROFILE'),region_name='us-east-1');s3=session.client('s3');creds=session.get_credentials().get_frozen_credentials()
 os.environ.update(AWS_ACCESS_KEY_ID=creds.access_key,AWS_SECRET_ACCESS_KEY=creds.secret_key,AWS_REGION='us-east-1')
 if creds.token:os.environ['AWS_SESSION_TOKEN']=creds.token
 else:os.environ.pop('AWS_SESSION_TOKEN',None)
 os.environ.pop('AWS_ENDPOINT',None);os.environ.pop('AWS_ALLOW_HTTP',None)
else:
 os.environ.update(AWS_ACCESS_KEY_ID='local-development',AWS_SECRET_ACCESS_KEY='local-development-only-secret',AWS_REGION='us-east-1',AWS_ENDPOINT='http://127.0.0.1:19000',AWS_ALLOW_HTTP='true');os.environ.pop('AWS_SESSION_TOKEN',None)
 s3=boto3.client('s3',endpoint_url=os.environ['AWS_ENDPOINT'],region_name='us-east-1')
os.environ.update(AWS_BUCKET=bucket,LEASE_MS='6000')
server_binary=pathlib.Path(os.environ.get('ENGINE_BINARY',str(ROOT/'engine/target/debug'/('deoos-engine.exe' if os.name=='nt' else 'deoos-engine')))).resolve()
native_name={'Darwin':'libdeoos_engine.dylib','Linux':'libdeoos_engine.so','Windows':'deoos_engine.dll'}[platform.system()]
report['bucket']=bucket
report['artifacts']={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [server_binary,ROOT/'clients/python/deoos/native'/native_name,ROOT/'clients/typescript/dist/native/deoos_node.node',ROOT/'clients/python/deoos/__init__.py',ROOT/'clients/typescript/dist/index.js']}
created=False
try:
 s3.create_bucket(Bucket=bucket);created=True
 if backend=='aws':s3.put_public_access_block(Bucket=bucket,PublicAccessBlockConfiguration={k:True for k in ['BlockPublicAcls','IgnorePublicAcls','BlockPublicPolicy','RestrictPublicBuckets']})
 for mode in ['library','server']:
  passed=[];server=None;workers=[];clients=[]
  os.environ.update(EXECUTION_PREFIX='modes %/'+uuid.uuid4().hex,WORKER_MODE=mode,ENGINE_URL='http://127.0.0.1:17351',ENGINE_BIND='0.0.0.0:17351',ENGINE_TOKEN='acceptance-test-only-token')
  with tempfile.TemporaryDirectory() as scratch:
   def create():
    c=Client(bucket=bucket) if mode=='library' else Client.remote(os.environ['ENGINE_URL'],os.environ['ENGINE_TOKEN']);clients.append(c);return c
   try:
    if mode=='server':
     log=pathlib.Path(scratch)/'server.log'
     server=subprocess.Popen([str(server_binary)],env=os.environ.copy(),stdout=log.open('w'),stderr=subprocess.STDOUT)
     for _ in range(100):
      if server.poll() is not None:raise RuntimeError(log.read_text())
      try:
       if urllib.request.urlopen(os.environ['ENGINE_URL']+'/health',timeout=1).status==200:break
      except OSError:pass
      time.sleep(.1)
     else:raise AssertionError('server unavailable')
     try:Client.remote(os.environ['ENGINE_URL']).request('/info')
     except EngineError as e:assert e.status==401
     else:raise AssertionError('server accepted unauthenticated operation')
     passed.append('shared server accepts authenticated clients; rejects missing token')
    a,b=create(),create()
    def expect_status(status, fn):
     try: fn()
     except EngineError as error: assert error.status==status, error
     except urllib.error.HTTPError as error: assert error.code==status, error
     else: raise AssertionError(f'expected status {status}')
    if mode=='server':
     def browser_request(url,identifier,origin=None,content_type='application/json',token=None):
      headers={'Content-Type':content_type}
      if origin is not None:headers['Origin']=origin
      if token:headers['Authorization']='Bearer '+token
      request=urllib.request.Request(url+'/tasks',data=json.dumps({'id':identifier,'handler':'browser','inputs':{},'protocol_version':3}).encode(),headers=headers)
      with urllib.request.urlopen(request,timeout=10) as response:return json.load(response)
     for origin in ['http://foreign.invalid','null','invalid','http://127.0.0.1:17351/extra']:
      expect_status(403,lambda:browser_request(os.environ['ENGINE_URL'],'origin-rejected',origin,token=os.environ['ENGINE_TOKEN']))
     expect_status(415,lambda:browser_request(os.environ['ENGINE_URL'],'origin-rejected',content_type='text/plain',token=os.environ['ENGINE_TOKEN']))
     expect_status(404,lambda:a.inspect('origin-rejected'))
     browser_request(os.environ['ENGINE_URL'],'origin-same',os.environ['ENGINE_URL'],content_type='application/json; charset=utf-8',token=os.environ['ENGINE_TOKEN']);a.cancel('origin-same')
     with urllib.request.urlopen(os.environ['ENGINE_URL']+'/ui') as response:
      assert response.status==200 and b'DEOOS' in response.read() and "frame-ancestors 'none'" in response.headers['Content-Security-Policy']
     loop_env=dict(os.environ,ENGINE_BIND='127.0.0.1:17355');loop_env.pop('ENGINE_TOKEN',None)
     loopback=subprocess.Popen([str(server_binary)],env=loop_env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);workers.append(loopback)
     try:
      for _ in range(100):
       if loopback.poll() is not None:raise AssertionError('loopback server stopped')
       try:urllib.request.urlopen('http://127.0.0.1:17355/health',timeout=1);break
       except OSError:time.sleep(.1)
      else:raise AssertionError('loopback server unavailable')
      expect_status(403,lambda:browser_request('http://127.0.0.1:17355','loopback-rejected','http://foreign.invalid','text/plain'))
      expect_status(415,lambda:browser_request('http://127.0.0.1:17355','loopback-rejected',content_type='text/plain'))
      expect_status(404,lambda:a.inspect('loopback-rejected'))
      browser_request('http://127.0.0.1:17355','loopback-same','http://127.0.0.1:17355');a.cancel('loopback-same')
      plain=Client.remote('http://127.0.0.1:17355');plain.submit('loopback-sdk','browser',{});plain.close();a.cancel('loopback-sdk')
     finally:loopback.terminate();loopback.wait()
     passed.append('foreign/null/malformed browser origins and text/plain mutations rejected before writes; same-origin UI and originless SDK work even without loopback token')
    for invalid in ['.','..']:
     expect_status(400,lambda:a.submit(invalid,'raw',{}))
    expect_status(400,lambda:a.submit('overflow','raw',{},retry_ms=2**64-1))
    a.submit('numeric','numbers',{'nested':[{'number':5.0},{'number':-9007199254740991.0}]})
    a.submit('numeric','numbers',{'nested':[{'number':5},{'number':-9007199254740991}]})
    a.submit('large-integer','numbers',{'number':9007199254740993})
    expect_status(409,lambda:a.submit('large-integer','numbers',{'number':9007199254740992}))
    a.submit('unsafe-float','numbers',{'number':9007199254740992.0})
    expect_status(409,lambda:a.submit('unsafe-float','numbers',{'number':9007199254740992}))
    passed.append('safe integral float/integer replay matches; large integer definitions remain distinct')
    a.submit('names','names',{})
    t=a.request('/claim',dict(worker='names-test',handlers=['names']))['task']
    expect_status(400,lambda:a.request('/tasks/names/steps/..',dict(token=t['token'],operation_id='dot',value=True)))
    operation=dict(token=t['token'],operation_id='replay',value=None)
    a.request('/tasks/names/renew',operation)
    a.request('/tasks/names/renew',operation)
    expect_status(409,lambda:a.request('/tasks/names/complete',operation))
    a.request('/tasks/names/definitions/saved',dict(token=t['token'],operation_id='define-saved',value={'kind':'step','revision':'1'}))
    checkpoint=dict(token=t['token'],operation_id='state',value={'saved':True})
    a.request('/tasks/names/steps/saved',checkpoint)
    a.request('/tasks/names/steps/saved',checkpoint)
    expect_status(409,lambda:a.request('/tasks/names/steps/saved',dict(checkpoint,value={'changed':True})))
    a.request('/tasks/names/complete',dict(token=t['token'],operation_id='finish',value=True))
    assert a.request('/claim',dict(worker='after-result',handlers=['names']))['task'] is None
    passed.append('invalid dot names/overflow rejected; exact mutation replay; changed replay rejected; state-named result cannot poison discovery')
    info=a.request('/info')
    assert info['process_id']==(os.getpid() if mode=='library' else server.pid)
    child_env=dict(os.environ)
    if mode=='server':
     child_env={k:v for k,v in child_env.items() if not k.startswith('AWS_') and k not in ['EXECUTION_PREFIX','DEOOS_NATIVE_LIBRARY','DEOOS_NODE_LIBRARY']}
    script="import {Client} from './clients/typescript/dist/index.js';const c=process.env.WORKER_MODE==='library'?new Client({bucket:process.env.AWS_BUCKET,prefix:process.env.EXECUTION_PREFIX}):Client.remote(process.env.ENGINE_URL,process.env.ENGINE_TOKEN);if(process.env.WORKER_MODE==='library')globalThis.fetch=()=>{throw new Error('library used HTTP');};const i=await c.request('/info');if(process.env.WORKER_MODE==='library'&&i.process_id!==process.pid)throw new Error('engine not embedded');console.log(i.process_id);"
    subprocess.run(['node','--input-type=module','-e',script],cwd=ROOT,env=child_env,check=True,capture_output=True,text=True)
    passed.append('Python and Node engine process identity proves selected mode')
    script="import {Client} from './clients/typescript/dist/index.js';const c=process.env.WORKER_MODE==='library'?new Client({bucket:process.env.AWS_BUCKET,prefix:process.env.EXECUTION_PREFIX}):Client.remote(process.env.ENGINE_URL,process.env.ENGINE_TOKEN);await c.submit('prototype-steps','prototype-steps',{});await c.runOnce({'prototype-steps':async ctx=>{const a=await ctx.step('constructor',()=>21);const b=await ctx.step('toString',()=>21);return a+b;}});if((await c.request('/tasks/prototype-steps')).output!==42)throw new Error('prototype step names failed');"
    subprocess.run(['node','--input-type=module','-e',script],cwd=ROOT,env=child_env,check=True,capture_output=True,text=True)
    passed.append('TypeScript checkpoints accept constructor and toString names')
    racers=[create() for _ in range(8)]
    with cf.ThreadPoolExecutor(max_workers=8) as pool:list(pool.map(lambda c:c.submit('race','raw',{}),racers))
    def claim(c):return c.request('/claim',dict(worker=uuid.uuid4().hex,handlers=['raw']))['task']
    with cf.ThreadPoolExecutor(max_workers=8) as pool:claims=list(pool.map(claim,racers))
    winners=[t for t in claims if t];assert len(winners)==1
    def mutate(c,t,action,value=None):
     if action.startswith('steps/'):
      c.request('/tasks/'+t['id']+'/definitions/'+action[6:],dict(token=t['token'],operation_id=uuid.uuid4().hex,value={'kind':'step','revision':'1'}))
     return c.request('/tasks/'+t['id']+'/'+action,dict(token=t['token'],operation_id=uuid.uuid4().hex,value=value))
    mutate(a,winners[0],'complete',True);passed.append('concurrent submit and claim: one winner')
    a.submit('stale','raw',{});old=claim(a);time.sleep(6.3);new=claim(b);assert new['generation']==old['generation']+1
    for action in ['renew','steps/late','complete','fail']:
     try:mutate(a,old,action)
     except EngineError as e:assert e.status==409
     except urllib.error.HTTPError as e:assert e.code==409
     else:raise AssertionError('stale write accepted')
    mutate(b,new,'complete',True);passed.append('stale owner rejected for every mutation')
    a.submit('000-retry','raw',{},retry_ms=3000);t=claim(a);mutate(a,t,'steps/saved',42);mutate(a,t,'fail','transient');assert claim(b) is None;time.sleep(3.1);t=claim(b);assert 'saved' in t['steps'];mutate(b,t,'complete',True);passed.append('retry backoff and persisted checkpoint')
    trace=pathlib.Path(scratch)/'trace';ready=pathlib.Path(scratch)/'ready'
    a.submit('recovery','work',dict(trace=str(trace),ready=str(ready),number=20,pause=120),max_attempts=5)
    env=dict(child_env,PYTHONPATH=str(ROOT/'clients/python'))
    p=subprocess.Popen([sys.executable,str(ROOT/'examples/mode_worker.py')],env=env);workers.append(p)
    for _ in range(200):
     if p.poll() is not None:raise AssertionError('worker stopped before checkpoint')
     if ready.exists():break
     time.sleep(.1)
    else:raise AssertionError('checkpoint not committed')
    p.kill();p.wait();assert 'first' in a.request('/tasks/recovery')['steps']
    for c in clients:c.close()
    clients.clear()
    if server:
     server.kill();server.wait();server=subprocess.Popen([str(server_binary)],env=os.environ.copy(),stdout=log.open('a'),stderr=subprocess.STDOUT)
    time.sleep(6.3)
    subprocess.run(['node',str(ROOT/'examples/mode_worker.mjs')],env=env,check=True,timeout=30)
    c=create();a=c;state=c.request('/tasks/recovery');assert state['status']=='completed' and state['output']=={'number':42};assert trace.read_text()=='first\n'
    passed.append('killed Python worker; fresh Node worker resumed committed checkpoint from S3')
    if mode=='server':passed.append('shared server killed and reconstructed solely from object-store state')
    # Real parent/child workers: crash after first child creation, resume in the other SDK.
    workflow_trace=pathlib.Path(scratch)/'workflow-trace';workflow_ready=pathlib.Path(scratch)/'workflow-ready'
    a.submit('pipeline','pipeline',{'trace':str(workflow_trace),'ready':str(workflow_ready)},max_attempts=2)
    python_parent="""import os,pathlib,time;from deoos import Client
c=Client(bucket=os.environ['AWS_BUCKET']) if os.environ['WORKER_MODE']=='library' else Client.remote(os.environ['ENGINE_URL'],os.environ['ENGINE_TOKEN'])
original_submit=c.submit
def paused_submit(task_id,*args,**kwargs):
 result=original_submit(task_id,*args,**kwargs)
 if task_id.startswith('child-'):
  pathlib.Path(os.environ['READY']).touch()
  time.sleep(120)
 return result
c.submit=paused_submit
def parent(ctx,inputs):
 return ctx.spawn('left','double',{'number':5.0,'trace':inputs['trace']})
c.run_once({'pipeline':parent})
"""
    env=dict(child_env,PYTHONPATH=str(ROOT/'clients/python'))
    parent_process=subprocess.Popen([sys.executable,'-c',python_parent],env=dict(env,READY=str(workflow_ready)));workers.append(parent_process)
    for _ in range(200):
     if parent_process.poll() is not None:raise AssertionError('parent exited before child checkpoint')
     if workflow_ready.exists():break
     time.sleep(.1)
    else:raise AssertionError('parent did not spawn first child')
    parent_process.kill();parent_process.wait()
    assert 'left' not in a.request('/tasks/pipeline')['steps'],'crash must precede parent checkpoint'
    time.sleep(6.3)
    node_header="import {Client,ChildFailed} from './clients/typescript/dist/index.js';import{appendFileSync}from'node:fs';const c=process.env.WORKER_MODE==='library'?new Client({bucket:process.env.AWS_BUCKET,prefix:process.env.EXECUTION_PREFIX}):Client.remote(process.env.ENGINE_URL,process.env.ENGINE_TOKEN);"
    node_parent=node_header+"await c.runOnce({pipeline:async(ctx,inputs)=>{const left=await ctx.spawn('left','double',{number:5,trace:inputs.trace});const right=await ctx.spawn('right','double',{number:7,trace:inputs.trace});const values=await ctx.join('parts',[left,right]);return values.reduce((a,b)=>a+b,0);}});"
    subprocess.run(['node','--input-type=module','-e',node_parent],cwd=ROOT,env=env,check=True,capture_output=True,text=True)
    parent=a.request('/tasks/pipeline');assert parent['status']=='waiting' and parent['attempts']==2 and parent['expires_at']==0
    assert a.request('/claim',dict(worker='pending-join',handlers=['pipeline']))['task'] is None
    node_child=node_header+"await c.runOnce({double:async(ctx,inputs)=>ctx.step('double',async()=>{await new Promise(r=>setTimeout(r,200));appendFileSync(inputs.trace,inputs.number+'\\n');return inputs.number*2;})});"
    children=[subprocess.Popen(['node','--input-type=module','-e',node_child],cwd=ROOT,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) for _ in range(2)]
    workers.extend(children)
    for process in children:
     stdout,stderr=process.communicate(timeout=30);assert process.returncode==0,stderr
    python_resume="""import os;from deoos import Client
c=Client(bucket=os.environ['AWS_BUCKET']) if os.environ['WORKER_MODE']=='library' else Client.remote(os.environ['ENGINE_URL'],os.environ['ENGINE_TOKEN'])
def parent(ctx,inputs):
 left=ctx.spawn('left','double',{'number':5,'trace':inputs['trace']})
 right=ctx.spawn('right','double',{'number':7,'trace':inputs['trace']})
 return sum(ctx.join('parts',[left,right]))
assert c.run_once({'pipeline':parent})
c.close()
"""
    subprocess.run([sys.executable,'-c',python_resume],env=env,check=True,capture_output=True,text=True)
    parent=a.request('/tasks/pipeline');assert parent['status']=='completed' and parent['output']==24 and parent['attempts']==2
    assert sorted(workflow_trace.read_text().splitlines())==['5','7']
    for step in ['left','right']:
     child_id=a.request('/tasks/pipeline/steps/'+step)
     child=a.request('/tasks/'+child_id);assert child['status']=='completed' and child['attempts']==1
    passed.append('cross-language parent crash recovery; concurrent children; durable join releases worker; resume preserves retry budget')
    # Child errors may be handled by the application or terminate the parent without retrying it.
    for caught in [False,True]:
     identifier='caught' if caught else 'uncaught'
     a.submit(identifier,identifier,{},max_attempts=3)
     script=node_header+f"await c.runOnce({{'{identifier}':async ctx=>{{const id=await ctx.spawn('child','will-fail',{{}},1);return ctx.join('child-result',[id]);}}}});"
     subprocess.run(['node','--input-type=module','-e',script],cwd=ROOT,env=env,check=True,capture_output=True,text=True)
     failer="""import os;from deoos import Client,ChildFailed
c=Client(bucket=os.environ['AWS_BUCKET']) if os.environ['WORKER_MODE']=='library' else Client.remote(os.environ['ENGINE_URL'],os.environ['ENGINE_TOKEN'])
def fail(ctx,inputs):raise ValueError('deliberate child failure')
try:c.run_once({'will-fail':fail})
except ValueError:pass
c.close()
"""
     subprocess.run([sys.executable,'-c',failer],env=env,check=True,capture_output=True,text=True)
     script=node_header+f"try{{await c.runOnce({{'{identifier}':async ctx=>{{const id=await ctx.spawn('child','will-fail',{{}},1);try{{return await ctx.join('child-result',[id]);}}catch(e){{if({str(caught).lower()}&&e instanceof ChildFailed)return 'compensated';throw e;}}}}}});}}catch(e){{if(!(e instanceof ChildFailed))throw e;}}"
     python_caught="""import os;from deoos import Client,ChildFailed
c=Client(bucket=os.environ['AWS_BUCKET']) if os.environ['WORKER_MODE']=='library' else Client.remote(os.environ['ENGINE_URL'],os.environ['ENGINE_TOKEN'])
def parent(ctx,inputs):
 child=ctx.spawn('child','will-fail',{},1)
 try:return ctx.join('child-result',[child])
 except ChildFailed:return 'compensated'
assert c.run_once({'caught':parent})
c.close()
"""
     if caught:subprocess.run([sys.executable,'-c',python_caught],env=env,check=True,capture_output=True,text=True)
     else:subprocess.run(['node','--input-type=module','-e',script],cwd=ROOT,env=env,check=True,capture_output=True,text=True)
     state=a.request('/tasks/'+identifier);assert state['attempts']==1
     assert state['status']==('completed' if caught else 'failed')
     if caught:assert state['output']=='compensated'
    passed.append('child failure propagates terminally or is caught and compensated')
    for language,wrap in [('python',False),('python',True),('typescript',False),('typescript',True)]:
     task_id='broad-catch-'+language+('-wrap' if wrap else '-return')
     a.submit(task_id,task_id,{'trace':str(workflow_trace)},max_attempts=1)
     python_script="""import os;from deoos import Client
c=Client(bucket=os.environ['AWS_BUCKET']) if os.environ['WORKER_MODE']=='library' else Client.remote(os.environ['ENGINE_URL'],os.environ['ENGINE_TOKEN'])
def parent(ctx,inputs):
 child=ctx.spawn('child','double',{'number':11,'trace':inputs['trace']})
 try:return ctx.join('joined',[child])
 except BaseException as error:
  if os.environ['WRAP']=='true':raise RuntimeError('wrapped suspension') from error
  return 'incorrect fallback'
assert c.run_once({os.environ['PARENT_ID']:parent})
c.close()
"""
     node_script=node_header+"await c.runOnce({[process.env.PARENT_ID]:async(ctx,inputs)=>{const id=await ctx.spawn('child','double',{number:11,trace:inputs.trace});try{return await ctx.join('joined',[id]);}catch(e){if(process.env.WRAP==='true')throw new Error('wrapped suspension',{cause:e});return 'incorrect fallback';}}});"
     parent_env=dict(env,PARENT_ID=task_id,WRAP=str(wrap).lower())
     command=[sys.executable,'-c',python_script] if language=='python' else ['node','--input-type=module','-e',node_script]
     subprocess.run(command,cwd=ROOT,env=parent_env,check=True,capture_output=True,text=True)
     state=a.request('/tasks/'+task_id);assert state['status']=='waiting' and state['attempts']==1
     subprocess.run(['node','--input-type=module','-e',node_child],cwd=ROOT,env=env,check=True,capture_output=True,text=True)
     subprocess.run(command,cwd=ROOT,env=parent_env,check=True,capture_output=True,text=True)
     state=a.request('/tasks/'+task_id);assert state['status']=='completed' and state['output']==[22] and state['attempts']==1
    passed.append('broad application catch cannot accidentally complete or fail a suspended parent')
    # Timers and signals release the process; the next language resumes from object storage.
    wait_ready=pathlib.Path(scratch)/'wait-ready'
    a.submit('approval','approval',{},max_attempts=1)
    python_wait="""import os,pathlib,time;from deoos import Client
c=Client(bucket=os.environ['AWS_BUCKET']) if os.environ['WORKER_MODE']=='library' else Client.remote(os.environ['ENGINE_URL'],os.environ['ENGINE_TOKEN'])
original_request=c.request
def paused_request(path,data=None):
 result=original_request(path,data)
 if path.endswith('/suspend') and result['status']=='waiting':
  pathlib.Path(os.environ['READY']).touch()
  time.sleep(120)
 return result
c.request=paused_request
def work(ctx,inputs):
 ctx.sleep('cooldown',3000)
 return ctx.wait_signal('approval')
c.run_once({'approval':work})
"""
    process=subprocess.Popen([sys.executable,'-c',python_wait],env=dict(env,READY=str(wait_ready)));workers.append(process)
    for _ in range(200):
     if wait_ready.exists():break
     if process.poll() is not None:raise AssertionError('timer worker exited before suspension')
     time.sleep(.1)
    else:raise AssertionError('timer never persisted')
    process.kill();process.wait()
    state=a.request('/tasks/approval');deadline=state['timers']['cooldown']['deadline']
    assert state['status']=='waiting' and state['attempts']==1 and 'cooldown' not in state['steps']
    time.sleep(max(0,(deadline-time.time()*1000)/1000)+.1)
    node_wait=node_header+"await c.runOnce({approval:async ctx=>{await ctx.sleep('cooldown',3000);return ctx.waitSignal('approval');}});"
    subprocess.run(['node','--input-type=module','-e',node_wait],cwd=ROOT,env=env,check=True,capture_output=True,text=True)
    state=a.request('/tasks/approval');assert state['status']=='waiting' and state['waiting_on']['kind']=='signal' and state['timers']['cooldown']['deadline']==deadline and state['attempts']==1
    assert 'cooldown' in state['steps'] and a.request('/tasks/approval/steps/cooldown') is None
    a.signal('approval','approval',{'approved':True,'number':5.0})
    a.signal('approval','approval',{'approved':True,'number':5})
    expect_status(409,lambda:a.signal('approval','approval',{'approved':False}))
    python_finish="""import os;from deoos import Client
c=Client(bucket=os.environ['AWS_BUCKET']) if os.environ['WORKER_MODE']=='library' else Client.remote(os.environ['ENGINE_URL'],os.environ['ENGINE_TOKEN'])
def work(ctx,inputs):
 ctx.sleep('cooldown',3000)
 return ctx.wait_signal('approval')
assert c.run_once({'approval':work})
c.close()
"""
    subprocess.run([sys.executable,'-c',python_finish],env=env,check=True,capture_output=True,text=True)
    state=a.request('/tasks/approval');assert state['status']=='completed' and state['output']=={'approved':True,'number':5} and state['attempts']==1
    a.signal('approval','approval',{'approved':True,'number':5}) # Lost external response can be retried after completion.
    expect_status(409,lambda:a.signal('approval','new',True))
    passed.append('killed timer worker; cross-language timer and signal resume without spending attempts; signal assignment is immutable')
    a.submit('timer-definition','timer-definition',{})
    task=a.request('/claim',{'worker':'timer-definition','handlers':['timer-definition']})['task']
    assert mutate(a,task,'suspend',{'timer':{'name':'instant','milliseconds':0}})['status']=='running'
    expect_status(409,lambda:mutate(a,task,'suspend',{'timer':{'name':'instant','milliseconds':1}}))
    expect_status(400,lambda:mutate(a,task,'suspend',{'timer':{'name':'overflow','milliseconds':2**64-1}}))
    mutate(a,task,'complete',True)
    # Signal before/during suspension shares the same CAS object, so no wakeup can disappear.
    signal_client=create()
    for index in range(4):
     task_id='signal-race-'+str(index);a.submit(task_id,'signal-race',{},max_attempts=1)
     task=a.request('/claim',{'worker':'signal-racer','handlers':['signal-race']})['task']
     with cf.ThreadPoolExecutor(max_workers=2) as pool:
      futures=[pool.submit(mutate,a,task,'suspend',{'signal':'go'}),pool.submit(signal_client.signal,task_id,'go',index)]
      for future in futures:future.result()
     state=a.request('/tasks/'+task_id)
     if state['status']=='waiting':task=a.request('/claim',{'worker':'resume','handlers':['signal-race']})['task']
     assert task['id']==task_id and a.request('/tasks/'+task_id+'/signals/go')==index
     mutate(a,task,'complete',index)
     assert a.request('/tasks/'+task_id)['attempts']==1
    passed.append('signals raced against suspension never lose wakeup or consume retry budget')
    a.submit('join-ready-child','join-ready-child',{})
    child=a.request('/claim',{'worker':'child','handlers':['join-ready-child']})['task'];mutate(a,child,'complete',42)
    a.submit('join-ready-parent','join-ready-parent',{},max_attempts=1)
    parent=a.request('/claim',{'worker':'parent','handlers':['join-ready-parent']})['task']
    assert mutate(a,parent,'suspend',{'children':['join-ready-child']})['status']=='waiting'
    parent=a.request('/claim',{'worker':'resume-parent','handlers':['join-ready-parent']})['task']
    assert parent['id']=='join-ready-parent' and parent['attempts']==1
    mutate(a,parent,'complete',42)
    passed.append('children finishing before suspension still release and resume parent without another attempt')
    # Cancel all nonterminal states, then prove both real SDKs stop before another callback/child.
    for status in ['queued','running','waiting']:
     task_id='cancel-'+status;a.submit(task_id,task_id,{})
     if status!='queued':
      task=a.request('/claim',{'worker':'cancel-test','handlers':[task_id]})['task']
      if status=='waiting':mutate(a,task,'suspend',{'signal':'go'})
     assert a.cancel(task_id)['status']=='cancelled' and a.cancel(task_id)['status']=='cancelled'
     expect_status(409,lambda:a.signal(task_id,'go',True))
     if status!='queued':
      for action in ['renew','steps/late','complete','fail','suspend']:expect_status(409,lambda:mutate(a,task,action,{'signal':'go'} if action=='suspend' else None))
     assert a.request('/claim',{'worker':'after-cancel','handlers':[task_id]})['task'] is None
    expect_status(409,lambda:a.cancel('approval'))
    for language in ['python','typescript']:
     task_id='cancel-boundary-'+language;ready_file=pathlib.Path(scratch)/(task_id+'-ready');continue_file=pathlib.Path(scratch)/(task_id+'-continue');effect_file=pathlib.Path(scratch)/(task_id+'-effect')
     a.submit(task_id,task_id,{})
     python_cancel="""import os,pathlib,time;from deoos import Client
c=Client(bucket=os.environ['AWS_BUCKET']) if os.environ['WORKER_MODE']=='library' else Client.remote(os.environ['ENGINE_URL'],os.environ['ENGINE_TOKEN'])
def work(ctx,inputs):
 pathlib.Path(os.environ['READY']).touch()
 while not pathlib.Path(os.environ['CONTINUE']).exists():time.sleep(.05)
 try:ctx.step('late',lambda:pathlib.Path(os.environ['EFFECT']).touch())
 except Exception:pass
 try:ctx.spawn('late-child','noop',{})
 except Exception:pass
 return True
try:c.run_once({os.environ['PARENT_ID']:work})
except Exception:pass
c.close()
"""
     node_cancel=node_header+"import{existsSync,writeFileSync}from'node:fs';try{await c.runOnce({[process.env.PARENT_ID]:async ctx=>{writeFileSync(process.env.READY,'');while(!existsSync(process.env.CONTINUE))await new Promise(r=>setTimeout(r,50));try{await ctx.step('late',()=>writeFileSync(process.env.EFFECT,''));}catch{}try{await ctx.spawn('late-child','noop',{});}catch{}return true;}});}catch{}"
     command=[sys.executable,'-c',python_cancel] if language=='python' else ['node','--input-type=module','-e',node_cancel]
     process=subprocess.Popen(command,cwd=ROOT,env=dict(env,PARENT_ID=task_id,READY=str(ready_file),CONTINUE=str(continue_file),EFFECT=str(effect_file)),stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);workers.append(process)
     for _ in range(200):
      if ready_file.exists():break
      if process.poll() is not None:raise AssertionError('cancel worker stopped early')
      time.sleep(.1)
     else:raise AssertionError('cancel worker never ran')
     a.cancel(task_id);continue_file.touch();stdout,stderr=process.communicate(timeout=30);assert process.returncode==0,stderr
     assert not effect_file.exists() and a.request('/tasks/'+task_id)['status']=='cancelled'
     child_id='child-'+hashlib.sha256((task_id+'/late-child').encode()).hexdigest()[:32]
     expect_status(404,lambda:a.request('/tasks/'+child_id))
    passed.append('cancellation fences queued/running/waiting tasks and prevents both SDKs starting subsequent effects or children')
    # Deploy a new handler alongside a waiting old execution; each keeps its declared result shape.
    a.submit('code-v1','code.v1',{})
    version_trace=pathlib.Path(scratch)/'version-trace'
    version_env=dict(env,TRACE=str(version_trace))
    script=node_header+"await c.runOnce({'code.v1':async ctx=>{const result=await ctx.step('schema',()=>{appendFileSync(process.env.TRACE,'v1\\n');return {version:1};},'1');await ctx.sleep('cooldown',0);await ctx.waitSignal('continue');return result;}});"
    subprocess.run(['node','--input-type=module','-e',script],cwd=ROOT,env=version_env,check=True,capture_output=True,text=True)
    old=a.request('/tasks/code-v1');assert old['status']=='waiting'
    a.submit('code-v2','code.v2',{})
    script=node_header+"await c.runOnce({'code.v2':async ctx=>ctx.step('schema',()=>{appendFileSync(process.env.TRACE,'v2\\n');return ['version',2];},'2')});if(await c.runOnce({'code.v2':()=>{throw new Error('claimed v1');}}))throw new Error('v2 worker claimed incompatible task');"
    subprocess.run(['node','--input-type=module','-e',script],cwd=ROOT,env=version_env,check=True,capture_output=True,text=True)
    expect_status(409,lambda:a.submit('code-v1','code.v2',{}))
    a.signal('code-v1','continue',True)
    version_resume="""import os;from deoos import Client
c=Client(bucket=os.environ['AWS_BUCKET']) if os.environ['WORKER_MODE']=='library' else Client.remote(os.environ['ENGINE_URL'],os.environ['ENGINE_TOKEN'])
def work(ctx,inputs):
 result=ctx.step('schema',lambda:(_ for _ in ()).throw(AssertionError('replayed callback')),revision='1')
 ctx.sleep('cooldown',0)
 ctx.wait_signal('continue')
 return result
assert c.run_once({'code.v1':work})
c.close()
"""
    subprocess.run([sys.executable,'-c',version_resume],env=version_env,check=True,capture_output=True,text=True)
    old=a.request('/tasks/code-v1');assert old['status']=='completed' and old['output']=={'version':1} and old['attempts']==1
    assert a.request('/tasks/code-v2')['output']==['version',2] and version_trace.read_text()=='v1\nv2\n'
    passed.append('v2 handlers cannot claim active v1 executions; cross-language v1 replay preserves committed shape')
    # Definition checks happen before cached values, callbacks or child creation, even after completion of a step.
    a.submit('contracts','contracts',{},max_attempts=1)
    script=node_header+"await c.runOnce({contracts:async ctx=>{await ctx.step('plain',()=>42,'1');await ctx.sleep('slept',0);await ctx.join('joined',[]);await ctx.spawn('spawned','unused-child',{number:5});return ctx.waitSignal('continue');}});"
    subprocess.run(['node','--input-type=module','-e',script],cwd=ROOT,env=env,check=True,capture_output=True,text=True)
    a.signal('contracts','continue',True)
    contract_resume="""import os;from deoos import Client,EngineError
c=Client(bucket=os.environ['AWS_BUCKET']) if os.environ['WORKER_MODE']=='library' else Client.remote(os.environ['ENGINE_URL'],os.environ['ENGINE_TOKEN'])
def work(ctx,inputs):
 def forbidden():raise AssertionError('incompatible callback executed')
 changes=[lambda:ctx.step('plain',forbidden,revision='2'),lambda:ctx.sleep('plain',0),lambda:ctx.wait_signal('slept'),lambda:ctx.sleep('slept',1),lambda:ctx.join('joined',['does-not-exist']),lambda:ctx.spawn('plain','should-not-exist',{}),lambda:ctx.spawn('spawned','unused-child',{'number':6})]
 for change in changes:
  try:change()
  except EngineError as error:assert error.status==409
  else:raise AssertionError('changed definition accepted')
 assert ctx.step('plain',forbidden,revision='1')==42
 ctx.sleep('slept',0)
 assert ctx.join('joined',[])==[]
 ctx.spawn('spawned','unused-child',{'number':5.0})
 return ctx.wait_signal('continue')
assert c.run_once({'contracts':work})
c.close()
"""
    subprocess.run([sys.executable,'-c',contract_resume],env=env,check=True,capture_output=True,text=True)
    assert a.request('/tasks/contracts')['status']=='completed'
    child_id='child-'+hashlib.sha256(b'contracts/plain').hexdigest()[:32];expect_status(404,lambda:a.request('/tasks/'+child_id))
    a.submit('missing-contract','missing-contract',{})
    task=a.request('/claim',{'worker':'missing','handlers':['missing-contract']})['task']
    expect_status(409,lambda:a.request('/tasks/missing-contract/steps/unknown',{'token':task['token'],'operation_id':'no-contract','value':True}))
    mutate(a,task,'complete',True)
    passed.append('checkpoint kind/revision and built-in definitions are checked before cache, callback and child effects')
    # Previous protocol peers must receive no mutation from either SDK.
    legacy_posts=[];legacy_version=1
    class LegacyPeer(http.server.BaseHTTPRequestHandler):
     def log_message(self,*args):pass
     def do_GET(self):
      body=json.dumps({'protocol_version':legacy_version}).encode();self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
     def do_POST(self):
      legacy_posts.append(self.path);body=b'{"task":null}';self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    legacy_server=http.server.ThreadingHTTPServer(('127.0.0.1',0),LegacyPeer);legacy_thread=threading.Thread(target=legacy_server.serve_forever,daemon=True);legacy_thread.start()
    try:
     for legacy_version in [1,2]:
      legacy_url='http://127.0.0.1:'+str(legacy_server.server_port);legacy_client=Client.remote(legacy_url)
      for operation in [lambda:legacy_client.run_once({'legacy':lambda ctx,x:True}),lambda:legacy_client.submit('legacy','legacy',{}),lambda:legacy_client.signal('legacy','go',True),lambda:legacy_client.cancel('legacy')]:expect_status(409,operation)
      script="import{Client,EngineError}from'./clients/typescript/dist/index.js';const c=Client.remote(process.env.LEGACY_URL);for(const op of [()=>c.runOnce({legacy:()=>true}),()=>c.submit('legacy','legacy',{}),()=>c.signal('legacy','go',true),()=>c.cancel('legacy')]){try{await op();throw new Error('legacy mutation accepted');}catch(e){if(!(e instanceof EngineError)||e.status!==409)throw e;}}"
      subprocess.run(['node','--input-type=module','-e',script],cwd=ROOT,env=dict(env,LEGACY_URL=legacy_url),check=True,capture_output=True,text=True)
      assert not legacy_posts,'protocol negotiation reached a mutating legacy endpoint'
    finally:legacy_server.shutdown();legacy_server.server_close();legacy_thread.join()
    passed.append('both SDKs reject legacy server before any mutating request')
    a.submit('old-client','old-client',{})
    def old_request(path,data):
     if mode=='library':return a.native.request('POST',path,data)
     request=urllib.request.Request(os.environ['ENGINE_URL']+path,data=json.dumps(data).encode(),headers={'Content-Type':'application/json','Authorization':'Bearer '+os.environ['ENGINE_TOKEN']})
     with urllib.request.urlopen(request,timeout=10) as response:return json.load(response)
    for previous in [None,1,2]:
     marker={} if previous is None else {'protocol_version':previous}
     expect_status(409,lambda:old_request('/claim',dict(marker,**{'worker':'old-client','handlers':['old-client']})))
     expect_status(409,lambda:old_request('/tasks',dict(marker,**{'id':'old-created','handler':'old','inputs':{}})))
     expect_status(409,lambda:old_request('/tasks/old-client/cancel',marker))
     expect_status(409,lambda:old_request('/tasks/old-client/signals/go',dict(marker,**{'operation_id':'old','value':True})))
    assert a.request('/tasks/old-client')['attempts']==0 and a.request('/tasks/old-client')['status']=='queued'
    expect_status(404,lambda:a.request('/tasks/old-created'))
    a.cancel('old-client')
    passed.append('new native/shared engines reject legacy mutating clients before creating tasks or consuming attempts')
    # Operator retry uses observed revision and retains completed work.
    a.submit('ops-retry','ops-retry.v1',{},max_attempts=1)
    script="""import os;from deoos import Client
c=Client(bucket=os.environ['AWS_BUCKET']) if os.environ['WORKER_MODE']=='library' else Client.remote(os.environ['ENGINE_URL'],os.environ['ENGINE_TOKEN'])
def work(ctx,inputs):
 value=ctx.step('saved',lambda:42)
 try:ctx.log('😃'*1025)
 except ValueError:pass
 else:raise AssertionError('oversized log accepted')
 ctx.log('python worker failed after checkpoint')
 raise RuntimeError('expected failure')
try:c.run_once({'ops-retry.v1':work})
except RuntimeError as e:assert str(e)=='expected failure'
else:raise AssertionError('worker did not fail')
"""
    subprocess.run([sys.executable,'-c',script],cwd=ROOT,env=env,check=True,capture_output=True,text=True)
    failed=a.inspect('ops-retry');assert failed['status']=='failed' and failed['attempts']==1
    assert any(event['event']=='log' and event.get('detail')=='python worker failed after checkpoint' for event in failed['history'])
    retry_clients=[create(),create()]
    def try_retry(pair):
     index,client=pair
     try:return index,client.retry('ops-retry',failed['revision'],'operator-'+str(index))
     except EngineError as error:assert error.status==409;return index,None
    with cf.ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(try_retry,enumerate(retry_clients)))
    winner,state=next((index,state) for index,state in results if state is not None)
    assert sum(state is not None for _,state in results)==1 and state['status']=='queued' and state['attempts']==0
    assert state['steps']==failed['steps'] and state['definitions']==failed['definitions'] and state['generation']==failed['generation']
    expect_status(409,lambda:a.request('/tasks/ops-retry/log',{'token':failed['token'],'operation_id':'stale-log','value':'must not appear'}))
    script=node_header+"await c.runOnce({'ops-retry.v1':async ctx=>{const value=await ctx.step('saved',()=>{throw new Error('checkpoint callback repeated');});await ctx.log('TypeScript resumed saved checkpoint');return value;}});const state=await c.inspect('ops-retry');if(state.status!=='completed'||state.output!==42)throw new Error('retry did not complete');if(!(await c.listTasks()).tasks.some(t=>t.id==='ops-retry'))throw new Error('listing omitted task');"
    subprocess.run(['node','--input-type=module','-e',script],cwd=ROOT,env=child_env,check=True,capture_output=True,text=True)
    completed=a.inspect('ops-retry');assert completed['attempts']==1
    replay=a.retry('ops-retry',failed['revision'],'operator-'+str(winner));assert replay['revision']==completed['revision'] and replay['status']=='completed' and replay['attempts']==1
    expect_status(409,lambda:a.retry('ops-retry',completed['revision'],'operator-'+str(winner)))
    expect_status(409,lambda:a.retry('ops-retry',failed['revision'],'stale-retry'))
    expect_status(409,lambda:a.retry('ops-retry',completed['revision'],'terminal-retry'))
    assert a.inspect('ops-retry')['revision']==completed['revision']
    # Task history keeps a bounded suffix; logging is explicit and ownership-fenced.
    a.submit('ops-history','ops-history.v1',{})
    script=node_header+"await c.runOnce({'ops-history.v1':async ctx=>{try{await ctx.log('😃'.repeat(1025));throw new Error('oversized log accepted');}catch(e){if(!String(e).includes('4096'))throw e;}for(let i=0;i<35;i++)await ctx.log('entry-'+i);return true;}});"
    subprocess.run(['node','--input-type=module','-e',script],cwd=ROOT,env=child_env,check=True,capture_output=True,text=True,timeout=120)
    history=a.inspect('ops-history')['history'];assert len(history)==32 and history[-1]['event']=='complete'
    assert [event['detail'] for event in history if event['event']=='log']==['entry-'+str(i) for i in range(4,35)]
    assert all(set(event)<={'at_ms','event','attempts','generation','detail'} for event in history)
    # Cancellation followed by explicit retry preserves timers, signals and checkpoints.
    a.submit('ops-cancel','ops-cancel.v1',{})
    script=node_header+"await c.runOnce({'ops-cancel.v1':async ctx=>{await ctx.step('saved',()=>7);await ctx.sleep('timer',0);return ctx.waitSignal('go');}});"
    subprocess.run(['node','--input-type=module','-e',script],cwd=ROOT,env=child_env,check=True,capture_output=True,text=True)
    assert a.inspect('ops-cancel')['status']=='waiting'
    a.signal('ops-cancel','go',{'ready':True});cancelled=a.cancel('ops-cancel');reset=a.retry('ops-cancel',cancelled['revision'],'cancelled-retry')
    for field in ['steps','definitions','timers','signals','generation']:assert reset[field]==cancelled[field],field
    expect_status(409,lambda:a.retry('ops-cancel',reset['revision'],'already-queued'))
    subprocess.run(['node','--input-type=module','-e',script],cwd=ROOT,env=child_env,check=True,capture_output=True,text=True)
    assert a.inspect('ops-cancel')['output']=={'ready':True}
    # CLI hits the real embedded/remote engine with the same configuration.
    cli_env=dict(child_env,PYTHONPATH=str(ROOT/'clients/python'))
    if mode=='library':cli_env.pop('ENGINE_URL',None)
    result=subprocess.run([sys.executable,'-m','deoos','inspect','ops-retry'],cwd=ROOT,env=cli_env,check=True,capture_output=True,text=True)
    assert json.loads(result.stdout)['output']==42
    assert any(task['id']=='ops-retry' for task in a.list_tasks()['tasks'])
    passed.append('revision-fenced manual retry races/replays safely; cross-language checkpoints survive; stale owners cannot log; explicit history stays bounded; CLI reads real state')
    task_id='unverifiable-'+mode;a.submit(task_id,task_id,{})
    objects=[obj for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket) for obj in page.get('Contents',[]) if obj['Key'].endswith('/tasks/'+task_id+'/state.json')]
    key=objects[0]['Key'];unverifiable=json.loads(s3.get_object(Bucket=bucket,Key=key)['Body'].read())
    unverifiable['steps']['saved']=a.request('/tasks/names')['steps']['saved']
    s3.put_object(Bucket=bucket,Key=key,Body=json.dumps(unverifiable).encode())
    task=a.request('/claim',{'worker':'unverifiable','handlers':[task_id]})['task']
    expect_status(409,lambda:a.request('/tasks/'+task_id+'/definitions/saved',{'token':task['token'],'operation_id':'no-adoption','value':{'kind':'step','revision':'1'}}))
    assert a.request('/tasks/'+task_id)['definitions']=={}
    a.cancel(task_id)
    passed.append('untyped existing checkpoint is rejected without silently adopting a definition')
    # Isolate schedule policy fixtures from the earlier workflow fixtures in the same bucket.
    for client in clients:client.close()
    clients.clear()
    os.environ['EXECUTION_PREFIX']='schedules %/'+uuid.uuid4().hex
    if server:
     server.terminate();server.wait(timeout=10)
     server=subprocess.Popen([str(server_binary)],env=os.environ.copy(),stdout=log.open('a'),stderr=subprocess.STDOUT)
     for _ in range(100):
      if server.poll() is not None:raise AssertionError('schedule server stopped')
      try:urllib.request.urlopen(os.environ['ENGINE_URL']+'/health',timeout=1);break
      except OSError:time.sleep(.1)
     else:raise AssertionError('schedule server unavailable')
    a=create();child_env=dict(os.environ)
    if mode=='server':child_env={k:v for k,v in child_env.items() if not k.startswith('AWS_') and k not in ['EXECUTION_PREFIX','DEOOS_NATIVE_LIBRARY','DEOOS_NODE_LIBRARY']}
    env=dict(child_env,PYTHONPATH=str(ROOT/'clients/python'))
    script=node_header+"const first=await c.schedule('node-control','node-control.v1',{},86400000);await new Promise(r=>setTimeout(r,10));const replay=await c.schedule('node-control','node-control.v1',{},86400000);if(replay.first_due_ms!==first.first_due_ms)throw new Error('default anchor changed');if(!(await c.pauseSchedule('node-control')).paused)throw new Error('pause failed');if((await c.resumeSchedule('node-control')).paused)throw new Error('resume failed');const job=await c.backfill('node-control',first.first_due_ms,first.first_due_ms+1,1);if(job.backfill.remaining!==1)throw new Error('backfill failed');await c.pauseSchedule('node-control');const state=await c.inspectSchedule('node-control');if(!state.paused||state.backfill.remaining!==1)throw new Error('inspect failed');"
    subprocess.run(['node','--input-type=module','-e',script],cwd=ROOT,env=env,check=True,capture_output=True,text=True)
    passed.append('TypeScript creates/replays default-anchor schedule, pauses/resumes, accepts bounded backfill and inspects durable state')
    schedule_trace=pathlib.Path(scratch)/'schedule-trace'
    node_scheduled=node_header+"await c.runOnce({[process.env.HANDLER]:async ctx=>ctx.step('emit',()=>{appendFileSync(process.env.TRACE,JSON.stringify(ctx.task.schedule)+'\\n');return ctx.task.schedule;})});"
    python_scheduled="""import json,os;from deoos import Client
c=Client(bucket=os.environ['AWS_BUCKET']) if os.environ['WORKER_MODE']=='library' else Client.remote(os.environ['ENGINE_URL'],os.environ['ENGINE_TOKEN'])
def work(ctx,inputs):
 def emit():
  with open(os.environ['TRACE'],'a') as trace:trace.write(json.dumps(ctx.task['schedule'])+'\\n')
  return ctx.task['schedule']
 return ctx.step('emit',emit)
c.run_once({os.environ['HANDLER']:work})
c.close()
"""
    def scheduled_worker(handler,language='typescript'):
     command=['node','--input-type=module','-e',node_scheduled] if language=='typescript' else [sys.executable,'-c',python_scheduled]
     return subprocess.run(command,cwd=ROOT,env=dict(env,HANDLER=handler,TRACE=str(schedule_trace)),check=True,capture_output=True,text=True,timeout=60)
    def trace_for(identifier):
     return [record['scheduled_at'] for record in (map(json.loads,schedule_trace.read_text().splitlines()) if schedule_trace.exists() else []) if record['id']==identifier]
    anchor=int(time.time()*1000)-5000
    a.schedule('latest','scheduled-latest.v1',{'number':5.0},1000,first_due_ms=anchor,overlap='allow')
    a.schedule('latest','scheduled-latest.v1',{'number':5},1000,first_due_ms=anchor,overlap='allow')
    expect_status(409,lambda:a.schedule('latest','scheduled-latest.v2',{'number':5},1000,first_due_ms=anchor,overlap='allow'))
    expect_status(409,lambda:a.schedule('latest','scheduled-latest.v1',{'number':5},1001,first_due_ms=anchor,overlap='allow'))
    before=int(time.time()*1000);scheduled_worker('scheduled-latest.v1');after=int(time.time()*1000)
    due,=trace_for('latest');assert before-1000<=due<=after and (due-anchor)%1000==0
    assert a.inspect_schedule('latest')['next_due_ms']==due+1000
    a.pause_schedule('latest')
    anchor=int(time.time()*1000)-4000
    a.schedule('catchup','scheduled-catchup.v1',{},1000,first_due_ms=anchor,missed='catchup',overlap='skip')
    first=a.request('/claim',{'worker':'catchup-active','handlers':['scheduled-catchup.v1']})['task'];assert first['schedule']['scheduled_at']==anchor
    cursor=a.inspect_schedule('catchup')['next_due_ms']
    assert a.request('/claim',{'worker':'no-overlap','handlers':['scheduled-catchup.v1']})['task'] is None
    assert a.inspect_schedule('catchup')['next_due_ms']==cursor
    mutate(a,first,'complete',True)
    for language in ['python','typescript','python']:scheduled_worker('scheduled-catchup.v1',language)
    assert trace_for('catchup')==[anchor+1000,anchor+2000,anchor+3000]
    a.pause_schedule('catchup')
    # Latest/skip discards periods while busy; allow permits a distinct occurrence concurrently.
    for policy in ['skip','allow']:
     identifier='overlap-'+policy;handler=identifier+'.v1';anchor=int(time.time()*1000)-1000
     a.schedule(identifier,handler,{},200,first_due_ms=anchor,overlap=policy)
     first=a.request('/claim',{'worker':'first','handlers':[handler]})['task'];time.sleep(.25)
     second=a.request('/claim',{'worker':'second','handlers':[handler]})['task']
     if policy=='skip':assert second is None and a.inspect_schedule(identifier)['next_due_ms']>first['schedule']['scheduled_at']
     else:
      assert second and second['id']!=first['id'];mutate(a,second,'complete',True)
     mutate(a,first,'complete',True);a.pause_schedule(identifier)
    anchor=int(time.time()*1000)-1000
    a.schedule('paused','scheduled-paused.v1',{},86400000,first_due_ms=anchor)
    a.pause_schedule('paused');scheduled_worker('scheduled-paused.v1','python');assert trace_for('paused')==[]
    a.resume_schedule('paused');scheduled_worker('scheduled-paused.v1','typescript');assert trace_for('paused')==[anchor]
    a.pause_schedule('paused')
    # Acceptance is bounded before work; an accepted historical range waits for active work.
    anchor=int(time.time()*1000)-8000
    a.schedule('backfill','scheduled-backfill.v1',{},1000,first_due_ms=anchor)
    active=a.request('/claim',{'worker':'active','handlers':['scheduled-backfill.v1']})['task']
    expect_status(400,lambda:a.backfill('backfill',anchor,anchor+3000,limit=2))
    assert a.inspect_schedule('backfill')['backfill'] is None
    expect_status(400,lambda:a.backfill('backfill',anchor,anchor+100000,limit=100))
    job=a.backfill('backfill',anchor,anchor+3000,limit=3)['backfill'];assert job['remaining']==3
    assert a.backfill('backfill',anchor,anchor+3000,limit=3)['backfill']==job
    expect_status(409,lambda:a.backfill('backfill',anchor+1000,anchor+3000,limit=2))
    assert a.request('/claim',{'worker':'backfill-waits','handlers':['scheduled-backfill.v1']})['task'] is None
    assert a.inspect_schedule('backfill')['backfill']==job
    mutate(a,active,'complete',True)
    for language in ['typescript','python','typescript']:scheduled_worker('scheduled-backfill.v1',language)
    assert trace_for('backfill')==[anchor,anchor+1000,anchor+2000]
    assert a.inspect_schedule('backfill')['backfill'] is None
    a.pause_schedule('backfill')
    # Repeat an already emitted slot: recurring and backfill share IDs, so no callback repeats.
    a.backfill('paused',int(time.time()*1000)-86400000,int(time.time()*1000),limit=2)
    a.resume_schedule('paused');scheduled_worker('scheduled-paused.v1','python');assert trace_for('paused')==[a.inspect_schedule('paused')['first_due_ms']]
    a.pause_schedule('paused')
    # Independent engines racing one stable slot produce one claim for one deterministic task.
    anchor=int(time.time()*1000)-1000;a.schedule('schedule-race','scheduled-race.v1',{},86400000,first_due_ms=anchor)
    racers=[create() for _ in range(4)]
    with cf.ThreadPoolExecutor(max_workers=4) as pool:
     tasks=list(pool.map(lambda client:client.request('/claim',{'worker':uuid.uuid4().hex,'handlers':['scheduled-race.v1']})['task'],racers))
    task,=[task for task in tasks if task];assert task['schedule']=={'id':'schedule-race','scheduled_at':anchor}
    mutate(a,task,'complete',True);a.pause_schedule('schedule-race')
    passed.append('UTC recurring schedules honor latest/catchup, overlap, pause/resume, bounded serial backfill and occurrence deduplication across SDKs')
    passed.append('independent pollers racing a schedule claim one deterministic occurrence')
    if backend=='rustfs':
     from fault_proxy import FaultProxy
     proxy=FaultProxy();proxy.start();fault_server=None;fault=None;child=None
     fault_env=dict(os.environ,AWS_ENDPOINT='http://127.0.0.1:19002',PYTHONPATH=str(ROOT/'clients/python'))
     try:
      if mode=='server':
       fault_env.update(ENGINE_BIND='127.0.0.1:17353',ENGINE_URL='http://127.0.0.1:17353')
       fault_server=subprocess.Popen([str(server_binary)],env=fault_env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
       for _ in range(100):
        if fault_server.poll() is not None:raise AssertionError('fault server exited')
        try:urllib.request.urlopen(fault_env['ENGINE_URL']+'/health',timeout=1);break
        except OSError:time.sleep(.1)
       else:raise AssertionError('fault server unavailable')
       fault=Client.remote(fault_env['ENGINE_URL'],fault_env['ENGINE_TOKEN'])
      else:fault=Client(bucket=bucket,endpoint=fault_env['AWS_ENDPOINT'])
      a.submit('uncertain','fault',{})
      t=fault.request('/claim',dict(worker='fault-test',handlers=['fault']))['task']
      proxy.arm('lost-state-response')
      mutate(fault,t,'complete',{'committed':True})
      assert proxy.triggered==1 and a.request('/tasks/uncertain')['status']=='completed'
      passed.append('lost successful storage completion response reconciles exact revision')
      a.submit('signal-response','fault',{})
      task=fault.request('/claim',dict(worker='signal-fault',handlers=['fault']))['task']
      proxy.arm('pause-signal-state-response')
      with cf.ThreadPoolExecutor(max_workers=1) as pool:
       future=pool.submit(fault.signal,'signal-response','go',{'received':True})
       assert proxy.ready.wait(15),'signal commit never reached proxy'
       mutate(a,task,'renew') # Supersede the lost signal revision while retaining its committed reference.
       proxy.release.set();future.result(timeout=15)
      assert a.request('/tasks/signal-response/signals/go')=={'received':True}
      mutate(a,task,'complete',True)
      passed.append('lost signal commit response reconciles even after a heartbeat supersedes its revision')
      a.submit('retry-response','retry-response.v1',{},max_attempts=1)
      task=a.request('/claim',{'worker':'failed-worker','handlers':['retry-response.v1']})['task'];mutate(a,task,'fail','expected')
      before=a.inspect('retry-response');proxy.arm('pause-retry-state-response')
      with cf.ThreadPoolExecutor(max_workers=1) as pool:
       future=pool.submit(fault.retry,'retry-response',before['revision'],'uncertain-retry')
       assert proxy.ready.wait(15),'retry commit never reached proxy'
       recovered=a.request('/claim',{'worker':'next-worker','handlers':['retry-response.v1']})['task'];mutate(a,recovered,'complete',True)
       final=a.inspect('retry-response');proxy.release.set();ack=future.result(timeout=15)
      assert ack['status']=='completed' and ack['revision']==final['revision'] and ack['attempts']==1
      assert a.inspect('retry-response')['revision']==final['revision']
      passed.append('lost accepted retry response reconciles after another engine completes without resetting attempts again')
      orphan_id='orphan-'+mode
      a.submit(orphan_id,'fault',{},max_attempts=5)
      t=fault.request('/claim',dict(worker='fault-test',handlers=['fault']))['task']
      a.request('/tasks/'+orphan_id+'/definitions/saved',dict(token=t['token'],operation_id='define-orphan',value={'kind':'step','revision':'1'}))
      proxy.arm('pause-result-response')
      mutation=dict(token=t['token'],operation_id='orphan-upload',value={'uncommitted':True})
      script="import json,os;from deoos import Client;c=Client(bucket=os.environ['AWS_BUCKET']) if os.environ['WORKER_MODE']=='library' else Client.remote(os.environ['ENGINE_URL'],os.environ['ENGINE_TOKEN']);c.request('/tasks/'+os.environ['ORPHAN_ID']+'/steps/saved',json.loads(os.environ['MUTATION']))"
      child=subprocess.Popen([sys.executable,'-c',script],env=dict(fault_env,MUTATION=json.dumps(mutation),ORPHAN_ID=orphan_id),stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
      assert proxy.ready.wait(15),'immutable result upload never reached proxy'
      child.kill();child.wait()
      if fault_server:fault_server.kill();fault_server.wait()
      proxy.release.set()
      assert 'saved' not in a.request('/tasks/'+orphan_id)['steps']
      listed=[obj for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket) for obj in page.get('Contents',[]) if f'/tasks/{orphan_id}/results/' in obj['Key']]
      assert len(listed)==1,'expected one orphan upload'
      time.sleep(6.3)
      t=a.request('/claim',dict(worker='recovery',handlers=['fault']))['task'];assert t['id']==orphan_id
      mutate(a,t,'steps/saved',{'retried':True});mutate(a,t,'complete',True)
      assert a.request('/tasks/'+orphan_id+'/steps/saved')=={'retried':True}
      passed.append('engine killed after result upload; orphan ignored and retry committed fresh result')
      for gap in ['intent','task']:
       if mode=='server' and fault_server.poll() is not None:
        fault_server=subprocess.Popen([str(server_binary)],env=fault_env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        for _ in range(100):
         if fault_server.poll() is not None:raise AssertionError('schedule fault server stopped')
         try:urllib.request.urlopen(fault_env['ENGINE_URL']+'/health',timeout=1);break
         except OSError:time.sleep(.1)
        else:raise AssertionError('schedule fault server unavailable')
       identifier='gap-'+gap+'-'+mode;handler=identifier+'.v1';anchor=int(time.time()*1000)-1000
       a.schedule(identifier,handler,{},86400000,first_due_ms=anchor)
       proxy.arm('pause-schedule-intent-response' if gap=='intent' else 'pause-scheduled-task-response')
       script="import os;from deoos import Client;c=Client(bucket=os.environ['AWS_BUCKET']) if os.environ['WORKER_MODE']=='library' else Client.remote(os.environ['ENGINE_URL'],os.environ['ENGINE_TOKEN']);c.run_once({os.environ['HANDLER']:lambda ctx,inputs:True})"
       child=subprocess.Popen([sys.executable,'-c',script],env=dict(fault_env,HANDLER=handler),stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
       assert proxy.ready.wait(15),'schedule durability gap never reached proxy'
       pending=a.inspect_schedule(identifier)['pending'];assert pending and pending['due_ms']==anchor
       if gap=='intent':expect_status(404,lambda:a.request('/tasks/'+pending['task_id']))
       else:assert a.request('/tasks/'+pending['task_id'])['status']=='queued'
       child.kill();child.wait()
       if fault_server:fault_server.kill();fault_server.wait()
       proxy.release.set()
       # A fresh main engine completes the durable intent after the emitter process is gone.
       recovered=a.request('/claim',{'worker':'fresh-recovery','handlers':[handler]})['task']
       assert recovered['id']==pending['task_id'] and recovered['attempts']==1 and recovered['schedule']=={'id':identifier,'scheduled_at':anchor}
       mutate(a,recovered,'complete',True)
       schedule=a.inspect_schedule(identifier);assert schedule['pending'] is None and schedule['next_due_ms']==anchor+86400000
       assert a.request('/claim',{'worker':'no-duplicate','handlers':[handler]})['task'] is None
       a.pause_schedule(identifier)
      passed.append('emitter killed after pending intent and after task creation; independent engine resumes one occurrence and advances cursor')
     finally:
      proxy.release.set()
      if fault:fault.close()
      for process in [child,fault_server]:
       if process and process.poll() is None:process.kill();process.wait()
      proxy.close()
    # Unsupported stored tasks are neither adopted nor claimed by the new engine.
    legacy_id='zz-legacy-'+mode;a.submit(legacy_id,'legacy',{})
    objects=[obj for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket) for obj in page.get('Contents',[]) if obj['Key'].endswith('/tasks/'+legacy_id+'/state.json')]
    assert len(objects)==1
    key=objects[0]['Key'];legacy=json.loads(s3.get_object(Bucket=bucket,Key=key)['Body'].read());legacy['version']=1
    for version in [1,2]:
     legacy['version']=version;s3.put_object(Bucket=bucket,Key=key,Body=json.dumps(legacy).encode())
     expect_status(409,lambda:a.request('/claim',{'worker':'new','handlers':['legacy']}))
     unchanged=json.loads(s3.get_object(Bucket=bucket,Key=key)['Body'].read());assert unchanged==legacy and unchanged['attempts']==0
    passed.append('unsupported legacy storage rejects before writes, attempts or automatic checkpoint adoption')
    # Listing reads at most 100 task states; direct lookup reaches the remaining IDs.
    os.environ['EXECUTION_PREFIX']='listing/'+uuid.uuid4().hex
    if mode=='server':
     server.kill();server.wait();server=subprocess.Popen([str(server_binary)],env=os.environ.copy(),stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
     for _ in range(100):
      if server.poll() is not None:raise AssertionError('listing server stopped')
      try:urllib.request.urlopen(os.environ['ENGINE_URL']+'/health',timeout=1);break
      except OSError:time.sleep(.1)
     else:raise AssertionError('listing server unavailable')
    listing_clients=[create() for _ in range(8)];listing=listing_clients[0]
    assert listing.list_tasks()=={'tasks':[],'truncated':False}
    def seed(index):return listing_clients[index%8].submit('listing-'+str(index).zfill(3),'listing',{})
    with cf.ThreadPoolExecutor(max_workers=8) as pool:list(pool.map(seed,range(101)))
    listed=listing.list_tasks();assert listed['truncated'] and [task['id'] for task in listed['tasks']]==['listing-'+str(index).zfill(3) for index in range(100)]
    assert listing.inspect('listing-100')['status']=='queued'
    passed.append('listing returns exactly first100 lexicographic IDs and truthful truncation; direct ID lookup reaches omitted tasks')
    report['modes'][mode]={'passed':passed,'success':True}
   finally:
    for c in clients:c.close()
    for p in workers+[server]:
     if p and p.poll() is None:p.kill();p.wait()
 report['success']=True
except BaseException as e:report['success']=False;report['error']=repr(e);raise
finally:
 if created:
  for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket):
   objects=[{'Key':x['Key']} for x in page.get('Contents',[])]
   if objects:assert not s3.delete_objects(Bucket=bucket,Delete={'Objects':objects}).get('Errors')
  s3.delete_bucket(Bucket=bucket)
  try:s3.head_bucket(Bucket=bucket)
  except ClientError as error:assert error.response['ResponseMetadata']['HTTPStatusCode']==404
  else:raise AssertionError('test bucket still exists')
  report['cleaned']=True
 (ROOT.parent/'outputs'/'evidence'/f'two-modes-{backend}.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
