import { randomUUID, createHash } from 'node:crypto';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url);
export type StorageConfig = {prefix?:string;lease_ms?:number} & (
  {provider:'filesystem';directory:string;bucket?:never;region?:never;endpoint?:never;access_key_id?:never;secret_access_key?:never;session_token?:never;allow_http?:never}
  | {provider?:'s3'|'gcs'|'azure';bucket:string;directory?:never;region?:string;endpoint?:string;access_key_id?:string;secret_access_key?:string;session_token?:string;allow_http?:boolean}
);
export interface ScheduleOptions {first_due_ms?:number;missed?:'latest'|'catchup';overlap?:'skip'|'allow';max_attempts?:number;retry_ms?:number}
interface NativeHandle {request(method:string,path:string,data:string):Promise<string>}
export interface HistoryEvent {at_ms:number;event:string;attempts:number;detail?:unknown}
export interface Task { history?:HistoryEvent[];revision?:string;status?:string; version:number; id:string; handler:string; inputs:unknown; token:string; expires_at:number; steps:Record<string,string>;schedule?:{id:string;scheduled_at:number}|null }
export interface TaskView {
  id:string;
  function:string;
  status:ExecutionSummary['status'];
  inputs:unknown;
  output:unknown;
  error:string|null;
  attempts:number;
  completed_steps:string[];
  wait?:ExecutionSummary['wait'];
}
export interface TaskEvent {at_ms:number;event:string;detail?:string}
export type Handler = (context:Context, inputs:any) => unknown | Promise<unknown>;
export interface ExecutionSummary {
  summary_version:1;
  id:string;
  handler:string;
  status:'queued'|'running'|'waiting'|'completed'|'failed'|'cancelled';
  attempts:number;
  max_attempts:number;
  available_at_ms:number;
  completed_steps:string[];
  wait:{kind:'timer';name:string;deadline_ms:number|null}
    | {kind:'signal';name:string;assigned:boolean}
    | {kind:'children';ids:string[]}
    | null;
  last_failure:{message:string;at_ms:number|null}|null;
  actions:Array<'cancel'|'retry'|'signal'>;
}
export interface WorkerOptions {
  signal:AbortSignal;
  pollIntervalMs?:number;
  workerId?:string;
  /** taskId means the task failure was recorded; it does not classify error origin. */
  onError?:(error:unknown,taskId?:string)=>'continue'|'propagate'|Promise<'continue'|'propagate'>;
}
export class EngineError extends Error {
  constructor(public status:number, message:string){super(`${status}: ${message}`);this.name="EngineError";}
}
class Suspended extends Error {}
export class ChildFailed extends Error {
  constructor(public taskId:string, public status:string, public reason:unknown){
    super(`Child ${taskId} ${status}: ${reason??"no error detail"}`);this.name="ChildFailed";
  }
}
function validStep(name:string){
  if(typeof name!=='string'||!/^[A-Za-z0-9_.-]{1,128}$/.test(name)||name==='.'||name==='..')throw new Error('invalid step name');
}
function safeInteger(value:number,name:string,minimum=0){if(!Number.isSafeInteger(value)||value<minimum)throw new Error(`${name} must be a safe integer >= ${minimum}`);}
export class Client {
  private native?:NativeHandle;
  public url?:string;
  constructor(config:StorageConfig|string, private token?:string) {
    if(typeof config==='string') this.url=config.replace(/\/$/,'');
    else {
      let addon;
      try {
        addon=require(process.env.DEOOS_NODE_LIBRARY??'./native/deoos_node.node');
      } catch(error) {
        throw new Error('Cannot load the embedded DEOOS engine. Install the prebuilt npm package from the release for your operating system and CPU architecture, using Node 22 or newer. If DEOOS_NODE_LIBRARY is set, check that it points to a compatible addon. To connect to a shared server, use Client.remote(url, token).',{cause:error});
      }
      this.native=new addon.NativeEngine(JSON.stringify({...config,prefix:config.prefix??process.env.EXECUTION_PREFIX??"durable-v3",lease_ms:config.lease_ms??Number(process.env.LEASE_MS??30000)}));
    }
  }
  static remote(url:string,token?:string){return new Client(url,token); }
  async request(path:string, data?:unknown):Promise<any> {
    if(data!==undefined){
      let info;
      try{info=await this.request('/info');}catch(error){
        if(error instanceof EngineError&&error.status===404)throw new EngineError(409,'unsupported engine protocol; expected 3');
        throw error;
      }
      if(info?.protocol_version!==3)throw new EngineError(409,'unsupported engine protocol; expected 3');
      if(data===null||typeof data!=='object'||Array.isArray(data))throw new Error('request body must be an object');
      data={...data,protocol_version:3};
    }
    if(this.native){
      const response=JSON.parse(await this.native.request(data===undefined?'GET':'POST',path,JSON.stringify(data??null)));
      if(response.status!==200)throw new EngineError(response.status,response.error);
      return response.value;
    }
    const response=await fetch(this.url+path,{method:data===undefined?'GET':'POST',headers:{'Content-Type':'application/json',...(this.token?{Authorization:'Bearer '+this.token}:{})},body:data===undefined?undefined:JSON.stringify(data),signal:AbortSignal.timeout(10000)});
    if(!response.ok) throw new EngineError(response.status,await response.text());
    return response.json();
  }
  submit(id:string,handler:string,inputs:unknown,max_attempts=3,retry_ms=0) { return this.request('/tasks',{id,handler,inputs,max_attempts,retry_ms}); }
  signal(id:string,name:string,value:unknown,operationId:string=randomUUID()){validStep(id);validStep(name);validStep(operationId);return this.request(`/tasks/${id}/signals/${name}`,{operation_id:operationId,value});}
  cancel(id:string){validStep(id);return this.request(`/tasks/${id}/cancel`,{});}
  inspect(id:string){validStep(id);return this.request(`/tasks/${id}`);}
  /** Simple task view including application inputs/output, without coordination fields. */
  view(id:string):Promise<TaskView>{validStep(id);return this.request(`/tasks/${id}/view`);}
  async history(id:string):Promise<TaskEvent[]>{validStep(id);return (await this.request(`/tasks/${id}/history`)).history;}
  /** Persisted progress metadata without inputs, outputs, or checkpoint values. */
  summary(id:string):Promise<ExecutionSummary>{validStep(id);return this.request(`/tasks/${id}/summary`);}
  listTasks(){return this.request('/tasks');}
  retry(id:string,expected_revision:string,operation_id:string=randomUUID()){
    validStep(id);validStep(expected_revision);validStep(operation_id);
    return this.request(`/tasks/${id}/retry`,{expected_revision,operation_id});
  }
  schedule(id:string,handler:string,inputs:unknown,interval_ms:number,options:ScheduleOptions={}){
    validStep(id);validStep(handler);safeInteger(interval_ms,'interval_ms',1);
    const {first_due_ms,missed='latest',overlap='skip',max_attempts=3,retry_ms=0}=options;
    if(first_due_ms!==undefined)safeInteger(first_due_ms,'first_due_ms');
    safeInteger(max_attempts,'max_attempts',1);safeInteger(retry_ms,'retry_ms');
    if(!['latest','catchup'].includes(missed)||!['skip','allow'].includes(overlap))throw new Error('invalid schedule policy');
    return this.request('/schedules',{id,handler,inputs,interval_ms,first_due_ms,missed,overlap,max_attempts,retry_ms});
  }
  inspectSchedule(id:string){validStep(id);return this.request(`/schedules/${id}`);}
  pauseSchedule(id:string){validStep(id);return this.request(`/schedules/${id}/pause`,{});}
  resumeSchedule(id:string){validStep(id);return this.request(`/schedules/${id}/resume`,{});}
  backfill(id:string,start_ms:number,end_ms:number,limit=100){
    validStep(id);safeInteger(start_ms,'start_ms');safeInteger(end_ms,'end_ms');safeInteger(limit,'limit',1);
    if(end_ms<=start_ms||limit>1000)throw new Error('invalid backfill range or limit');
    return this.request(`/schedules/${id}/backfill`,{start_ms,end_ms,limit});
  }
  async runOnce(handlers:Record<string,Handler>,worker:string=randomUUID()):Promise<boolean> {
    return this.executeOnce(handlers,worker);
  }
  /**
   * One polling loop; the application owns signal and client lifecycle.
   * Abort interrupts idle waits and stops new claims after in-flight work finishes.
   * Errors propagate unless onError explicitly returns 'continue'; that decision
   * waits pollIntervalMs before retrying. taskId identifies a recorded task failure,
   * not error origin: application or storage errors inside a handler can receive it.
   * Claim, ownership, and terminal mutation failures have no taskId.
   */
  async runWorker(handlers:Record<string,Handler>,options:WorkerOptions):Promise<void> {
    if(!handlers||typeof handlers!=='object'||Array.isArray(handlers)||Object.keys(handlers).length===0)throw new Error('handlers must be a nonempty map of functions');
    const registered={...handlers};
    for(const [name,handler] of Object.entries(registered)){
      validStep(name);if(typeof handler!=='function')throw new Error('handlers must be a nonempty map of functions');
    }
    if(!options||!(options.signal instanceof AbortSignal))throw new Error('signal must be an application-owned AbortSignal');
    const {signal,pollIntervalMs=100,workerId=randomUUID(),onError}=options;
    safeInteger(pollIntervalMs,'pollIntervalMs',1);
    if(pollIntervalMs>2_147_483_647)throw new Error('pollIntervalMs exceeds the timer limit');
    validStep(workerId);
    if(onError!==undefined&&typeof onError!=='function')throw new Error('onError must be a function');
    const idle=async()=>{
      if(signal.aborted)return;
      await new Promise<void>(resolve=>{
        const finish=()=>{clearTimeout(timer);signal.removeEventListener('abort',finish);resolve();};
        const timer=setTimeout(finish,pollIntervalMs);
        signal.addEventListener('abort',finish,{once:true});
        if(signal.aborted)finish();
      });
    };
    while(!signal.aborted){
      let failedTaskId:string|undefined;
      try{
        const worked=await this.executeOnce(registered,workerId,id=>{failedTaskId=id;});
        if(!worked)await idle();
      }catch(error){
        if(onError===undefined)throw error;
        const decision=await onError(error,failedTaskId);
        if(decision==='propagate')throw error;
        if(decision!=='continue')throw new Error("onError must return 'continue' or 'propagate'",{cause:error});
        await idle();
      }
    }
  }
  private async executeOnce(handlers:Record<string,Handler>,worker:string,recordedFailure?:(taskId:string)=>void):Promise<boolean> {
    const {task}=await this.request('/claim',{worker,handlers:Object.keys(handlers)});
    if(!task) return false;
    if(task.version!==3) throw new Error("unsupported engine protocol version");
    const ctx=new Context(this,task);
    // Recursive timer prevents overlapping renewals and is drained before terminal writes.
    let stopped=false, timer:ReturnType<typeof setTimeout>, renewal=Promise.resolve();
    const heartbeatMs=Math.max(100,(task.expires_at-Date.now())/3);
    const beat=()=>{ timer=setTimeout(()=>{ renewal=ctx.mutate('renew').then(()=>{if(!stopped) beat();}).catch(e=>{ctx.ownershipError=e;}); },heartbeatMs); };
    beat();
    const stop=async()=>{stopped=true;clearTimeout(timer);await renewal;};
    let output:unknown;
    try { output=await handlers[task.handler](ctx,task.inputs);ctx.checkOwner(); }
    catch(error) {
      await stop();
      if(ctx.isSuspended)return true;
      if(!ctx.ownershipError){
        await ctx.mutate('fail',error instanceof ChildFailed?{error:String(error),terminal:true}:String(error));
        recordedFailure?.(task.id);
      }
      throw error;
    }
    await stop();ctx.checkOwner();await ctx.mutate('complete',output??null);return true;
  }
}
export class Context {
  private suspended=false;
  get isSuspended(){return this.suspended;}
  ownershipError:unknown;
  constructor(public client:Client, public task:Task) {}
  checkOwner(){if(this.suspended)throw new Suspended();if(this.ownershipError) throw new Error('ownership renewal failed; stop work',{cause:this.ownershipError});}
  private async currentState(){
    this.checkOwner();
    const state=await this.client.request(`/tasks/${this.task.id}`);
    if(state.status!=='running'||state.token!==this.task.token){
      this.ownershipError=new EngineError(409,'task is no longer owned by this worker');
      throw this.ownershipError;
    }
    return state;
  }
  idempotencyKey(step:string){return `${this.task.id}/${step}`;}
  mutate(action:string,value:unknown=null){return this.client.request(`/tasks/${this.task.id}/${action}`,{token:this.task.token,operation_id:randomUUID(),value});}
  async log(message:string){
    if(typeof message!=='string'||Buffer.byteLength(message,'utf8')>4096)throw new Error('log message must be a string up to 4096 bytes');
    await this.currentState();await this.mutate('log',message);
  }
  async spawn(name:string,handler:string,inputs:unknown,max_attempts=3,retry_ms=0):Promise<string>{
    validStep(name);
    const id='child-'+createHash('sha256').update(`${this.task.id}/${name}`).digest('hex').slice(0,32);
    return this.checkpoint(name,{kind:'spawn',revision:'1',child_id:id,handler,inputs,max_attempts,retry_ms},async()=>{
      await this.client.submit(id,handler,inputs,max_attempts,retry_ms);return id;
    });
  }
  async join(name:string,children:string[]):Promise<unknown[]>{
    return this.checkpoint(name,{kind:'join',revision:'1',children},async()=>{
      const states=await Promise.all(children.map(id=>this.client.request(`/tasks/${id}`)));
      if(states.some(state=>!['completed','failed','cancelled'].includes(state.status))){
        await this.mutate('suspend',{children});
        this.suspended=true;
        throw new Suspended();
      }
      const failed=states.find(state=>state.status!=='completed');
      if(failed)throw new ChildFailed(failed.id,failed.status,failed.error);
      return states.map(state=>state.output);
    });
  }
  private async suspend(condition:unknown){
    const state=await this.mutate('suspend',condition);
    if(state.status==='waiting'){this.suspended=true;throw new Suspended();}
    if(state.status!=='running')throw new Error('unexpected suspension state');
  }
  async sleep(name:string,milliseconds:number):Promise<void>{
    validStep(name);
    if(!Number.isSafeInteger(milliseconds)||milliseconds<0)throw new Error('milliseconds must be a nonnegative safe integer');
    await this.checkpoint(name,{kind:'sleep',revision:'1',milliseconds},async()=>{await this.suspend({timer:{name,milliseconds}});return null;});
  }
  async waitSignal<T=unknown>(name:string):Promise<T>{
    return this.checkpoint(name,{kind:'wait_signal',revision:'1',signal:name},async()=>{await this.suspend({signal:name});return this.client.request(`/tasks/${this.task.id}/signals/${name}`);});
  }
  async step<T>(name:string,fn:()=>T|Promise<T>,revision='1'):Promise<T> {
    validStep(revision);
    return this.checkpoint(name,{kind:'step',revision},fn);
  }
  private async checkpoint<T>(name:string,definition:unknown,fn:()=>T|Promise<T>):Promise<T> {
    validStep(name);
    this.checkOwner();
    let state;
    try{state=await this.mutate(`definitions/${name}`,definition);}
    catch(error){if(error instanceof EngineError&&error.status===409)await this.currentState();throw error;}
    if(Object.hasOwn(state.steps,name)) return this.client.request(`/tasks/${this.task.id}/steps/${name}`);
    const result=await fn();this.checkOwner();await this.mutate(`steps/${name}`,result);return result;
  }
}
