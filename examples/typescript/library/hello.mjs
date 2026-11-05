import {Client} from 'deoos';
import {randomUUID} from 'node:crypto';
const provider=process.env.DEOOS_STORAGE_PROVIDER??'s3';
const storage=provider==='filesystem'
  ? {provider,directory:process.env.DEOOS_STORAGE_DIRECTORY}
  : {provider,bucket:process.env.DEOOS_STORAGE_BUCKET||(provider==='s3'?process.env.AWS_BUCKET:undefined)};
const engine=new Client({...storage,prefix:process.env.EXECUTION_PREFIX??'deoos'});
const id=process.argv[2]??`greet-${randomUUID()}`;
await engine.submit(id,'greet',{name:'World'});
await engine.runOnce({greet:async(ctx,inputs)=>ctx.step('greeting',()=>`Hello, ${inputs.name}!`)});
console.log((await engine.request(`/tasks/${id}`)).output);
