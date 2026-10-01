import {Client} from 'deoos';
import {randomUUID} from 'node:crypto';
const engine=new Client({bucket:process.env.AWS_BUCKET,prefix:process.env.EXECUTION_PREFIX??'durable-v3'});
const id=process.argv[2]??`greet-${randomUUID()}`;
await engine.submit(id,'greet',{name:'World'});
await engine.runOnce({greet:async(ctx,inputs)=>ctx.step('greeting',()=>`Hello, ${inputs.name}!`)});
console.log((await engine.request(`/tasks/${id}`)).output);
