import {Client} from 'deoos';
import {randomUUID} from 'node:crypto';
const engine=Client.remote(process.env.ENGINE_URL,process.env.ENGINE_TOKEN);
const id=process.argv[2]??`greet-${randomUUID()}`;
await engine.submit(id,'greet',{name:'World'});
await engine.runOnce({greet:async(ctx,inputs)=>ctx.step('greeting',()=>`Hello, ${inputs.name}!`)});
console.log((await engine.request(`/tasks/${id}`)).output);
