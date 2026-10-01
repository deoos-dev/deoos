import {randomUUID} from 'node:crypto';
import {Client} from '../clients/typescript/dist/index.js';
const client=new Client(process.env.ENGINE_URL);
const id=process.argv[2]??`demo-${randomUUID()}`;
await client.submit(id,'summarize',{text:'  Durable tasks survive crashes  '});
await client.runOnce({summarize:async(ctx,inputs)=>{
 const text=await ctx.step('normalize',()=>inputs.text.trim().toLowerCase());
 return ctx.step('summarize',()=>({words:text?text.split(/\s+/).length:0,text}));
}});
console.log((await client.request(`/tasks/${id}`)).output);
