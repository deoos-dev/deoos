import {Client} from '../clients/typescript/dist/index.js';
const client=new Client(process.env.ENGINE_URL);
await client.runOnce({effect:async(ctx,inputs)=>ctx.step('effect',async()=>{
 const response=await fetch(inputs.url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({key:ctx.idempotencyKey('effect')})});
 if(!response.ok)throw new Error('external API failed');return response.json();
})});
