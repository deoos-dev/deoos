import {Client} from '../../clients/typescript/dist/index.js';
const client=process.env.DEOOS_MODE==='library'?new Client({bucket:process.env.DEOOS_STORAGE_BUCKET,prefix:process.env.EXECUTION_PREFIX,lease_ms:Number(process.env.LEASE_MS)}):Client.remote(process.env.ENGINE_URL,process.env.ENGINE_TOKEN);
await client.runOnce({work:async(ctx)=>{
 const value=await ctx.step('first',()=>{throw new Error('committed step repeated');});
 return ctx.step('second',()=>({number:value.number*2}));
}});
