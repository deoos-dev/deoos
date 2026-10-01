import {Client} from '../clients/typescript/dist/index.js';
const client = new Client(process.env.ENGINE_URL);
await client.runOnce({example: async (ctx,inputs)=>{
  const value = await ctx.step('first',()=>{throw new Error('completed first step was incorrectly repeated');});
  return ctx.step('second',()=>({number:value.number*2}));
}},'typescript-example');
