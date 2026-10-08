"""Runnable arbitrary-work example: normalize and summarize text."""
import os, sys, uuid
from deoos import Client
client=Client(os.environ.get('ENGINE_URL','http://127.0.0.1:7331'))
def summarize(ctx,inputs):
    text=ctx.step('normalize',lambda:inputs['text'].strip().lower())
    return ctx.step('summarize',lambda:{'words':len(text.split()),'text':text})
task_id=sys.argv[1] if len(sys.argv)>1 else 'demo-'+uuid.uuid4().hex
client.submit(task_id,'summarize',{'text':'  Durable tasks survive crashes  '})
client.run_once({'summarize':summarize})
print(client.request('/tasks/'+task_id)['output'])
