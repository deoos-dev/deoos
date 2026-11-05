import os,sys,uuid
from deoos import Client
engine=Client.remote(os.environ['ENGINE_URL'],token=os.environ.get('ENGINE_TOKEN'))
task_id=sys.argv[1] if len(sys.argv)>1 else 'greet-'+uuid.uuid4().hex
engine.submit(task_id,'greet',{'name':'World'})
engine.run_once({'greet':lambda ctx,inputs:ctx.step('greeting',lambda:f"Hello, {inputs['name']}!")})
print(engine.request('/tasks/'+task_id)['output'])
