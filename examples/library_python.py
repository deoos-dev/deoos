"""No engine process or localhost configuration: Rust runs in this Python process."""
import sys,uuid
from deoos import Client
engine=Client()
def greet(ctx,inputs):
    return ctx.step('greeting',lambda:f"Hello, {inputs['name']}!")
task_id=sys.argv[1] if len(sys.argv)>1 else 'greet-'+uuid.uuid4().hex
engine.submit(task_id,'greet',{'name':'World'})
engine.run_once({'greet':greet})
print(engine.request('/tasks/'+task_id)['output'])
engine.close()
