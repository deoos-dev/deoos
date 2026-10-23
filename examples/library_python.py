"""Library mode runs the Rust engine in this process.

Set DEOOS_STORAGE_BUCKET (or AWS_BUCKET) and storage credentials. This is not
zero-config. The filesystem provider is macOS ARM64 with local APFS only.
"""
import sys,uuid
from deoos import Client
engine=Client()
def greet(ctx,inputs):
    return ctx.step('greeting',lambda:f"Hello, {inputs['name']}!")
task_id=sys.argv[1] if len(sys.argv)>1 else 'greet-'+uuid.uuid4().hex
engine.submit(task_id,'greet',{'name':'World'})
engine.run_once({'greet':greet})
print(engine.view(task_id)['output'])
engine.close()
