"""Recovery test worker; chooses one of the two public SDK modes."""
import os,time
from deoos import Client
client=Client(bucket=os.environ['DEOOS_STORAGE_BUCKET']) if os.environ['DEOOS_MODE']=='library' else Client.remote(os.environ['ENGINE_URL'],os.environ.get('ENGINE_TOKEN'))
def work(ctx,inputs):
    def first():
        with open(inputs['trace'],'a') as f:f.write('first\n')
        return {'number':inputs['number']+1}
    value=ctx.step('first',first)
    with open(inputs['ready'],'w') as f:f.write('checkpoint committed')
    time.sleep(inputs.get('pause',0))
    return ctx.step('second',lambda:{'number':value['number']*2})
client.run_once({'work':work})
client.close()
