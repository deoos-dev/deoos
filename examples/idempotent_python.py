import json, os, pathlib, time, urllib.request
from deoos import Client
client=Client(os.environ['ENGINE_URL'])
def effect(ctx,inputs):
    def call():
        req=urllib.request.Request(inputs['url'],data=json.dumps({'key':ctx.idempotency_key('effect')}).encode(),headers={'Content-Type':'application/json'})
        with urllib.request.urlopen(req) as r:result=json.load(r)
        pathlib.Path(inputs['ready']).write_text('external effect happened; checkpoint not committed')
        time.sleep(120)
        return result
    return ctx.step('effect',call)
client.run_once({'effect':effect})
