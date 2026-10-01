import os, time
from deoos import Client
client = Client(os.environ.get('ENGINE_URL', 'http://127.0.0.1:7331'))
def example(ctx, inputs):
    def first():
        with open(inputs['marker'], 'a') as f:
            f.write('first-executed\n')
        return {'number': inputs['number'] + 1}
    value = ctx.step('first', first)
    # The harness kills this process after this checkpoint acknowledgement.
    with open(inputs['ready'], 'w') as f:
        f.write('checkpoint committed')
    time.sleep(inputs.get('pause_seconds', 0))
    return ctx.step('second', lambda: {'number': value['number'] * 2})
client.run_once({'example': example}, 'python-example')
