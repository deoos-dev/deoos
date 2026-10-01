# Historical HTTP/storage regression suite; current two-mode acceptance is tests/two_modes.py.
"""Race conditional writes across four independent RustFS network endpoints."""
import boto3, concurrent.futures as cf, json, pathlib, time, uuid
from botocore.exceptions import ClientError
(pathlib.Path(__file__).resolve().parents[2]/'outputs/evidence').mkdir(parents=True,exist_ok=True)
clients=[boto3.client('s3',endpoint_url=f'http://127.0.0.1:{19100+n}',region_name='us-east-1',aws_access_key_id='local-development',aws_secret_access_key='local-development-only-secret') for n in range(1,5)]
bucket='durable-cluster-test-'+uuid.uuid4().hex[:16]
report={'backend':'rustfs-four-node','bucket':bucket,'rounds':[],'cleaned':False}
created=False
try:
    end=time.monotonic()+60
    while True:
        try:clients[0].create_bucket(Bucket=bucket);created=True;break
        except Exception:
            if time.monotonic()>end:raise
            time.sleep(1)
    def put(n,key,**conditions):
        try:clients[n%4].put_object(Bucket=bucket,Key=key,Body=str(uuid.uuid4()).encode(),**conditions);return True
        except ClientError as e:
            if e.response['ResponseMetadata']['HTTPStatusCode'] in [409,412]:return False
            raise
    for r in range(10):
        key=f'round-{r}'
        with cf.ThreadPoolExecutor(max_workers=32) as pool:creates=list(pool.map(lambda n:put(n,key,IfNoneMatch='*'),range(32)))
        etags=[c.head_object(Bucket=bucket,Key=key)['ETag'] for c in clients]
        assert len(set(etags))==1,etags
        with cf.ThreadPoolExecutor(max_workers=32) as pool:updates=list(pool.map(lambda n:put(n,key,IfMatch=etags[0]),range(32)))
        report['rounds'].append({'create_winners':sum(creates),'update_winners':sum(updates)})
        assert sum(creates)==1 and sum(updates)==1,report['rounds'][-1]
    report['success']=True
except BaseException as e:report['success']=False;report['error']=repr(e);raise
finally:
    if created:
        keys=clients[0].list_objects_v2(Bucket=bucket).get('Contents',[])
        if keys:clients[0].delete_objects(Bucket=bucket,Delete={'Objects':[{'Key':o['Key']} for o in keys]})
        clients[0].delete_bucket(Bucket=bucket);report['cleaned']=True
    path=pathlib.Path(__file__).resolve().parents[2]/'outputs/evidence/rustfs-cluster.json'
    path.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
