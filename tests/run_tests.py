# Additional real-process/storage behavioral suite; current acceptance is tests/two_modes.py.
"""Provision isolated storage, run contract/lifecycle tests, and clean up."""
import argparse, concurrent.futures as cf, datetime, hashlib, json, os, pathlib, platform, subprocess, sys, uuid
(pathlib.Path(__file__).resolve().parents[1]/'outputs/evidence').mkdir(parents=True,exist_ok=True)
import boto3
from botocore.exceptions import ClientError
from lifecycle import run, ENGINE
parser=argparse.ArgumentParser();parser.add_argument('backend',choices=['rustfs','aws']);args=parser.parse_args()
ROOT=pathlib.Path(__file__).resolve().parents[1]
bucket='deoos-server-test-'+uuid.uuid4().hex[:20];prefix='proof-'+uuid.uuid4().hex
if args.backend=='aws':
    session=boto3.Session(profile_name=os.environ.get('AWS_PROFILE'),region_name='us-east-1')
    creds=session.get_credentials().get_frozen_credentials()
    os.environ.update(AWS_ACCESS_KEY_ID=creds.access_key,AWS_SECRET_ACCESS_KEY=creds.secret_key,AWS_REGION='us-east-1')
    if creds.token:os.environ['AWS_SESSION_TOKEN']=creds.token
    os.environ.pop('AWS_ENDPOINT',None);os.environ.pop('AWS_ALLOW_HTTP',None)
    s3=session.client('s3');identity=session.client('sts').get_caller_identity()['Account']
else:
    os.environ.update(AWS_ACCESS_KEY_ID='local-development',AWS_SECRET_ACCESS_KEY='local-development-only-secret',AWS_REGION='us-east-1',AWS_ENDPOINT='http://127.0.0.1:19000',AWS_ALLOW_HTTP='true')
    os.environ.pop('AWS_SESSION_TOKEN',None)
    s3=boto3.client('s3',endpoint_url=os.environ['AWS_ENDPOINT'],region_name='us-east-1');identity='local'
os.environ.update(DEOOS_STORAGE_BUCKET=bucket,EXECUTION_PREFIX=prefix)
report=dict(backend=args.backend,bucket=bucket,prefix=prefix,account=identity,started=datetime.datetime.now(datetime.timezone.utc).isoformat(),passed=[],cleaned=False)
report['engine_binary']=str(ENGINE)
report['engine_sha256']=hashlib.sha256(ENGINE.read_bytes()).hexdigest()
report['sdk_sha256']={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in [ROOT/'clients/python/deoos/__init__.py',ROOT/'clients/typescript/dist/index.js']}
report['python_version']=platform.python_version()
report['rust_version']=subprocess.check_output(['rustc','--version'],text=True).strip()
created=False
try:
    s3.create_bucket(Bucket=bucket);created=True
    if args.backend=='aws':
        s3.put_public_access_block(Bucket=bucket,PublicAccessBlockConfiguration={k:True for k in ['BlockPublicAcls','IgnorePublicAcls','BlockPublicPolicy','RestrictPublicBuckets']})
        s3.put_bucket_encryption(Bucket=bucket,ServerSideEncryptionConfiguration={'Rules':[{'ApplyServerSideEncryptionByDefault':{'SSEAlgorithm':'AES256'}}]})
        s3.put_bucket_tagging(Bucket=bucket,Tagging={'TagSet':[{'Key':'purpose','Value':'deoos-test'}]})
    def conditional(key,body,**kw):
        try:s3.put_object(Bucket=bucket,Key=key,Body=body,**kw);return True
        except ClientError as e:
            if e.response['ResponseMetadata']['HTTPStatusCode'] in [409,412]:return False
            raise
    with cf.ThreadPoolExecutor(max_workers=16) as pool:
        created_results=list(pool.map(lambda _:conditional('contract/create',b'initial',IfNoneMatch='*'),range(32)))
    assert sum(created_results)==1,created_results
    etag=s3.get_object(Bucket=bucket,Key='contract/create')['ETag']
    with cf.ThreadPoolExecutor(max_workers=16) as pool:
        updates=list(pool.map(lambda n:conditional('contract/create',str(n).encode(),IfMatch=etag),range(32)))
    assert sum(updates)==1,updates
    assert not conditional('contract/create',b'stale',IfMatch=etag)
    report['passed'].append('storage contract: one create winner, one ETag update winner, stale ETag rejected')
    report['passed']+=run()
    report['success']=True
except BaseException as e:
    report['success']=False;report['error']=repr(e);raise
finally:
    if created:
        try:
            pages=s3.get_paginator('list_objects_v2').paginate(Bucket=bucket)
            for page in pages:
                objects=[{'Key':o['Key']} for o in page.get('Contents',[])]
                if objects:
                    response=s3.delete_objects(Bucket=bucket,Delete={'Objects':objects})
                    assert not response.get('Errors'),response
            s3.delete_bucket(Bucket=bucket)
            try:s3.head_bucket(Bucket=bucket)
            except ClientError as e:assert e.response['ResponseMetadata']['HTTPStatusCode']==404
            else:raise AssertionError('deleted bucket still accessible')
            report['cleaned']=True
        finally:
            (ROOT/'outputs'/'evidence'/f'{args.backend}.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
