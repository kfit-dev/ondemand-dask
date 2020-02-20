import googleapiclient.discovery
from dask.distributed import Client
from datetime import datetime, timedelta
import time
import sys
import requests
import cloudpickle


print(sys.argv)
name = sys.argv[1]
project = sys.argv[2]
zone = sys.argv[3]
expired = int(sys.argv[4])


with open('post.pkl', 'rb') as fopen:
    post_message = cloudpickle.load(fopen)


while True:
    try:
        client = Client('dask:8786')
        break
    except:
        time.sleep(5)

now = datetime.now()
while True:
    workers = client.scheduler_info()['workers']
    if any([v['metrics']['executing'] != 0 for k, v in workers.items()]):
        now = datetime.now()

    if (datetime.now() - now).seconds > expired:
        slack_msg = """
            Gracefully deleted Dask cluster. 
            *Time shutdown*: {exec_date}
            *Dask cluster name*: {dask_name}
            """.format(
            exec_date = str(datetime.now()), dask_name = name
        )
        post_message(slack_msg)
        compute = googleapiclient.discovery.build('compute', 'v1')
        compute.instances().delete(
            project = project, zone = zone, instance = name
        ).execute()
        break
