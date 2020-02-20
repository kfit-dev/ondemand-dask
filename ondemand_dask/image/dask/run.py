import time
from dask.distributed import Client, LocalCluster
import sys

print(sys.argv)
worker = int(sys.argv[1])

if __name__ == '__main__':
    cluster = LocalCluster(
        n_workers = worker,
        scheduler_port = 8786,
        host = '0.0.0.0',
        dashboard_address = '0.0.0.0:8787',
    )

    while True:
        time.sleep(600)
