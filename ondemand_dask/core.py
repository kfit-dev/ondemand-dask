import googleapiclient.discovery
import time
from herpetologist import check_type
from datetime import datetime
from .function import port_open, post_slack, wait_for_operation
from typing import Callable


@check_type
def delete(cluster_name: str, project: str, zone: str):
    """
    function to delete dask cluster.

    parameter
    ---------

    cluster_name: str
        dask cluster name.
    project: str
        project id inside gcp.
    zone: str
        compute zone for the cluster.
    """

    compute = googleapiclient.discovery.build('compute', 'v1')
    operation = (
        compute.instances()
        .delete(project = project, zone = zone, instance = cluster_name)
        .execute()
    )
    wait_for_operation(compute, project, zone, operation['name'])
    return True


@check_type
def spawn(
    cluster_name: str,
    image_name: str,
    project: str,
    zone: str,
    cpu: int,
    ram: int,
    worker_size: int,
    disk_size: int = 10,
    check_exist: bool = True,
    preemptible: bool = False,
    graceful_delete: int = 180,
    webhook_function: Callable = post_slack,
    **kwargs,
):
    """
    function to spawn a dask cluster.

    parameter
    ---------

    cluster_name: str
        dask cluster name.
    image_name: str
        image name we built.
    project: str
        project id inside gcp.
    zone: str
        compute zone for the cluster.
    cpu: int
        cpu core count.
    ram: int
        ram size in term of MB.
    worker_size: int
        worker size of dask cluster, good value should be worker size = 2 * cpu core.
    disk_size: int, (default=10)
        Disk size (GB) for the dask cluster.
    check_exist: bool, (default=True)
        if True, will check the cluster exist. If exist, will return ip address.
    preemptible: bool, (default=False)
        if True, will use preemptible VM, low cost and short life span. 
        Read more, https://cloud.google.com/compute/docs/instances/preemptible
    graceful_delete: int, (default=180)
        Dask will automatically delete itself if no process after graceful_delete (seconds).
    webhook_function: Callable, (default=post_slack)
        Callable function to send alert, default is post_slack.
    **kwargs:
        Keyword arguments to pass to webhook_function.

    Returns
    -------
    dictionary: {'ip': ip_address, 'internal_ip': internal_ip}
    """

    if cpu < 1:
        raise Exception('cpu must be bigger than 0')
    if ram % 256 != 0:
        raise Exception('ram must be divisible by 256')
    if worker_size < 1:
        raise Exception('worker_size must be bigger than 0')

    if webhook_function.__name__ == 'post_slack':

        def nested_post(msg):
            return webhook_function(msg, **kwargs)

    else:

        def nested_post(msg):
            return webhook_function(msg)

    compute = googleapiclient.discovery.build('compute', 'v1')
    ip_address, internal_ip = None, None

    if check_exist:
        result = (
            compute.instances().list(project = project, zone = zone).execute()
        )
        results = result['items'] if 'items' in result else None
        dask = [r for r in results if r['name'] == cluster_name]
        if len(dask) > 0:
            dask = dask[0]
            ip_address = dask['networkInterfaces'][0]['accessConfigs'][0][
                'natIP'
            ]
            internal_ip = dask['networkInterfaces'][0]['networkIP']
            print(ip_address, internal_ip, 'done.')

    if not ip_address:

        image_response = (
            compute.images()
            .get(project = project, image = image_name)
            .execute()
        )

        source_disk_image = image_response['selfLink']
        machine_type = f'zones/{zone}/machineTypes/custom-{cpu}-{ram}-ext'

        startup_script = f'worker_size={worker_size} name={cluster_name} project={project} zone={zone} expired={graceful_delete} docker-compose -f docker-compose.yaml up --build'

        config = {
            'name': cluster_name,
            'tags': {'items': ['dask']},
            'machineType': machine_type,
            'disks': [
                {
                    'boot': True,
                    'autoDelete': True,
                    'diskSizeGb': disk_size,
                    'initializeParams': {'sourceImage': source_disk_image},
                }
            ],
            'networkInterfaces': [
                {
                    'network': 'global/networks/default',
                    'accessConfigs': [
                        {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
                    ],
                }
            ],
            'serviceAccounts': [
                {
                    'email': 'default',
                    'scopes': [
                        'https://www.googleapis.com/auth/devstorage.read_write',
                        'https://www.googleapis.com/auth/logging.write',
                        'https://www.googleapis.com/auth/compute',
                    ],
                }
            ],
            'metadata': {
                'items': [{'key': 'startup-script', 'value': startup_script}]
            },
        }

        if preemptible:
            config['scheduling'] = {'preemptible': True}

        operation = (
            compute.instances()
            .insert(project = project, zone = zone, body = config)
            .execute()
        )

        print(f'Waiting instance `{cluster_name}` to run.')

        while True:
            result = (
                compute.zoneOperations()
                .get(
                    project = project,
                    zone = zone,
                    operation = operation['name'],
                )
                .execute()
            )

            if result['status'] == 'DONE':
                if 'error' in result:
                    raise Exception(result['error'])
                else:
                    print('Done.')
                break

            time.sleep(1)

        while True:
            result = (
                compute.instances()
                .list(project = project, zone = zone)
                .execute()
            )
            results = result['items'] if 'items' in result else None
            dask = [r for r in results if r['name'] == cluster_name]
            if len(dask) > 0:
                dask = dask[0]
                ip_address = dask['networkInterfaces'][0]['accessConfigs'][0][
                    'natIP'
                ]
                internal_ip = dask['networkInterfaces'][0]['networkIP']
                print(ip_address, internal_ip, 'done.')

                break

            time.sleep(5)

        print('Waiting Dask cluster to run.')
        while True:
            if port_open(ip_address, 8786) and port_open(ip_address, 8787):
                print('Done.')
                break
            time.sleep(5)

    slack_msg = """
        Spawned Dask cluster. 
        *Time spawn*: {exec_date}
        *Dask cluster name*: {dask_name}
        *CPU Core*: {cpu}
        *RAM (MB)*: {ram}
        *Worker count*: {worker_size}
        *Dask Dasboard Url*: http://{dask_ip}:8787
        """.format(
        exec_date = str(datetime.now()),
        dask_name = cluster_name,
        dask_ip = ip_address,
        cpu = cpu,
        ram = ram,
        worker_size = worker_size,
    )
    nested_post(slack_msg)

    return {'ip': ip_address, 'internal_ip': internal_ip}
