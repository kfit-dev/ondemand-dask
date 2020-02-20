import googleapiclient.discovery
import time
import socket
from herpetologist import check_type
import requests
from datetime import datetime


def post_slack(slack_msg):
    payload = {
        'text': slack_msg,
        'username': 'Dask Alert',
        'icon_url': 'https://avatars3.githubusercontent.com/u/17131925?s=400&v=4',
    }
    requests.post(
        'https://hooks.slack.com/services/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',
        json = payload,
    )


def port_open(ip, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((ip, int(port)))
        s.shutdown(2)
        return True
    except:
        return False


@check_type
def delete_dask(name: str, project: str, zone: str):
    """
    function to delete dask cluster.

    parameter
    ---------

    name: str
        dask cluster name.
    project: str
        project id inside gcp.
    zone: str
        compute zone for the cluster.
    """

    compute = googleapiclient.discovery.build('compute', 'v1')
    return (
        compute.instances()
        .delete(project = project, zone = zone, instance = name)
        .execute()
    )


@check_type
def spawn_dask(
    name: str,
    project: str,
    cpu: int,
    ram: int,
    zone: str,
    worker_size: int,
    check_exist: bool = True,
    post_to_slack = True,
    graceful_delete: int = 180,
):
    """
    function to spawn a dask cluster.

    parameter
    ---------

    name: str
        dask cluster name.
    project: str
        project id inside gcp.
    cpu: int
        cpu core count.
    ram: int
        ram size in term of MB.
    zone: str
        compute zone for the cluster.
    worker_size: int
        worker size of dask cluster, good value should be worker size = 2 * cpu core.
    check_exist: bool, (default=True)
        if True, will check the cluster exist. If exist, will return ip address.
    post_to_slack: bool, (default=True)
        if True, will post to #data-alerts.
    graceful_delete: int, (default=180)
        Dask will automatically delete itself if no process after graceful_delete (seconds).
    """

    compute = googleapiclient.discovery.build('compute', 'v1')
    ip_address = None

    if check_exist:
        result = (
            compute.instances().list(project = project, zone = zone).execute()
        )
        results = result['items'] if 'items' in result else None
        dask = [r for r in results if r['name'] == name]
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
            .getFromFamily(project = 'fave-data', family = 'ubuntu-1804-lts')
            .execute()
        )
        source_disk_image = image_response['selfLink']
        machine_type = 'zones/%s/machineTypes/custom-%d-%d-ext' % (
            zone,
            cpu,
            ram,
        )

        startup_script = (
            'worker_size=%d name=%s project=%s zone=%s expired=%d docker-compose -f docker-compose.yaml up --build'
            % (worker_size, name, project, zone, graceful_delete)
        )

        config = {
            'name': name,
            'tags': {'items': ['dask']},
            'machineType': machine_type,
            'disks': [
                {
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': {'sourceImage': source_disk_image},
                }
            ],
            # Specify a network interface with NAT to access the public
            # internet.
            'networkInterfaces': [
                {
                    'network': 'global/networks/default',
                    'accessConfigs': [
                        {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
                    ],
                }
            ],
            # Allow the instance to access cloud storage and logging.
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

        operation = (
            compute.instances()
            .insert(project = project, zone = zone, body = config)
            .execute()
        )

        print('Waiting %s to run..' % (name))

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
                    print('done.')
                break

            time.sleep(1)

        while True:
            result = (
                compute.instances()
                .list(project = project, zone = zone)
                .execute()
            )
            results = result['items'] if 'items' in result else None
            dask = [r for r in results if r['name'] == name]
            if len(dask) > 0:
                dask = dask[0]
                ip_address = dask['networkInterfaces'][0]['accessConfigs'][0][
                    'natIP'
                ]
                internal_ip = dask['networkInterfaces'][0]['networkIP']
                print(ip_address, internal_ip, 'done.')

                break

            time.sleep(5)

        print('Waiting Dask cluster running..')
        while True:
            if port_open(ip_address, 8786) and port_open(ip_address, 8787):
                print('done.')
                break
            time.sleep(5)

    if post_to_slack:
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
            dask_name = name,
            dask_ip = ip_address,
            cpu = cpu,
            ram = ram,
            worker_size = worker_size,
        )
        post_slack(slack_msg)

    return {'ip': ip_address, 'internal_ip': internal_ip}
