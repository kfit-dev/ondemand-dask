from google.cloud import storage
import googleapiclient.discovery
import shutil
import os
import time
from .function import port_open, post_slack
from .libraries import extra_libraries, important_libraries
from herpetologist import check_type
import subprocess
import cloudpickle
from typing import Callable, List


additional_command = [
    'gsutil cp gs://general-bucket/dask.zip dask.zip',
    'unzip dask.zip',
    'worker_size=1 name=a project=a zone=a expired=99999 docker-compose -f docker-compose.yaml up --build',
]
dask_network = {
    'allowed': [{'IPProtocol': 'tcp', 'ports': ['8787', '8786']}],
    'description': '',
    'direction': 'INGRESS',
    'kind': 'compute#firewall',
    'name': 'dask-network',
    'priority': 1000.0,
    'sourceRanges': ['0.0.0.0/0'],
    'targetTags': ['dask'],
}


@check_type
def build_image(
    project: str,
    zone: str,
    bucket_name: str,
    image_name: str,
    family: str,
    instance_name: str = 'build-dask-instance',
    source_image: dict = {
        'project': 'ubuntu-os-cloud',
        'family': 'ubuntu-1804-lts',
    },
    storage_image: str = 'asia-southeast1',
    webhook_function: Callable = post_slack,
    validate_webhook: bool = True,
    additional_libraries: List[str] = extra_libraries,
    install_bash: str = None,
    dockerfile: str = None,
    **kwargs,
):
    """
    Parameters
    ----------

    project: str
        project id
    zone: str
    bucket_name: str
        bucket name to upload dask code, can be private.
    image_name: str
        image name for dask bootloader.
    family: str
        family name for built image
    instance_name: str (default='build-dask-instance')
        Start-up instance to build the image
    source_image: dict (default={'project': 'ubuntu-os-cloud', 'family': 'ubuntu-1804-lts'})
        Source image to start the instance for building the image
    storage_image: str, (default='asia-southeast1')
        storage location for dask image.
    webhook_function: Callable, (default=post_slack)
        Callable function to send alert during gracefully delete, default is post_slack.
    validate_webhook: bool, (default=True)
        if True, will validate `webhook_function`. 
        Not suggest to set it as False because this webhook_function will use during gracefully delete.
    additional_libraries: List[str], (default=extra_libraries). 
        add more libraries from PYPI. This is necessary if want dask cluster able to necessary libraries.
    install_bash: str, (default=None). 
        File path to custom start-up script to build disk image
    dockerfile: List[str], (default=None). 
        File path to custom Dockerfile to build docker image
    **kwargs:
        Keyword arguments to pass to webhook_function.
    """

    def nested_post(msg):
        return webhook_function(msg, **kwargs)

    if validate_webhook:
        if nested_post('Testing from ondemand-dask') != 200:
            raise Exception('`webhook_function` must returned 200.')

    compute = googleapiclient.discovery.build('compute', 'v1')
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    this_dir = os.path.dirname(__file__)
    pkl = os.path.join(this_dir, 'image', 'dask', 'post.pkl')
    with open(pkl, 'wb') as fopen:
        cloudpickle.dump(nested_post, fopen)

    reqs = important_libraries + additional_libraries
    reqs = list(set(reqs))

    req = os.path.join(this_dir, 'image', 'dask', 'requirements.txt')
    with open(req, 'w') as fopen:
        fopen.write('\n'.join(reqs))

    if dockerfile:
        with open(dockerfile, 'r') as fopen:
            script = fopen.read()
        dockerfile_path = os.path.join(this_dir, 'image', 'dask', 'Dockerfile')
        with open(dockerfile_path, 'w') as fopen:
            fopen.write(script)

    image = os.path.join(this_dir, 'image')
    shutil.make_archive('dask', 'zip', image)
    blob = bucket.blob('dask.zip')
    blob.upload_from_filename('dask.zip')
    os.remove('dask.zip')
    image_response = (
        compute.images()
        .getFromFamily(**source_image)
        .execute()
    )
    source_disk_image = image_response['selfLink']

    try:
        print('Creating `dask-network` firewall rule.')
        compute.firewalls().insert(
            project = project, body = dask_network
        ).execute()
        print('Done.')
    except:
        print('`dask-network` exists.')

    machine_type = f'zones/{zone}/machineTypes/n1-standard-1'

    if install_bash is None:
        install_bash = 'install.sh'
        install_bash = os.path.join(this_dir, install_bash)

    startup_script = open(install_bash).read()
    startup_script = '\n'.join(
        startup_script.split('\n') + additional_command
    ).replace('general-bucket', bucket_name)



    config = {
        'name': instance_name,
        'tags': {'items': ['dask']},
        'machineType': machine_type,
        'disks': [
            {
                'boot': True,
                'autoDelete': True,
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
            'items': [
                {'key': 'startup-script', 'value': startup_script},
                {'key': 'bucket', 'value': bucket_name},
            ]
        },
    }

    operation = (
        compute.instances()
        .insert(project = project, zone = zone, body = config)
        .execute()
    )

    print(f'Waiting instance `{instance_name}` to run.')

    while True:
        result = (
            compute.zoneOperations()
            .get(project = project, zone = zone, operation = operation['name'])
            .execute()
        )

        if result['status'] == 'DONE':
            if 'error' in result:
                raise Exception(result['error'])
            else:
                print('Done.')
            break

        time.sleep(1)

    print('Waiting IP Address to check health.')

    while True:
        result = (
            compute.instances().list(project = project, zone = zone).execute()
        )
        results = result['items'] if 'items' in result else None
        dask = [r for r in results if r['name'] == instance_name]
        if len(dask) > 0:
            dask = dask[0]
            ip_address = dask['networkInterfaces'][0]['accessConfigs'][0][
                'natIP'
            ]
            print(f'Got it, Public IP: {ip_address}')
            break

        time.sleep(2)

    print('Waiting Dask cluster to run.')
    while True:
        if port_open(ip_address, 8786) and port_open(ip_address, 8787):
            print('Done.')
            break
        time.sleep(5)

    compute = googleapiclient.discovery.build('compute', 'v1')
    print(f'Deleting image `{image_name}` if exists.')
    try:
        compute.images().delete(project = project, image = image_name).execute()
        print('Done.')
    except:
        pass

    # give a rest to gcp API before build the image.
    time.sleep(20)

    print(f'Building image `{image_name}`.')
    try:
        o = subprocess.check_output(
            [
                'gcloud',
                'compute',
                'images',
                'create',
                image_name,
                '--source-disk',
                instance_name,
                '--source-disk-zone',
                zone,
                '--family',
                family,
                '--storage-location',
                storage_image,
                '--force',
            ],
            stderr = subprocess.STDOUT,
        )
        print('Done.')
    except subprocess.CalledProcessError as e:
        print(e.output.decode('utf-8'))
        raise

    print(f'Deleting instance `{instance_name}`.')
    compute = googleapiclient.discovery.build('compute', 'v1')
    compute.instances().delete(
        project = project, zone = zone, instance = instance_name
    ).execute()
    print('Done.')
    return True
