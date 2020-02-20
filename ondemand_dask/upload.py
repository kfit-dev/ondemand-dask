from google.cloud import storage
import googleapiclient.discovery
import shutil
import os
import time
from function import port_open
from herpetologist import check_type
import subprocess

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
    instance_name: str,
    image_name: str,
    project_vm: str = 'ubuntu-os-cloud',
    family_vm: str = 'ubuntu-1804-lts',
    storage_image: str = 'asia-southeast1',
    install_bash: str = 'install.sh',
):
    compute = googleapiclient.discovery.build('compute', 'v1')
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    shutil.make_archive('dask', 'zip', 'image')
    blob = bucket.blob('dask.zip')
    os.remove('dask.zip')
    image_response = (
        compute.images()
        .getFromFamily(project = args.project_vm, family = args.family_vm)
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

    machine_type = 'zones/%s/machineTypes/n1-standard-1' % zone

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

    print('Waiting instance `%s` to run.' % (instance_name))

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
        dask = [r for r in results if r['name'] == args.name]
        if len(dask) > 0:
            dask = dask[0]
            ip_address = dask['networkInterfaces'][0]['accessConfigs'][0][
                'natIP'
            ]
            print('Got it, Public IP: %s' % (ip_address))
            break

        time.sleep(2)

    print('Waiting Dask cluster to run.')
    while True:
        if port_open(ip_address, 8786) and port_open(ip_address, 8787):
            print('Done.')
            break
        time.sleep(5)

    compute = googleapiclient.discovery.build('compute', 'v1')
    print('Deleting image `%s` if exists.' % (image_name))
    try:
        compute.images().delete(project = project, image = image_name)
        print('Done.')
    except:
        pass

    print('Building image `%s`.' % (image_name))
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
                family_vm,
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

    print('Deleting instance `%s`.' % (instance_name))
    compute = googleapiclient.discovery.build('compute', 'v1')
    compute.instances().delete(
        project = project, zone = zone, instance = name
    ).execute()
    print('Done.')
    return True
