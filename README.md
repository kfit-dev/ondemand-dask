<p align="center">
    <a href="#readme">
        <img alt="logo" width="40%" src="dask.png">
    </a>
</p>

---

**ondemand-dask**, Dask cluster on demand and automatically delete itself after expired.

## Problem statement

Dask is really a great library for distributed programming using Python scale up more than 1TB data. Traditionally, we spawned a Dask cluster using VM or Kubernetes to process our data, and after that, Dask cluster will idle probably most of the time. We got charged even Dask cluster in idle mode (no computation happened).

So this library will help you to spawn a Dask cluster with custom size of CPU, RAM and worker, and automatically gracefully delete itself after idle for certain period.

It also support simple alert system to post message during spawning and gracefully delete, default is Slack.

## Installing from the PyPI

```bash
pip install ondemand-dask
```

## Before use

**Make sure your machine already installed gcloud SDK and your GCP account powerful enough to spawn compute engine and upload to google storage**.

If not, simply download gcloud SDK, https://cloud.google.com/sdk/docs/downloads-versioned-archives, after that,

```bash
gcloud init
```

## examples

Simply check notebooks in [example](example).

## usage

#### ondemand_dask.build_image

**Before able to use on demand dask, you need to build the image first.**

```python

def build_image(
    project: str,
    zone: str,
    bucket_name: str,
    instance_name: str,
    image_name: str,
    project_vm: str = 'ubuntu-os-cloud',
    family_vm: str = 'ubuntu-1804-lts',
    storage_image: str = 'asia-southeast1',
    webhook_function: Callable = post_slack,
    install_bash: str = None,
    **kwargs
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
    project_vm: str, (default='ubuntu-os-cloud')
        project name for vm. 
    family_vm: str, (default='ubuntu-1804-lts')
        family name for vm.
    storage_image: str, (default='asia-southeast1')
        storage location for dask image.
    webhook_function: Callable, (default=post_slack)
        Callable function to send alert, default is post_slack.
    **kwargs:
        Keyword arguments to pass to callback.
    """

```

Usage is simply,

```python
import ondemand_dask

project = 'project'
zone = 'asia-southeast1-a'
bucket_name = 'bucket'
instance_name = 'dask-build'
image_name = 'dask-image'
webhook_slack = 'https://hooks.slack.com/services/'

ondemand_dask.build_image(
    project = project,
    zone = zone,
    bucket_name = bucket_name,
    instance_name = instance_name,
    image_name = instance_name,
    webhook = webhook_slack,
)
```

Simply check [example/upload.ipynb](example/upload.ipynb).

This process only need to do once, unless,

1. Custom alert platform, eg, Telegram, Discord and etc.

```python

# only accept one parameter.
def post_to_platform(msg: str):
    # do somethint

ondemand_dask.build_image(
    project = project,
    zone = zone,
    bucket_name = bucket_name,
    instance_name = instance_name,
    image_name = instance_name,
    webhook_function = post_to_platform
    # webhook not required
)

```