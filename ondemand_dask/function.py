import socket
import requests


def port_open(ip, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((ip, int(port)))
        s.shutdown(2)
        return True
    except:
        return False


def post_slack(
    slack_msg: str,
    webhook: str = None,
    username: str = 'Dask Alert',
    icon_url: str = 'https://avatars3.githubusercontent.com/u/17131925?s=400&v=4',
    **kwargs
):
    payload = {'text': slack_msg, 'username': username, 'icon_url': icon_url}
    return requests.post(webhook, json = payload).status_code


def wait_for_operation(compute, project, zone, operation):
    while True:
        result = (
            compute.zoneOperations()
            .get(project = project, zone = zone, operation = operation)
            .execute()
        )

        if result['status'] == 'DONE':
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)
