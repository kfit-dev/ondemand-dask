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
    slack_msg,
    webhook = None,
    username = 'Dask Alert',
    icon_url = 'https://avatars3.githubusercontent.com/u/17131925?s=400&v=4',
    **kwargs
):
    payload = {
        'text': slack_msg,
        'username': 'Dask Alert',
        'icon_url': 'https://avatars3.githubusercontent.com/u/17131925?s=400&v=4',
    }
    requests.post(webhook, json = payload)
