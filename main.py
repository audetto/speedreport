import json
import sqlite3
import subprocess
from pathlib import Path
from typing import Dict

import pandas as pd


def get_result() -> Dict:
    command = ['speedtest', '-f', 'json-pretty']
    output = subprocess.run(command, capture_output=True)
    if output.returncode:
        print(output.stderr.decode())
        raise RuntimeError(f'Exit code = {output.returncode}')
    stdout = output.stdout.decode()

    data = json.loads(stdout)
    return data


def dummy_json() -> Dict:
    data = {
        "type": "result",
        "timestamp": "2020-08-09T15:00:10Z",
        "ping": {
            "jitter": 3.7690000000000001,
            "latency": 9.6180000000000003
        },
        "download": {
            "bandwidth": 13932979,
            "bytes": 123698296,
            "elapsed": 9013
        },
        "upload": {
            "bandwidth": 1250537,
            "bytes": 7515120,
            "elapsed": 6000
        },
        "isp": "Virgin Media",
        "interface": {
            "internalIp": "192.168.0.30",
            "name": "eno1",
            "macAddr": "11:11:11:11:11:11",
            "isVpn": False,
            "externalIp": "11.11.11.11"
        },
        "server": {
            "id": 1111,
            "name": "ISP",
            "location": "City",
            "country": "Country",
            "host": "a.server.net",
            "port": 8080,
            "ip": "22.22.22.22"
        },
        "result": {
            "id": "1111",
            "url": "https://www.speedtest.net/result/c/1111"
        }
    }

    return data


def flat_dict(data: Dict, root: str = '', sep: str = '_'):
    for k, v in data.items():
        key = root + sep + k if root else k
        if isinstance(v, Dict):
            yield from flat_dict(v, key, sep)
        else:
            yield key, v


def main():
    data = get_result()
    # data = dummy_json()

    data['timestamp'] = pd.to_datetime(data['timestamp'])
    if 'packetLoss' not in data:
        # it is not always reported
        # we need to avoid schema problems
        data['packetLoss'] = 0

    flat_data = {k: v for k, v in flat_dict(data)}

    df = pd.DataFrame(flat_data, index=[0])

    path = Path.home() / 'speedreport.db'

    with sqlite3.connect(path) as db:
        df.to_sql('data', db, if_exists='append')


if __name__ == '__main__':
    main()
