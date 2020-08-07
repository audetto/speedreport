import os
import subprocess
import sqlite3
import io

import pandas as pd


def get_result():
    command = ['speedtest', '-f', 'csv', '--output-header']
    output = subprocess.run(command, capture_output=True)
    if output.returncode:
        print(output.stderr.decode())
        raise RuntimeError(f'Exit code = {output.returncode}')
    stdout = output.stdout.decode()
    return stdout


def dummy():
    a = '"server name","server id","latency","jitter","packet loss","download","upload","download bytes","upload bytes","share url"\n"ISP - Server","12345","7.273","3.599","N/A","13971184","1244924","130792296","4863832","https://www.speedtest.net"\n'
    return a


def main():
    data = get_result()
    # data = dummy()
    f = io.StringIO(data)
    df = pd.read_csv(f)
    df['date'] = pd.Timestamp.now()

    this_folder = os.path.dirname(__file__)
    path = os.path.join(this_folder, 'data.db')

    with sqlite3.connect(path) as db:
        df.to_sql('data', db, if_exists='append')


if __name__ == '__main__':
    main()
