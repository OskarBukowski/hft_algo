import os
import argparse
import subprocess

path = '/home/obukowski/Desktop/hft_algo/db_ex_connections/'


def get_python_files():
    files = dict()
    for entry in os.listdir(path):
        if os.path.isdir(os.path.join(path, entry)):
            for f in os.listdir(f'{path}/{entry}'):
                if f.split(".")[1] == 'py':
                    files[f] = entry
    return files

print(get_python_files())

def parser():
    files = get_python_files()
    parser = argparse.ArgumentParser()
    parser.add_argument("--stop", action="store_true", help="stop all running processes")
    parser.add_argument("--start", action="store_true", help="start all processes")
    args = parser.parse_args()
    if args.stop:
        for k,v in files.items():
            subprocess.call(['sh', f'{path}stop.sh {k}'])

    elif args.start:
        for k,v in files.items():
            subprocess.call(['sh', f'{path}start.sh {k}'])

if __name__ == '__main__':
    parser()