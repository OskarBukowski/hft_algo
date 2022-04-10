import os
import sys
import argparse
import subprocess
print(sys.path[0])

path = f'{sys.path[0]}/'


def get_python_files():
    files = dict()
    for entry in os.listdir(path):
        if os.path.isdir(os.path.join(path, entry)):
            for f in os.listdir(f'{path}/{entry}'):
                if f.split(".")[1] == 'py':
                    files[f] = entry
    return files


def parser():
    files = get_python_files()
    parser = argparse.ArgumentParser(description="Start/stop all exchanges using 'all' or do it with the single one using 'name' argument")
    parser.add_argument("exchange", type=str, help="exchange name if single, 'all' if the whole application")
    parser.add_argument("--start_ex", action="store_true", help="start all processes for exchange")
    parser.add_argument("--stop_ex", action="store_true", help="start all processes for exchange")
    args = parser.parse_args()
    if args.stop_ex:
        for k, v in files.items():
            if args.exchange == 'all':
                subprocess.call(["bash", f"{path}{v}/stop.sh", f"{path}{v}/{k}"])
            if v == args.exchange:
                subprocess.call(["bash", f"{path}{args.exchange}/stop.sh", f"{path}{args.exchange}/{k}"])

    elif args.start_ex:
        for k, v in files.items():
            if args.exchange == 'all':
                subprocess.call(["bash", f"{path}{v}/start.sh", f"{path}{v}/{k}"])
            if v == args.exchange:
                subprocess.call(["bash", f"{path}{args.exchange}/start.sh", f"{path}{args.exchange}/{k}"])


if __name__ == '__main__':
    parser()
