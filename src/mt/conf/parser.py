import json

import yaml

global_config = {}

mongo_cmd_lines = {}


def parse_yaml(file_path: str):
    with open(file_path, 'r') as f:
        return yaml.load(f, Loader=yaml.CLoader)


def parse_json(file_path: str):
    with open(file_path, 'r') as f:
        return json.load(f)


if __name__ == '__main__':
    # print(parse_yaml('./default.yaml'))
    print(parse_json('../operation/reboot/cmd.json'))
