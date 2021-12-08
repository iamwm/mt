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


# noinspection PyGlobalUndefined
def refresh_global_config(file_path: str):
    global_config.update(parse_yaml(file_path))


def refresh_mongo_cmd_lines(file_path: str):
    mongo_cmd_lines.update(parse_json(file_path))


if __name__ == '__main__':
    # print(parse_yaml('./default.yaml'))
    print(parse_json('../operation/reboot/cmd.json'))
