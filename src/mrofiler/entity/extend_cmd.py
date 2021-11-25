import subprocess
from typing import Optional
from json import loads

from mrofiler.errors.errors import ExtendCmdException


class ExtendCmd:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def convert_str_to_dict(self, origin_str: str) -> dict:
        # find the last {} structure
        origin_str = origin_str.replace('\n', '').replace('\t', '')
        bracket_stack = []
        starts = False
        end_index = start_index = None
        for x in range(len(origin_str) - 1, -1, -1):
            current_char = origin_str[x]
            if current_char in ['{', '}']:
                if starts:
                    if current_char == '{':
                        bracket_stack.pop()
                    else:
                        bracket_stack.append(current_char)
                    if not bracket_stack:
                        start_index = x
                        break
                else:
                    end_index = x
                    bracket_stack.append(current_char)
                    starts = True
        target_info_str = origin_str[start_index: end_index + 1]
        return loads(target_info_str)

    def get_primary_replication_info(self) -> Optional[dict]:
        """
        related link:https://docs.mongodb.com/manual/reference/method/db.getReplicationInfo/#mongodb-method-db.getReplicationInfo
        :return: getReplicationInfo runs on target mongo instance
        """
        cmd = f"mongo --host {self.host} --port {self.port} --eval 'db.getReplicationInfo()'"
        try:
            with subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True) as proc:
                output = proc.stdout.read()
                output_str = str(output, encoding="utf8")
                result = self.convert_str_to_dict(output_str)
                return result
        except Exception:
            raise ExtendCmdException()

    def get_slave_replication_info(self):
        """
               related link:https://docs.mongodb.com/manual/reference/method/db.printSecondaryReplicationInfo/#mongodb-method-db.printSecondaryReplicationInfo
               :return: printSecondaryReplicationInfo() runs on target mongo instance
               """
        cmd = f"mongo --host {self.host} --port {self.port} --eval 'db.printSlaveReplicationInfo()'"
        try:
            with subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True) as proc:
                output = proc.stdout.read()
                output_str = str(output, encoding="utf8")
                return output_str
        except Exception:
            raise ExtendCmdException()
