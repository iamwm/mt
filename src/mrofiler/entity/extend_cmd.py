import subprocess
from datetime import datetime
from json import loads
from typing import Optional

from dateutil import parser

from mrofiler.errors.errors import ExtendCmdException


def convert_timestamp_to_str(timestamp: datetime) -> str:
    timestamp_str = timestamp.astimezone().strftime('%Y-%m-%d %H:%M:%S')
    return f'"{timestamp_str}"'


class ShellJsonify:
    _special = {
        'ISODate(': 'find_iso_date_value',
        'NumberLong(': 'find_number_long_value',
        'Timestamp(': 'find_timestamp_value',
        'BinData(': 'find_binary_value',
        'ObjectId(': 'find_object_id_value'
    }

    @staticmethod
    def extract_target_value(origin_str: str, start_index: int) -> str:
        target_end_index = origin_str.index(')', start_index)
        target_special_value = origin_str[start_index: target_end_index + 1]
        return target_special_value

    def find_iso_date_value(self, origin_str: str, start_index: int):
        target_special_value = self.extract_target_value(origin_str, start_index)
        if target_special_value in self._replace_mapper:
            return
        time_str = target_special_value.split('"')[1]
        timestamp = parser.parse(time_str)
        converted_timestamp_str = convert_timestamp_to_str(timestamp)
        self._replace_mapper.update({target_special_value: converted_timestamp_str})

    def find_number_long_value(self, origin_str: str, start_index: int):
        target_special_value = self.extract_target_value(origin_str, start_index)
        if target_special_value in self._replace_mapper:
            return
        target_str = target_special_value[11:-1]
        self._replace_mapper.update({target_special_value: target_str})

    def find_timestamp_value(self, origin_str: str, start_index: int):
        target_special_value = self.extract_target_value(origin_str, start_index)
        if target_special_value in self._replace_mapper:
            return
        last_index = target_special_value.index(',')
        target_timestamp = target_special_value[10:last_index]
        timestamp = datetime.utcfromtimestamp(int(target_timestamp))
        converted_timestamp_str = convert_timestamp_to_str(timestamp)
        self._replace_mapper.update({target_special_value: converted_timestamp_str})

    def find_binary_value(self, origin_str: str, start_index: int):
        target_special_value = self.extract_target_value(origin_str, start_index)
        if target_special_value in self._replace_mapper:
            return
        self._replace_mapper.update({target_special_value: '"binary_data"'})

    def find_object_id_value(self, origin_str: str, start_index: int):
        target_special_value = self.extract_target_value(origin_str, start_index)
        if target_special_value in self._replace_mapper:
            return
        target_str = target_special_value[9:-1]
        self._replace_mapper.update({target_special_value: target_str})

    def __init__(self, shell_json_str: str):
        self._origin_str = shell_json_str
        self._special_str_index_bag = {}
        self._special_str_value_bag = {}
        self._replace_mapper = {}

    def reconstruct_json_string(self) -> str:
        try:
            for special in self._special:
                self._search_special_str_index(special, self._origin_str)
            self._search_special_str_value()
            self._replace_special_str()
            return self._origin_str
        except Exception as e:
            print(str(e))

    def _search_special_str_index(self, special_str: str, origin_str: str):
        start_index = 0
        while start_index < len(origin_str):
            try:
                target_index = origin_str.index(special_str, start_index)
                target_bag = self._special_str_index_bag.setdefault(special_str, [])
                target_bag.append(target_index)
                start_index = target_index + 1
            except ValueError:
                break

    def _search_special_str_value(self):
        for special_str, index_list in self._special_str_index_bag.items():
            target_convert_func_name = self._special.get(special_str)
            convert_func = getattr(self, target_convert_func_name)
            for index in index_list:
                convert_func(self._origin_str, index)

    def _replace_special_str(self):
        for origin_str, new_str in self._replace_mapper.items():
            self._origin_str = self._origin_str.replace(origin_str, new_str)


class PrimaryOplogInfo:

    def __init__(self, info: dict):
        self.max_log_size_mb = int(info.get('logSizeMB'))
        self.used_log_size_mb = int(info.get('usedMB'))
        self.max_hours_contained = int(info.get('timeDiffHours'))
        self.oplog_starts = parser.parse(info.get('tFirst'))
        self.oplog_ends = parser.parse(info.get('tLast'))


class OplogDiffInfo:

    def __init__(self, oplog_info: str):
        lines_of_info = oplog_info.split('\n')
        self.diff_info = []
        line_number = 0
        while line_number < len(lines_of_info):
            if lines_of_info[line_number].startswith('source:'):
                node_oplog_diff = {}
                node_info = lines_of_info[line_number].replace('source:', '').strip()
                node_oplog_diff.update({'source': node_info})
                line_number += 1
                last_sync_time = parser.parse(lines_of_info[line_number].replace('syncedTo:', '').strip())
                node_oplog_diff.update({'last_sync_time': last_sync_time})
                line_number += 1
                diff_seconds = lines_of_info[line_number].strip().split(' ')[0]
                node_oplog_diff.update({'diff_seconds': int(diff_seconds)})
                self.diff_info.append(node_oplog_diff)
            else:
                line_number += 1


class ReplicationStatus:
    def __init__(self, status_str):
        shell_jsonify = ShellJsonify(status_str)
        converted_status_str = shell_jsonify.reconstruct_json_string()
        self.replication_status = loads(converted_status_str)


class ExtendCmd:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def convert_str_to_dict(self, origin_str: str) -> dict:
        # find the last {} structure
        target_info_str = self.extract_information_from_str(origin_str)
        return loads(target_info_str)

    def extract_information_from_str(self, origin_str):
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
        return target_info_str

    def get_primary_replication_info(self) -> Optional[PrimaryOplogInfo]:
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
                return PrimaryOplogInfo(result)
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
                return OplogDiffInfo(output_str)
        except Exception:
            raise ExtendCmdException()

    def get_replication_status(self) -> dict:
        """
        related link:https://docs.mongodb.com/manual/reference/method/rs.status/#mongodb-method-rs.status
        :return: rs.status() runs on target mongo instance
        """
        cmd = f"mongo --host {self.host} --port {self.port} --eval 'rs.status()'"
        try:
            with subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True) as proc:
                output = proc.stdout.read()
                output_str = str(output, encoding="utf8")
                useful_str = self.extract_information_from_str(output_str)
                converted_status = ReplicationStatus(useful_str)
                return converted_status
        except Exception as e:
            print(str(e))
            raise ExtendCmdException()

    def get_sharding_status(self) -> dict:
        """
        related link: https://docs.mongodb.com/manual/reference/method/db.printShardingStatus/#mongodb-method-db.printShardingStatus
        :return: run db.printShardingStatus() command on target sharding cluster
        """
        cmd = f"mongo --host {self.host} --port {self.port} --eval 'db.printShardingStatus()'"
        try:
            with subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True) as proc:
                output = proc.stdout.read()
                output_str = str(output, encoding="utf8")
                result = self.convert_str_to_dict(output_str)
                return result
        except Exception:
            raise ExtendCmdException()
