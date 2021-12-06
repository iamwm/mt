from mrofiler.core.common import convert_timestamp_to_str
from mrofiler.core.connector import ReplicationSet


class ReplicationProfile:
    """
    分析指定shard cluster相关的分片副本集
    1. 副本集成员状态
    2. replication lag
    3. last election
    4. 成员角色分布
    5. 成员负载
    """
    _important_keys = ['replication_name', 'refresh_time', 'running', 'max_oplog_size', 'current_oplog_size',
                       'max_hours_op_maintained', 'current_hours_op_maintained', 'oplog_lag_info',
                       'is_member_plays_well', 'last_election']

    def __init__(self, replication_set: ReplicationSet):
        self.replication_set = replication_set
        # init basic info
        self.replication_name = None
        self.refresh_time = None
        self.running = False
        # oplog info
        self.max_oplog_size = 0
        self.current_oplog_size = 0
        self.max_hours_op_maintained = 0
        self.current_hours_op_maintained = 0
        self.oplog_lag_info = []
        # member priority and role
        # return True if role and priority of all member matches
        self.is_member_plays_well = False
        # election info
        self.last_election = ''

    def analyze_replication_status(self):
        """
        副本集整体状态和成员状态
        :return:
        """
        origin_status = self.replication_set.replication_status.replication_status
        if not origin_status:
            raise Exception('replication info invalid')
        self.replication_name = origin_status.get('set')
        self.refresh_time = origin_status.get('date')
        self.running = True if origin_status.get('myState') == 1 else False

    def analyze_oplog_lag(self):
        """
        副本集从节点oplog delay
        :return:
        """
        origin_oplog_info = self.replication_set.oplog_info
        self.max_oplog_size = f'{origin_oplog_info.max_log_size_mb}MB'
        self.current_oplog_size = f'{origin_oplog_info.used_log_size_mb}MB'
        self.max_hours_op_maintained = f'{origin_oplog_info.max_hours_contained} hours'
        oplog_maintained = origin_oplog_info.oplog_ends - origin_oplog_info.oplog_starts
        self.current_hours_op_maintained = f'{oplog_maintained.total_seconds() // 3600} hours'
        origin_oplog_lag = self.replication_set.oplog_lag_info
        self.oplog_lag_info = origin_oplog_lag.diff_info

    def analyze_last_selection(self):
        """
        副本集主节点选举时间
        :return:
        """
        member_info = self.replication_set.replication_member_set
        current_primary_node = member_info.primary_node
        self.last_election = convert_timestamp_to_str(current_primary_node.last_election)

    def analyze_member_role(self):
        """
        根据副本集成员priority和state是否匹配
        暂时只判断主节点优先级是否为最高的节点
        :return:
        """
        member_info = self.replication_set.replication_member_set
        primary_node = member_info.primary_node
        primary_node_priority = primary_node.priority
        all_priority = [x.priority for x in member_info.member_set]
        max_priority = max(all_priority)
        self.is_member_plays_well = primary_node_priority == max_priority

    def analyze_summary(self) -> dict:
        self.analyze_replication_status()
        self.analyze_oplog_lag()
        self.analyze_last_selection()
        self.analyze_member_role()

        summary = {}
        for key in self._important_keys:
            summary.update({key: getattr(self, key)})
        return summary


if __name__ == '__main__':
    from mrofiler.core.connector import ShardingCluster

    c = ReplicationSet("mongodb://192.168.20.120:27011,192.168.20.170:27011,192.168.20.183:27011")
    profiler = ReplicationProfile(c)
    print(profiler.analyze_summary())
