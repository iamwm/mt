from mrofiler.core.connector import ShardingCluster


class ReplicationProfile:
    """
    分析指定shard cluster相关的分片副本集
    1. 副本集成员状态
    2. replication lag
    3. last election
    4. 成员角色分布
    5. 成员负载
    """

    def __init__(self, shard_cluster: ShardingCluster):
        pass

    def replication_status(self):
        """
        副本集整体状态和成员状态
        :return:
        """
        pass

    def analyze_oplog_lag(self):
        """
        副本集从节点oplog delay
        :return:
        """
        pass

    def analyze_last_selection(self):
        """
        副本集主节点选举时间
        :return:
        """
        pass

    def analyze_member_role(self):
        """
        根据副本集成员priority和state是否匹配
        :return:
        """
        pass
