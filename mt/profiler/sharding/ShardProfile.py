from mt.core.connector import ShardingCluster


class ShardProfile:
    """
    分析指定shard cluster相关shard
    1. 分片健康状态
    2. 数据库在集群中的分布状态
    3. 数据表在集群中的分布状态
    4. 数据量/索引量/可回收磁盘空间状态
    5. 分片数据表数据balance状态
    6. 集群负载
    7. 连接信息
    8. active session状态
    """

    def __init__(self, shard_cluster: ShardingCluster):
        pass

    def shard_status(self):
        """
        当前分片集群每个分片的运行基本状态，能否正常读写
        :return:
        """
        pass

    def analyze_database_of_shard(self):
        """
        分析集群中所有数据库在集群分片的分布状态，提供分片状态
        :return:
        """
        pass

    def analyze_collection_of_shard(self):
        """
        分析集群中所有与数据库表的分布状态，提供分片建议
        :return:
        """
        pass

    def analyze_storage_of_shard(self):
        """
        分析集群中数据存储，包括总体存储、索引存储和可用空间
        :return:
        """
        pass

    def analyze_data_balance_of_shard(self):
        """
        分析所有数据表在集群中的分布是否平衡
        :return:
        """
        pass

    def analyze_active_session_of_shard(self):
        """
        分析集群active session
        :return:
        """
        pass
