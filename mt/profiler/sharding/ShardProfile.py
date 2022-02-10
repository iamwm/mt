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
        self.shard_cluster = shard_cluster
        self.shard_cluster.profile_databases()
        self.server_status = None

    def shard_status(self):
        """
        当前分片集群每个分片的运行基本状态，能否正常读写
        :return:
        """
        pass

    def analyze_database_of_shard(self) -> dict:
        """
        分析集群中所有数据库在集群分片的分布状态，提供分片状态
        :return:
        """
        database_profile = self.shard_cluster.database_profile
        database_on_shard_info = {}
        for info in database_profile:
            database_on_shard_info.update({info.database_name: info.size_on_shards})
        return database_on_shard_info

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

    def analyze_data_balance_of_shard(self) -> dict:
        """
        分析所有数据表在集群中的分布是否平衡
        主要分析已经分片的表是否在分片中分布均匀
        :return:
        """
        database_profile = self.shard_cluster.database_profile
        database_on_shard_info = {}
        for info in database_profile:
            database_name = info.database_name
            collection_of_database = info.collections
            for collection in collection_of_database:
                if collection.sharded:
                    sharding_detail = collection.sharding_detail
                    storage_distribution = {}
                    for sharding_name, storage_info in sharding_detail.items():
                        storage_size = storage_info.storage_size
                        if storage_size:
                            storage_distribution.update({sharding_name: storage_size})
                    if storage_distribution:
                        database_on_shard_info.update({f'{database_name}.{collection.name}': storage_distribution})
        return database_on_shard_info

    def analyze_active_session_of_shard(self) -> int:
        """
        分析集群active session
        :return:
        """
        self.server_status = self.shard_cluster.refresh_cluster_server_status()  # refresh shard cluster status
        active_session_count = self.server_status.get('logicalSessionRecordCache', {}).get('activeSessionsCount')
        if not active_session_count:
            return -1
        try:
            return int(active_session_count)
        except Exception:
            return -1


if __name__ == '__main__':
    c = ShardingCluster("mongodb://192.168.20.120:27010,192.168.20.170:27010,192.168.20.183:27010")
    p = ShardProfile(c)
    database_on_shard_info = p.analyze_database_of_shard()
    data_on_shard_info = p.analyze_data_balance_of_shard()
    session_info_on_shard = p.analyze_active_session_of_shard()
