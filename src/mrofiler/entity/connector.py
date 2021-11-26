from pymongo import MongoClient

from mrofiler.entity.extend_cmd import ExtendCmd
from mrofiler.errors.errors import MongoURIException, NotSharingException, NotReplicationException


class ShardingCluster:
    """
    try to connect to mongos of a mongo cluster
    """

    def __init__(self, mongo_uri: str):
        if not mongo_uri.startswith('mongodb://'):
            raise MongoURIException()
        self.basic_connection = MongoClient(mongo_uri)
        is_mongos = self.basic_connection.is_mongos
        if not is_mongos:
            raise NotSharingException()
        self._server_status = self.refresh_cluster_server_status()
        self.init_sharding()

    @property
    def config_server(self):
        pass

    @property
    def shards(self):
        pass

    @property
    def server_status(self):
        return self._server_status

    def refresh_cluster_server_status(self):
        server_status = self.basic_connection.get_database('test').command('serverStatus')
        return server_status

    def init_config_server(self):
        config_server_info = self.server_status.get('sharding', {})
        connection_str = config_server_info.get('configsvrConnectionString').replace('cfgset/', 'mongodb://')
        config_server = ConfigServer(connection_str)
        config_server.get_replication_status()
        config_server.get_oplog_info()

    def init_sharding(self):
        # init config server
        self.init_config_server()
        # init shards

        # pick one mongos to get more information
        # first_mongos_info = set(self.basic_connection.nodes).pop()
        # mongos_host, mongos_port = first_mongos_info
        # mongos_cmd = ExtendCmd(mongos_host, mongos_port)
        # mongos_cmd.get_sharding_status()


class ReplicationSet:
    def __init__(self, mongo_uri: str):
        if not mongo_uri.startswith('mongodb://'):
            raise MongoURIException()
        self.basic_connection = MongoClient(mongo_uri)
        is_mongos = self.basic_connection.is_mongos
        if is_mongos:
            raise NotReplicationException()
        self.name = self.basic_connection.topology_description.replica_set_name
        self.primary_info = self.basic_connection.primary
        self.secondaries_info = self.basic_connection.secondaries
        self.arbiters_info = self.basic_connection.arbiters

    def get_oplog_info(self):
        mongos_host, mongos_port = self.primary_info
        mongos_cmd = ExtendCmd(mongos_host, mongos_port)
        primary_oplog_info = mongos_cmd.get_primary_replication_info()
        oplog_diff_info = mongos_cmd.get_slave_replication_info()

    def get_replication_status(self):
        mongos_host, mongos_port = self.primary_info
        mongos_cmd = ExtendCmd(mongos_host, mongos_port)
        replication_status = mongos_cmd.get_replication_status()

    def get_election_info(self):
        pass


class ConfigServer(ReplicationSet):

    def __init__(self, mongo_uri: str):
        super(ConfigServer, self).__init__(mongo_uri)


class Shard(ReplicationSet):
    pass


if __name__ == '__main__':
    c = ShardingCluster("mongodb://192.168.20.120:27010,192.168.20.170:27010,192.168.20.183:27010")
    c.init_sharding()
