from datetime import datetime
from typing import Dict, List

from pymongo import MongoClient

from mrofiler.core.extend_cmd import ExtendCmd, ReplicationStatus, ReplicationConf, PrimaryOplogInfo, OplogDiffInfo
from mrofiler.errors.errors import MongoURIException, NotSharingException, NotReplicationException
from collections import namedtuple
from copy import deepcopy

Address = namedtuple('Address', ['ip', 'port'])


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

        self._config_server = None
        self._shards = {}

        self.init_sharding_cluster()

    @property
    def config_server(self):
        return self._config_server

    @property
    def shards(self):
        return self._shards

    @property
    def server_status(self):
        return self._server_status

    def refresh_cluster_server_status(self):
        server_status = self.basic_connection.get_database('test').command('serverStatus')
        return server_status

    def init_config_server(self):
        config_server_info = self.server_status.get('sharding', {})
        connection_str = config_server_info.get('configsvrConnectionString').replace('cfgset/', 'mongodb://')
        self._config_server = ConfigServer(connection_str)

    def init_sharding(self):
        config_database = self.basic_connection.get_database('config')
        for shard_info in config_database.get_collection('shards').find():
            host = shard_info.get('host')
            shard_id = shard_info.get('_id')
            shard_status = shard_info.get('state')
            connection_str = f'mongodb://{host.split("/")[1]}'
            if shard_status == 1:
                shard_server = Shard(connection_str)
            else:
                shard_server = None
            self._shards.update({shard_id: shard_server})

    def do_statistic(self):
        response = {}
        mongo_client = self.basic_connection
        for database_info in mongo_client.list_databases():
            target_db_name = database_info.get('name')
            target_db = self.basic_connection.get_database(target_db_name)
            all_names = target_db.list_collection_names()
            for collection in all_names:
                stats = target_db.command("collstats", collection)
                target_collection_info = CollectionInfo(stats)
                response.update({collection: target_collection_info})

        print(response)

    def init_sharding_cluster(self):
        # init config server
        self.init_config_server()
        # init shards
        self.init_sharding()
        # get statistics info of shard cluster
        self.do_statistic()


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
        self.replication_status: ReplicationStatus = None
        self.replication_conf: ReplicationConf = None
        self.oplog_info: PrimaryOplogInfo = None
        self.oplog_lag_info: OplogDiffInfo = None
        self.get_replication_status()
        self.get_oplog_status()
        self.replication_member_set = ReplicationMemberSet(self.replication_status, self.replication_conf,
                                                           self.oplog_info, self.oplog_lag_info)

    def get_oplog_status(self):
        mongos_host, mongos_port = self.primary_info
        mongos_cmd = ExtendCmd(mongos_host, mongos_port)
        self.oplog_info = mongos_cmd.get_primary_replication_info()
        self.oplog_lag_info = mongos_cmd.get_slave_replication_info()

    def get_replication_status(self):
        mongos_host, mongos_port = self.primary_info
        mongos_cmd = ExtendCmd(mongos_host, mongos_port)
        self.replication_status = mongos_cmd.get_replication_status()
        self.replication_conf = mongos_cmd.get_replication_conf()


class ReplicationMemberSet:
    def __init__(self, replication_status: ReplicationStatus, replication_conf: ReplicationConf,
                 oplog_info: PrimaryOplogInfo, oplog_lag_info: OplogDiffInfo):
        self.rs_status = replication_status.replication_status
        self.rs_conf = replication_conf.replication_conf
        self.oplog_info = oplog_info
        self.oplog_lag = oplog_lag_info.diff_info
        self.member_set = set()
        self.init_member_set()

    def init_member_set(self):
        set_name = self.rs_status.get('set')
        members_info_of_status = self.rs_status.get('members')
        members_info_of_conf = self.rs_conf.get('members')
        merged_member_info = []
        for index, member in enumerate(members_info_of_status):
            copy_of_member = deepcopy(member)
            copy_of_member.update(members_info_of_conf[index])
            merged_member_info.append(copy_of_member)
        for member in merged_member_info:
            name = set_name
            address = Address(*member.get('host').split(':'))
            role = member.get('stateStr')
            priority = member.get('priority')
            status = member.get('health')
            if role == 'PRIMARY':
                oplog_lag = None
                oplog_info = self.oplog_info
                last_election = datetime.strptime(member.get('electionTime'), '%Y-%m-%d %H:%M:%S')
            elif role == 'SECONDARY':
                oplog_lag = self.oplog_lag
                oplog_info = self.oplog_info
                last_election = None
            else:
                oplog_lag = None
                oplog_info = None
                last_election = None
            replication_member = ReplicationMember(name, address, role, priority, status, oplog_lag, oplog_info,
                                                   last_election)
            self.member_set.add(replication_member)


class ReplicationMember:
    __slots__ = ['name', 'address', 'role', 'priority', 'status', 'oplog_lag', 'oplog_info', 'last_election']

    def __init__(self,
                 name: str,
                 address: Address,
                 role: str,
                 priority: int,
                 status: str,
                 oplog_lag: OplogDiffInfo,
                 oplog_info: PrimaryOplogInfo,
                 last_election: datetime
                 ):
        self.name = name
        self.address = address
        self.role = role
        self.priority = priority
        self.status = status
        self.oplog_lag = oplog_lag
        self.oplog_info = oplog_info
        self.last_election = last_election


class ConfigServer(ReplicationSet):

    def __init__(self, mongo_uri: str):
        super(ConfigServer, self).__init__(mongo_uri)


class Shard(ReplicationSet):
    def __init__(self, mongo_uri: str):
        super(Shard, self).__init__(mongo_uri)


class BasicSizeInfo:
    __slots__ = ['count', 'reuse_size', 'total_index_size', 'index_size_detail', 'storage_size']

    def __init__(self, size_info: dict):
        for key, value in size_info.items():
            setattr(self, key, value)

    def __repr__(self):
        response = {}
        for x in self.__slots__:
            response.update({x: getattr(self, x)})
        return str(response)


class CollectionInfo:
    __slots__ = ["sharded", "capped", "basic_size_info", "sharding_detail"]

    def __init__(self, stats: dict, scale=1024 * 1024):
        self.sharded = stats.get('sharded')
        self.capped = stats.get('capped')
        self.basic_size_info = self.parse_size_info(stats, scale)
        sharding_info = stats.get('shards', {})
        self.sharding_detail = {}
        for shard_name, detail in sharding_info.items():
            sharding_size_info = self.parse_size_info(detail, scale)
            self.sharding_detail.update({shard_name: sharding_size_info})

    @staticmethod
    def parse_size_info(stats: dict, scale: int) -> BasicSizeInfo:
        response = {}
        response.update({'count': stats.get('count')})

        reuse_bytes = stats.get('wiredTiger').get('block-manager').get('file bytes available for reuse')
        reuse_mb = reuse_bytes // scale
        response.update({'reuse_size': reuse_mb})

        total_index_size = stats.get('totalIndexSize')
        index_mb = total_index_size // scale
        response.update({'total_index_size': index_mb})

        index_size_detail_origin = stats.get('indexSizes', {})
        index_size_detail = {}
        for _index_name, _index_size in index_size_detail_origin.items():
            index_size_detail.update({_index_name: _index_size // scale})
        response.update({'index_size_detail': index_size_detail})

        storage_size = stats.get('storageSize')
        data_mb = storage_size // scale
        response.update({'storage_size': data_mb})

        return BasicSizeInfo(response)

    def __repr__(self):
        response = {}
        for x in self.__slots__:
            response.update({x: getattr(self, x)})
        return str(response)


class DatabaseInfo:

    def __init__(self, database_name: str, sharding: bool, size_on_disk: int, size_on_shards: Dict[str, int],
                 collections: List[CollectionInfo]):
        pass


if __name__ == '__main__':
    c = ShardingCluster("mongodb://192.168.20.120:27010,192.168.20.170:27010,192.168.20.183:27010")
