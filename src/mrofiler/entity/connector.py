from pymongo import MongoClient

from mrofiler.entity.extend_cmd import ExtendCmd
from mrofiler.errors.errors import MongoURIException


class Connector:

    def __init__(self, mongo_uri: str):
        if not mongo_uri.startswith('mongodb://'):
            raise MongoURIException()
        self.basic_connection = None
        self.mongo_uri = mongo_uri

    def init_connection(self):
        self.basic_connection = MongoClient(self.mongo_uri)
        info = self.basic_connection.server_info()
        print(info)
        extend_cmd_primary = ExtendCmd('192.168.20.120', 27001)
        extend_cmd_primary.get_primary_replication_info()
        extend_cmd_primary.get_slave_replication_info()


class ShardingConnector:

    def get_sharding_status(self):
        pass

    def get_sharding_members(self):
        pass


class ReplicateNodeConnector:

    def get_replication_members(self):
        pass

    def get_oplog_status(self):
        pass

    def get_member_role_status(self):
        pass


class StandaloneConnector:
    def get_basic_info(self):
        pass

    def get_database_info(self):
        pass

    def get_collection_info(self):
        pass


if __name__ == '__main__':
    c = Connector("mongodb://192.168.20.120:27001,192.168.20.170:27001,192.168.20.183:27001")
    c.init_connection()
