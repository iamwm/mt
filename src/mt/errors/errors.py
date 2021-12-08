class MongoURIException(Exception):
    def __init__(self, message='Mongo uri is not good'):
        super(MongoURIException, self).__init__(message)


class NotSharingException(Exception):
    def __init__(self, message='this is not a sharding cluster'):
        super(NotSharingException, self).__init__(message)


class NotReplicationException(Exception):
    def __init__(self, message='this is not a replication'):
        super(NotReplicationException, self).__init__(message)


class ExtendCmdException(Exception):
    def __init__(self, message='Extend cmd failed'):
        super(ExtendCmdException, self).__init__(message)
