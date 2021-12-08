from mt.conf.parser import global_config
from mt.core.connector import ShardingCluster
from mt.operation.reboot.replication_set import save_cmd_lines, replication_reboot
from json import dump


def save_cmd_lines_of_shards(cluster: ShardingCluster):
    replication_sets_mapper = cluster.shards
    all_cmd_lines = {}
    for _, shard in replication_sets_mapper.items():
        start_cmd = save_cmd_lines(shard)
        all_cmd_lines.update({shard.name: start_cmd})
    else:
        # saving info to disk file
        cmd_save_path = global_config.get('cmd_save_path')
        with open(cmd_save_path, 'w') as f:
            dump(all_cmd_lines, f)


def reboot_cluster_shards(cluster: ShardingCluster):
    replication_sets_mapper = cluster.shards
    for _, shard in replication_sets_mapper.items():
        replication_reboot(shard)


if __name__ == '__main__':
    c = ShardingCluster("mongodb://192.168.20.120:27010,192.168.20.170:27010,192.168.20.183:27010")
    # save_cmd_lines_of_shards(c)
    reboot_cluster_shards(c)
