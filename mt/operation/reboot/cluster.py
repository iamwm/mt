import os
from json import dump

from rich.progress import track

from mt.conf.parser import global_config
from mt.core.connector import ShardingCluster
from mt.operation.reboot import console
from mt.operation.reboot.common import success_style
from mt.operation.reboot.replication_set import save_cmd_lines, replication_reboot


def save_cmd_lines_of_shards(cluster: ShardingCluster):
    cmd_save_path = global_config.get('cmd_save_path')
    if os.path.exists(cmd_save_path):
        console.print('mongo start cmd exists, use old config', style='yellow')
        console.print(f'if you want use new config, remove current file:{cmd_save_path}', style='yellow')
        return
    replication_sets_mapper = cluster.shards
    all_cmd_lines = {}
    for _, shard in replication_sets_mapper.items():
        start_cmd = save_cmd_lines(shard)
        all_cmd_lines.update({shard.name: start_cmd})
    else:
        # saving info to disk file
        with open(cmd_save_path, 'w') as f:
            dump(all_cmd_lines, f)
            console.print('cmd to start sharding has been saved!', style=success_style)


def reboot_cluster_shards(cluster: ShardingCluster):
    replication_sets_mapper = cluster.shards
    for shard in track(list(replication_sets_mapper.values()), description=f"REBOOTING SHARD..."):
        replication_reboot(shard)


if __name__ == '__main__':
    c = ShardingCluster("mongodb://192.168.20.120:27010,192.168.20.170:27010,192.168.20.183:27010")
    save_cmd_lines_of_shards(c)
    reboot_cluster_shards(c)
