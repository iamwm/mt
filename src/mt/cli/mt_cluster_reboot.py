import click as click

from mt.conf.parser import refresh_global_config, global_config, refresh_mongo_cmd_lines
from mt.core.connector import ShardingCluster
from mt.operation.reboot.cluster import save_cmd_lines_of_shards, reboot_cluster_shards


@click.command()
@click.option("--conf", '-f', type=str, default='./mt.yaml', help="mt yaml config path", required=True)
def cluster_reboot(conf: str):
    """
    reboot mongo cluster
    """
    refresh_global_config(conf)

    mongo_uri = global_config.get('mongo_cluster_config', {}).get('mongo_uri')
    if not mongo_uri:
        print('no available mongo uri')
        exit(1)
    c = ShardingCluster(mongo_uri)
    save_cmd_lines_of_shards(c)

    cmd_save_path = global_config.get('cmd_save_path')
    refresh_mongo_cmd_lines(cmd_save_path)

    reboot_cluster_shards(c)


if __name__ == '__main__':
    cluster_reboot()
