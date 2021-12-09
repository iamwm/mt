import invoke
from fabric import Connection

from mt.conf.parser import global_config, mongo_cmd_lines
from mt.core.common import ReplicationRole
from mt.core.connector import ReplicationSet, ReplicationMember, Address
from mt.operation.reboot import console
from mt.operation.reboot.common import create_ssh_from_host_info, success_style, mongo_start_prefix


def get_ssh_connection_of_node(address: 'Address'):
    # find ssh info of current node
    ip, port = address.ip, address.port
    ssh_info = global_config.get('ssh_config', {})
    target_ssh_info = {}
    for host, host_info in ssh_info.items():
        if host_info.get('host') == ip:
            target_ssh_info = host_info
            break
    if not target_ssh_info:
        raise Exception('no ssh info available')
    ssh_connection = create_ssh_from_host_info(target_ssh_info)
    return ssh_connection


def save_cmd_lines(replication: ReplicationSet):
    replication_members = replication.replication_member_set.member_set
    # ssh to target host and save target shard start cmd line
    start_cmd_line_info = {}
    for member in replication_members:
        if member.status != 1:
            raise Exception(f'current replication node:{member.address} is not health, cmd line saving abort!')
        ssh_connection = get_ssh_connection_of_node(member.address)
        cmd_result = get_start_cmd_of_target_node(ssh_connection, member.name)
        start_cmd_line_info.update({member.address.ip: cmd_result})
    return start_cmd_line_info


def get_start_cmd_of_target_node(connection: Connection, shard_name: str):
    with connection as c:
        result = c.run(f'ps -ef | grep {shard_name}', hide=True)
        output = result.stdout
        lines = output.split('\n')
        target_cmd = ''
        for line in lines:
            if 'mongod' in line and shard_name in line:
                target_cmd = ' '.join(list(filter(lambda x: x, line.split(' ')))[7:])
                break
        return target_cmd


def replication_reboot(replication: ReplicationSet):
    replication_members = replication.replication_member_set.member_set
    # start rebooting current replication set
    # step1. rebooting secondaries
    replication_name = replication.name
    target_cmd_lines = mongo_cmd_lines.get(replication_name, {})
    if not target_cmd_lines:
        raise Exception('no mongo cmd saved!')
    secondary_nodes = filter(lambda x: x.role and x.role == ReplicationRole.SECONDARY.value, replication_members)
    for node in secondary_nodes:
        node_ip = node.address.ip
        node_start_cmd = target_cmd_lines.get(node_ip)
        if not node_start_cmd:
            raise Exception(
                f'no start cmd # current replication member:{node.address.ip}:{node.address.port} of '
                f'replication:{replication_name}')
        secondary_reboot(node, node_start_cmd)

    unhealthy_nodes = filter(lambda x: x.role and x.role == ReplicationRole.OFFLINE.value, replication_members)
    for node in unhealthy_nodes:
        node_ip = node.address.ip
        node_start_cmd = target_cmd_lines.get(node_ip)
        if not node_start_cmd:
            raise Exception(
                f'no start cmd # current replication member:{node.address.ip}:{node.address.port} of '
                f'replication:{replication_name}')
        unhealthy_node_reboot(node, node_start_cmd)

    primary_node = replication.replication_member_set.primary_node
    primary_reboot(primary_node, target_cmd_lines.get(primary_node.address.ip))


def primary_reboot(primary_node: ReplicationMember, start_cmd: str):
    ssh_connection = get_ssh_connection_of_node(primary_node.address)
    mongo_port = primary_node.address.port
    with ssh_connection as c:
        # step down
        step_down_cmd = f"mongo --port {mongo_port} --eval 'rs.stepDown()'"
        try:
            c.run(step_down_cmd, hide=True)
        except invoke.exceptions.UnexpectedExit as ie:
            console.print('rs stepdown!', style='bold')
        except Exception as e:
            raise e
        # shutdown mongod
        cmd = f"mongo --port {mongo_port} admin --eval 'db.shutdownServer()'"
        c.run(cmd, hide=True)
        # restart mongod with cmd line
        c.run(' && '.join(mongo_start_prefix + [start_cmd]), hide=True, replace_env=True)
    console.print(f'rebooted # {primary_node.role} member {primary_node.address.ip}:{primary_node.address.port} of'
                  f' replication:{primary_node.name}', style=success_style)


def secondary_reboot(secondary_node: ReplicationMember, start_cmd: str):
    ssh_connection = get_ssh_connection_of_node(secondary_node.address)
    mongo_port = secondary_node.address.port
    with ssh_connection as c:
        # shutdown mongod
        # if secondary node is not running, just start it
        cmd = f"mongo --port {mongo_port} admin --eval 'db.shutdownServer()'"
        c.run(cmd, hide=True)
        # restart mongod with cmd line
        c.run(' && '.join(mongo_start_prefix + [start_cmd]), hide=True, replace_env=True)

    console.print(
        f'rebooted # {secondary_node.role} member {secondary_node.address.ip}:{secondary_node.address.port} of'
        f' replication:{secondary_node.name}', style=success_style)


def unhealthy_node_reboot(node: ReplicationMember, start_cmd: str):
    console.print(f'current node:{node.address.ip}:{node.address.port} is not running', style='yellow')
    ssh_connection = get_ssh_connection_of_node(node.address)
    with ssh_connection as c:
        # just start this node
        console.print(f'just start this node', style='yellow bold')
        c.run(' && '.join(mongo_start_prefix + [start_cmd]), hide=True, replace_env=True)

    console.print(
        f'started # {node.role} member {node.address.ip}:{node.address.port} of replication:{node.name}',
        style=success_style)


if __name__ == '__main__':
    from mt.core.connector import ShardingCluster

    # c = ShardingCluster("mongodb://192.168.20.120:27010,192.168.20.170:27010,192.168.20.183:27010")
    r_c = ReplicationSet("mongodb://192.168.20.120:27001,192.168.20.170:27001,192.168.20.183:27001")
    replication_reboot(r_c)
