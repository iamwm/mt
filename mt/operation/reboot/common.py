from fabric import Connection
from rich.style import Style

connection_container = []


def create_ssh_from_host_info(host_info: dict):
    return create_ssh_connection(host_info.get('host'), host_info.get('port'), host_info.get('user_name'),
                                 str(host_info.get('password')))


def create_ssh_connection(host: str, port: int, user_name: str, password: str) -> Connection:
    try:
        connection = Connection(host, user=user_name, port=port, connect_kwargs={'password': password})
    except Exception as e:
        print(e)
    else:
        return connection


danger_style = Style(color="red", blink=True, bold=True)
success_style = Style(color="green", bold=True)

mongo_start_prefix = [
    'ulimit -f unlimited',
    'ulimit -t unlimited',
    'ulimit -v unlimited',
    'ulimit -l unlimited',
    'ulimit -n 64000',
    'ulimit -m unlimited',
    'ulimit -u 64000'
]

if __name__ == '__main__':
    c = create_ssh_connection('192.168.20.120', 22, 'root', '123456')
    print(c.run('uname -s', hide=True))
