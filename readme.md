# mt

用于对指定mongo集群进行监控和运维。

现已完成功能：

- 集群基本情况
- 数据库概要分析
- 索引统计
- 分片统计
- 集群分片重启

## 使用说明

#### 打包

`python setup.py bdist_wheel` 生成whl包

#### 配置

基础配置描述当前mongo集群连接方式：

```yaml
mongo_cluster_config:
  mongo_uri: "mongodb://192.168.20.120:27010,192.168.20.170:27010,192.168.20.183:27010"
ssh_config:
  mongodb1:
    host: 192.168.20.120
    port: 22
    user_name: root
    password: 123456
  mongodb2:
    host: 192.168.20.170
    port: 22
    user_name: root
    password: 123456
  mongodb3:
    host: 192.168.20.183
    port: 22
    user_name: root
    password: 123456
  mongodb4:
    host: 192.168.20.133
    port: 22
    user_name: root
    password: 123456
  mongodb5:
    host: 192.168.20.153
    port: 22
    user_name: root
    password: 123456
cmd_save_path: './cmd.json'
```

字段说明：

mongo_cluster_config.mongo_uri：指定用于连接mongo集群的mongo uri

ssh_config.xxxx：指定当前mongo集群相关的虚拟机信息和ssh连接，包括ip、port、user_name和password

cmd_save_path: 工具初次运行获取当前集群所有节点启动脚本保存路径

#### 使用

将打包生成文件和配置文件远程拷贝到相关机器后：

##### install

运行命令安装`pip3 install --ignore-installed mt-0.0.1-py3-none-any.whl`

如遇到安装问题，请优先升级当前环境pip版本：` pip install --upgrade pip`

安装完成后会增加一个命令行工具**mt_reboot**

##### run

当前对外提供的工具，mt_reboot，只需要指定启动配置文件，即可完成当前环境mongo集群的重启工作

```shell
root@manugence:~/cluster_reboot# mt_reboot --help
Usage: mt_reboot [OPTIONS]

  reboot mongo cluster

Options:
  -f, --conf TEXT  mt yaml config path  [required]
  --help           Show this message and exit.
```
#### 注意事项

当前工具还处于开发状态中，后期会持续优化，当前阶段注意事项：

1. 初次运行mt_reboot时，要保证当前环境集群运行状态正常，因为要保存全量的节点运行命令；
2. 配置文件中ssh_config部分一定要把当前集群所有运行了mongo节点的远程信息写入。

## TODO:
1. 提供api获取指定集群信息
2. 提供规则过滤触发集群告警
3. 提供grafana模板，展示集群信息