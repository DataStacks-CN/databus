# KafkaSinkV010

KafkaSinkV010 用于把日志记录写入 Kafka 集群。适用于 Kafka0.10 及以上的版本。


## 特性

KafkaSinkV010 实现的实用特性：
* Kafka 生产者客户端的参数无需在 KafkaSinkV010 中声明，即可自动装载到程序中。这样在添加 Kafka 客户端配置参数时，无需修改程序代码，只需在配置文件中添加相应配置项。


## 配置说明
| 配置项 | 类型 | 默认值 | 重要性 | 描述 |
| :--- | :--- | :--- | :--- | :--- |
| sink.kafka.* |  |  |  | kafka 配置项前需添加前缀"sink.kafka."，具体配置项参考 Kafka 官网 |


## 示例
```shell
sink.kafka.bootstrap.servers=hostname1:9092,hostname2:9092,hostname3:9092
sink.kafka.key.serializer=org.apache.kafka.common.serialization.StringSerializer
sink.kafka.value.serializer=org.apache.kafka.common.serialization.StringSerializer
sink.kafka.compression.type=gzip
```