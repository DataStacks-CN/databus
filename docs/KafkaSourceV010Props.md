# KafkaSourceV010

KafkaSourceV010 用于消费 Kafka 集群的日志。适用于 Kafka0.10 及以上的版本。


## 特性

KafkaSourceV010 的实用特性：
* Kafka 消费者客户端的参数无需在 KafkaSourceV010 中声明，即可自动装载到程序中。这样在添加 Kafka 客户端配置参数时，无需修改程序代码，只需在配置文件中添加相应配置项。


## 配置说明
| 配置项 | 类型 | 默认值 | 重要性 | 描述 |
| :--- | :--- | :--- | :--- | :--- |
| source.kafka.topic.and.threads | String |  | high | 配置待消费的 topic 名称及相应的消费线程数，每组 topic 使用`,`分隔，topic 名称和线程数使用`:`分隔。格式：topic1:num1,topic2:num2 |
| source.kafka.* |  |  |  | kafka 配置项前需添加前缀"source.kafka."，具体配置项参考 Kafka 官网 |


## 示例
```shell
source.kafka.bootstrap.servers=hostname1:9092,hostname2:9092,hostname3:9092
source.kafka.group.id=consumer_group_name
source.kafka.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
source.kafka.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
source.kafka.topic.and.threads=topic_name1:1,topic_name2:1
source.kafka.session.timeout.ms=30000
source.kafka.max.poll.records=200
source.kafka.auto.offset.reset=earliest
```
