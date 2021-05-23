# TopicNameConverter.md

转换日志行的 Topic 名称。从 Source 端读取的日志行包含 Topic 字段，把 Topic 字段修改为新名称，再把日志行传输给 Sink 端。
适用于需要修改数据集名称的情况。


## 配置说明
| 配置项 | 类型 |  描述 |  
| :--- | :--- | :--- | 
| topic.mappings | String | 转换 topic 名称。每组 topic 使用`,`分隔，原 topic 名称和新 topic 名称使用`:`分隔 |


## 示例
```shell
topic.mappings=test1:test2,topic1:topic2
```