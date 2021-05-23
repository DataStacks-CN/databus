# FileSource2ScribeSink

读取本地文件的日志记录，通过 Scribe 协议发送给 ScribeServer。

## 适用条件

* 文件名称必须唯一。
* 不支持读取压缩格式的文件（二进制文件），只支持按行读取的文本文件。
* 不支持投递正在写入的文件，只支持投递写入完成的完整文件。

## 特性

* 正则是对应每种数据集的最小粒度，而不是文件夹。
* 文件投递的先后顺序可配置。
* 文件保留时间可配置。
* 文件读取完是否删除可配置。
* ScribeClient 的实例数可以横向扩展，从而提高日志的传输速率。
* ScribeServer 的服务状态可感知，ScribeClient 实现了故障转移策略。

## 配置文件示例
```shell
vim file-to-scribe-test.properties
pipeline.name=file-to-scribe-test

pipeline.source=com.weibo.dip.databus.source.FileSource
pipeline.converter=com.weibo.dip.databus.converter.TopicNameConverter
pipeline.store=com.weibo.dip.databus.store.DefaultStore
pipeline.sink=com.weibo.dip.databus.sink.ScribeSink

#source
source.file.directory=/data0/log/databus/test/
source.file.include.pattern=^.*\\.test\\.log$
source.file.category=test
source.file.delete.after.read=true
source.file.retention.second=7200

#sink
sink.scribe.server.hosts=hostname1,hostname2,hostname3,hostname4,hostname5
sink.scribe.server.port=1466
sink.scribe.thread.number=5
```


## 配置说明
* [FileSource](FileSourceProps.md)
* [ScribeSink](ScribeSinkProps.md)

