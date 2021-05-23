# FileSource

FileSource 负责监控某个磁盘目录下匹配正则的文件，把文件的每一行读取出来投递给 Sink。

## 特性

FileSource 实现了很多方便实用的特性：
* 正则是对应每种数据集的最小粒度，而不是文件夹。我们可以在一个文件夹下存放不同的数据集文件，每种数据集文件对应一个正则，在投递文件时，根据正则匹配出对应的数据集文件，实现了在同一个目录下多个数据集文件的投递互不干扰。
* 文件投递的先后顺序可配置。FileSource 目前提供了两种文件投递顺序的策略：优先投递旧文件和优先投递新文件，默认采用优先投递旧文件的策略。
* 文件保留时间可配置。对于某个数据集来说，超过一定时间的日志，不再具有分析价值。通过配置文件保留时间，可以只投递具有时效性的文件。
* 文件读取完是否删除可配置。无需额外的脚本控制文件的删除策略，在程序中即可通过配置来控制文件读取完是否需要删除。

## 适用条件

FileSource 具有一定的适用条件，这里约定：
* 不支持读取压缩格式的文件（二进制文件），只支持按行读取的文本文件。
* 文件名称必须唯一。若文件在投递过程中，新文件名称与正在传输的文件名称一样，新文件会把原文件覆盖，造成旧文件剩余部分未被读取，新文件开始部分未被读取。为避免上述问题，生产新文件时，可以给文件名称加上时间戳来确保名称唯一。
* 不支持投递正在写入的文件，只支持投递写入完成的完整文件。


## 配置说明

| 配置项 | 类型 | 默认值 | 重要性 | 描述 |
| :--- | :--- | :--- | :--- | :--- |
| source.file.directory | String | | high | 待投递文件所在目录 |
| source.file.category | String | | high | 数据集名称 |
| source.file.include.pattern | String | ^.*\\.log$ | high | 通过正则表达式来匹配需要投递的文件 |
| source.file.retention.second | int | 86400 | medium | 文件最长保存时间，超过该时间会被删除  |
| source.file.delete.after.read | boolean | false | medium | 文件读取完后是否要删除文件 |
| source.file.read.order | String | asc |medium | 读取文件顺序 |
| source.file.thread.number | int | 1 |low | 读取文件的线程数 |
| source.file.scan.interval.second | int | 30 | low | 扫描文件夹的时间间隔，获取未读的文件列表 |
| source.file.flush.init.delay.second | int | 60 | low | 调度线程首次把文件当前读取位置信息刷到磁盘的时间间隔 |
| source.file.flush.interval.second | int | 60 | low | 调度线程定时把文件当前读取位置信息刷到磁盘的时间间隔 |
| source.file.buffer.size | int | 1024 * 1024 | low | 读取一批日志申请的字节缓存空间大小 |
| source.file.line.buffer.size | int | 1024 | low | 每行日志申请的字节缓存空间大小 |


## 示例

```shell
source.file.directory=/data0/log/databus/test/
source.file.include.pattern=^.*\\.test\\.log$
source.file.category=test
source.file.delete.after.read=true
source.file.retention.second=7200
```
