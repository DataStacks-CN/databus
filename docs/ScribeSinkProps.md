# ScribeSink

ScribeSink 从 Source 端获取日志记录后，发送日志记录到 ScribeSever 端。

## 特性

ScribeSink 实现了很多方便实用的特性：
* ScribeClient 的实例数可以横向扩展，从而提高日志的传输速率。通过配置发送日志记录的线程数，每个线程会实例化一个 ScribeClient，若已有的 ScribeClient 网络传输速率较慢，可以增大线程数来扩展 ScribeClient 个数，进而提高网络传输速率。
* ScribeServer 的服务状态可感知，ScribeClient 实现了故障转移策略。若 ScribeClient 连接的 ScribeServer 发生故障，会重连配置中其他正常服务的 ScribeServer。
  极限情况下，所有 ScribeServer 不可用，ScribeClient 也不会异常退出，而是会不断重连，直到连接到正常工作的 ScribeServer 为止。


## 配置说明
| 配置项 | 类型 | 默认值 | 重要性 | 描述 |
| :--- | :--- | :--- | :--- | :--- |
| sink.scribe.server.hosts | String |  | high | ScribeServer 的服务器列表 |
| sink.scribe.server.port | int | 1466 | high | ScribeServer 的端口号 |
| sink.scribe.thread.number | int | 6 | high | 发送线程的个数 |
| sink.scribe.buffer.capacity | int | 1000 | medium | 阻塞队列的长度，表示阻塞队列最多存放多少条日志记录 |
| sink.scribe.batch.size | int | 100 | low | ScribeClient 的一个批次攒够多少条日志记录，或超过多长时间，便发送数据到ScribeServer |
| sink.scribe.max.send.interval | int | 1000 | low | ScribeClient 的一个批次超过多长时间，或攒够多少条日志记录，便发送数据到ScribeServer |
| sink.scribe.worker.sleep | long | 60000 | low | 若连接所有的 ScribeServer 均失败，那么线程 sleep 一段时间后重连 |
| sink.scribe.poll.timeout | int | 1000 | low | 从阻塞队列拉取元素的超时时间，单位：ms |
| sink.scribe.socket.timeout | int | 60000 | low | 连接 ScribeServer 的超时时间，单位：ms |
| sink.scribe.retry.time | int | 3 | low | 连接 ScribeServer 最多重试次数|


## 示例
```shell
sink.scribe.server.hosts=hostname1,hostname2,hostname3,hostname4,hostname5
sink.scribe.server.port=1466
sink.scribe.thread.number=5
```