### DataStream数据源




数据源是程序中读取输入的位置。你可以使用将附加源附加到程序
```angular2html
StreamExecutionEnvironment.addSource(sourceFunction)
//eg:
StreamExecutionEnvironment.addSource(kafkaConsumer)

```
Flink附带了许多预先实现的源函数，可以通过实现SourceFunction 非并行源，或通过实现ParallelSourceFunction接口或扩展 RichParallelSourceFunction并行源来编写自己的自定义源。

有几个预定义的流源可从以下位置访问StreamExecutionEnvironment：

##### 基于文件的：

readTextFile(path)- TextInputFormat逐行读取文本文件，即符合规范的文件，并将它们作为字符串返回。

readFile(fileInputFormat, path) - 按指定的文件输入格式指定读取（一次）文件。

readFile(fileInputFormat, path, watchType, interval, pathFilter) - 这是前两个内部调用的方法。它path根据给定的内容读取文件fileInputFormat。根据提供的内容watchType，此源可以定期监视（每intervalms）新数据（FileProcessingMode.PROCESS_CONTINUOUSLY）的路径，或者处理当前在路径中的数据并退出（FileProcessingMode.PROCESS_ONCE）。使用该pathFilter，用户可以进一步排除正在处理的文件。

###### 重要笔记：

* 如果watchType设置为FileProcessingMode.PROCESS_CONTINUOUSLY，则在修改文件时，将完全重新处理其内容。这可以打破“完全一次”的语义，因为在文件末尾追加数据将导致其所有内容被重新处理。

* 如果watchType设置为FileProcessingMode.PROCESS_ONCE，则源扫描路径一次并退出，而不等待读者完成读取文件内容。当然读者将继续阅读，直到读取所有文件内容。在该点之后关闭源将导致不再有检查点。这可能会导致节点发生故障后恢复速度变慢，因为作业将从上一个检查点恢复读取。

##### socket基础的：

socketTextStream - 从套接字读取。元素可以用分隔符分隔。
##### 基于集合：

fromCollection(Seq) - 从Java Java.util.Collection创建数据流。集合中的所有元素必须属于同一类型。

fromCollection(Iterator) - 从迭代器创建数据流。该类指定迭代器返回的元素的数据类型。

fromElements(elements: _*) - 从给定的对象序列创建数据流。所有对象必须属于同一类型。

fromParallelCollection(SplittableIterator) - 并行地从迭代器创建数据流。该类指定迭代器返回的元素的数据类型。

generateSequence(from, to) - 并行生成给定间隔中的数字序列。

##### 自定义：

addSource - 附加新的源功能。
例如，要从Apache Kafka中读取，您可以使用 addSource(new FlinkKafkaConsumer08<>(...))

### DataStream数据输出
数据接收器使用DataStream并将它们转发到文件，套接字，外部系统或打印它们。Flink带有各种内置输出格式，这些格式封装在DataStreams上的操作后面：

* writeAsText()/ TextOutputFormat- 按字符串顺序写入元素。通过调用每个元素的toString（）方法获得字符串。

* writeAsCsv(...)/ CsvOutputFormat- 将元组写为逗号分隔值文件。行和字段分隔符是可配置的。每个字段的值来自对象的toString（）方法。

* print()/ printToErr() - 在标准输出/标准错误流上打印每个元素的toString（）值。可选地，可以提供前缀（msg），其前缀为输出。这有助于区分不同的打印调用。如果并行度大于1，则输出也将与生成输出的任务的标识符一起添加。

* writeUsingOutputFormat()/ FileOutputFormat- 自定义文件输出的方法和基类。支持自定义对象到字节的转换。

* writeToSocket - 根据a将元素写入套接字 SerializationSchema

* addSink - 调用自定义接收器功能。Flink捆绑了其他系统（如Apache Kafka）的连接器，这些系统实现为接收器功能。

请注意，write*()方法DataStream主要用于调试目的。他们没有参与Flink的检查点，这意味着这些函数通常具有至少一次的语义。刷新到目标系统的数据取决于OutputFormat的实现。这意味着并非所有发送到OutputFormat的元素都会立即显示在目标系统中。此外，在失败的情况下，这些记录可能会丢失。

要将流可靠，准确地一次传送到文件系统，请使用flink-connector-filesystem。此外，通过该.addSink(...)方法的自定义实现可以参与Flink的精确一次语义检查点。

### 运行参数设置、
该StreamExecutionEnvironment包含ExecutionConfig允许为运行时设置工作的具体配置值。

* setAutoWatermarkInterval(long milliseconds)：设置自动水印发射的间隔。可以使用获取当前值 getAutoWatermarkInterval()

### 控制延迟

* java
```angular2html
LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
env.setBufferTimeout(timeoutMillis);

env.generateSequence(1,10).map(new MyMapper()).setBufferTimeout(timeoutMillis);
```
为了最大化吞吐量，设置setBufferTimeout(-1)哪个将删除超时和缓冲区只有在它们已满时才会被刷新。要最小化延迟，请将超时设置为接近0的值（例如5或10 ms）。应避免缓冲区超时为0，因为它可能导致严重的性能下降。
* scala 
```angular2html
val env: LocalStreamEnvironment = StreamExecutionEnvironment.createLocalEnvironment
env.setBufferTimeout(timeoutMillis)

env.generateSequence(1,10).map(myMap).setBufferTimeout(timeoutMillis)
```