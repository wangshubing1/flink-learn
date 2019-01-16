## Windows Join
窗口连接连接两个共享公共密钥并位于同一窗口中的流的元素。可以使用窗口分配器定义这些窗口，并对来自两个流的元素进行评估。

然后将来自双方的元素传递给用户定义的，JoinFunction或者FlatJoinFunction用户可以发出满足连接条件的结果。

一般用法可概括如下：

```java
stream.join(otherStream)
    .where(<KeySelector>)
    .equalTo(<KeySelector>)
    .window(<WindowAssigner>)
    .apply(<JoinFunction>)
```
关于语义的一些注释：

* 两个流的元素的成对组合的创建表现得像内部连接，意味着如果它们没有来自要连接的另一个流的对应元素，则不会发出来自一个流的元素。
* 那些加入的元素将在其时间戳中包含仍位于相应窗口中的最大时间戳。例如，[5, 10)具有其边界的窗口将导致连接的元素具有9作为其时间戳。
### Tumbling Window Join
当执行翻滚窗口连接时，具有公共密钥和公共翻滚窗口的所有元素以成对组合的形式连接并传递给JoinFunction或FlatJoinFunction。因为它的行为类似于内连接，所以不会发出一个流的元素，这些元素在其翻滚窗口中没有来自另一个流的元素！
![0f6bcb1b1a573678f3b49860567fecf3.svg+xml](evernotecid://5A90C59E-93AA-4AA3-A5FE-F344D5A0463E/appyinxiangcom/19148229/ENResource/p921)
如图所示，我们定义了一个大小为2毫秒的翻滚窗口，这导致了窗体的窗口[0,1], [2,3], ...。图像显示了每个窗口中所有元素的成对组合，这些元素将被传递给JoinFunction。请注意，在翻滚窗口中[6,7]没有任何东西被发射，因为绿色流中不存在与橙色元素⑥和⑦连接的元素。
```java
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
 
...

DataStream<Integer> orangeStream = ...
DataStream<Integer> greenStream = ...

orangeStream.join(greenStream)
    .where(<KeySelector>)
    .equalTo(<KeySelector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(2)))
    .apply (new JoinFunction<Integer, Integer, String> (){
        @Override
        public String join(Integer first, Integer second) {
            return first + "," + second;
        }
    });
```
### Sliding Window Join
执行滑动窗口连接时，具有公共键和公共滑动窗口的所有元素都是成对组合并传递给JoinFunction或FlatJoinFunction。不会释放当前滑动窗口中没有来自其他流的元素的一个流的元素！请注意，某些元素可能在一个滑动窗口中连接而在另一个滑动窗口中不连![54700307dca47f8e86fae6e11ddd1890.svg+xml](evernotecid://5A90C59E-93AA-4AA3-A5FE-F344D5A0463E/appyinxiangcom/19148229/ENResource/p922)
在这个例子中，我们使用大小为2毫秒的滑动窗口并将它们滑动一毫秒，从而产生滑动窗口[-1, 0],[0,1],[1,2],[2,3], …。x轴下方的连接元素是传递给JoinFunction每个滑动窗口的元素。在这里，您还可以看到橙色②如何与窗口中的绿色③ [2,3]连接，但未与窗口中的任何内容连接[1,2]。
```java
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

...

DataStream<Integer> orangeStream = ...
DataStream<Integer> greenStream = ...

orangeStream.join(greenStream)
    .where(<KeySelector>)
    .equalTo(<KeySelector>)
    .window(SlidingEventTimeWindows.of(Time.milliseconds(2) /* size */, Time.milliseconds(1) /* slide */))
    .apply (new JoinFunction<Integer, Integer, String> (){
        @Override
        public String join(Integer first, Integer second) {
            return first + "," + second;
        }
    });
```
### Session Window Join
在执行会话窗口连接时，具有相同键的所有元素在“组合”满足会话条件时以成对组合方式连接并传递给JoinFunction或FlatJoinFunction。再次执行内连接，因此如果有一个会话窗口只包含来自一个流的元素，则不会发出任何输出！![b4f20c5464d886a85d5ec9eeb958d232.svg+xml](evernotecid://5A90C59E-93AA-4AA3-A5FE-F344D5A0463E/appyinxiangcom/19148229/ENResource/p923)
这里我们定义一个会话窗口连接，其中每个会话除以至少1ms的间隙。有三个会话，在前两个会话中，两个流的连接元素都传递给JoinFunction。在第三阶段，绿色流中没有元素，所以⑧和⑨没有连接！
```java

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
 
...

DataStream<Integer> orangeStream = ...
DataStream<Integer> greenStream = ...

orangeStream.join(greenStream)
    .where(<KeySelector>)
    .equalTo(<KeySelector>)
    .window(EventTimeSessionWindows.withGap(Time.milliseconds(1)))
    .apply (new JoinFunction<Integer, Integer, String> (){
        @Override
        public String join(Integer first, Integer second) {
            return first + "," + second;
        }
    });
```
### Interval Join
区间连接使用公共密钥连接两个流的元素（我们现在将它们称为A和B），并且流B的元素具有时间戳，该时间戳位于流A中元素的时间戳的相对时间间隔中。这也可以更正式地表达为 b.timestamp ∈ [a.timestamp + lowerBound; a.timestamp + upperBound]或 a.timestamp + lowerBound &lt;= b.timestamp &lt;= a.timestamp + upperBound其中a和b是共享公共密钥的A和B的元素。只要下限总是小于或等于上限，下限和上限都可以是负数或上限。间隔连接当前仅执行内连接。当一对元素传递给ProcessJoinFunction它们时，它们将被赋予ProcessJoinFunction.Context两个元素的更大的时间戳（可以通过它访问）。注意间隔连接当前仅支持事件时间。![3469214e182e37049fba314c58a7ed09.svg+xml](evernotecid://5A90C59E-93AA-4AA3-A5FE-F344D5A0463E/appyinxiangcom/19148229/ENResource/p924)
在上面的例子中，我们连接两个流'orange'和'green'，下限为-2毫秒，上限为+1毫秒。缺省情况下，这些界限是包容性的，但.lowerBoundExclusive()并.upperBoundExclusive可以应用到改变行为。再次使用更正式的表示法，这将转化为orangeElem.ts + lowerBound &lt;= greenElem.ts &lt;= orangeElem.ts + upperBound如三角形所示。
```java
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

...

DataStream<Integer> orangeStream = ...
DataStream<Integer> greenStream = ...

orangeStream
    .keyBy(<KeySelector>)
    .intervalJoin(greenStream.keyBy(<KeySelector>))
    .between(Time.milliseconds(-2), Time.milliseconds(1))
    .process (new ProcessJoinFunction<Integer, Integer, String(){

        @Override
        public void processElement(Integer left, Integer right, Context ctx, Collector<String> out) {
            out.collect(first + "," + second);
        }
    });
```