* 对于流处理（DataStream），Flink相同提供了对迭代的支持。这一节我们主要来分析流处理中的迭代，我们将会看到流处理中的迭代相较于批处理有类似之处。但差异也是十分之明显。

* 可迭代的流处理程序同意定义“步函数”（step function）并将其内嵌到一个可迭代的流（IterativeStream）中。由于一个流处理程序可能永不终止，因此不同于批处理中的迭代机制，流处理中无法设置迭代的最大次数。取而代之的是，你能够指定等待反馈输入的最大时间间隔（假设超过该时间间隔没有反馈元素到来。那么该迭代将会终止）。通过应用split或filter转换，你能够指定流的哪一部分用于反馈给迭代头，哪一部分分发给下游。这里我们以filter作为演示样例来展示可迭代的流处理程序的API使用模式。

1.  首先。基于输入流构建IterativeStream。这是一个迭代的起始。通常称之为迭代头：

```java 
IterativeStream<Integer> iteration = inputStream.iterate();
```

2. 接着。我们指定一系列的转换操作用于表述在迭代过程中运行的逻辑（这里简单以map转换作为演示样例）。map API所接受的UDF就是我们上文所说的步函数：

```java
DataStream<Integer> iteratedStream = iteration.map(/* this is executed many times */);
```

3. 然后。作为迭代我们肯定须要有数据反馈给迭代头进行反复计算，所以我们从迭代过的流中过滤出符合条件的元素组成的部分流，我们称之为反馈流：

```java
DataStream<Integer> feedbackStream = iteratedStream.filter(/* one part of the stream */);
```

4. 将反馈流反馈给迭代头就意味着一个迭代的完整逻辑的完毕，那么它就能够“关闭”这个闭合的“环”了。通过调用IterativeStream的closeWith这一实例方法能够关闭一个迭代（也可表述为定义了迭代尾）。传递给closeWith的数据流将会反馈给迭代头：

```java
iteration.closeWith(feedbackStream);
```

5. 另外，一个惯用的模式是过滤出须要继续向前分发的部分流，这个过滤转换事实上定义的是“终止迭代”的逻辑条件，符合条件的元素将被分发给下游而不用于进行下一次迭代：

```java
DataStream<Integer> output = iteratedStream.filter(/* some other part of the stream */);
```
### eg:

1. 首先，我们先通过source函数创建初始的流对象inputStream：

```java
DataStream<Tuple2<Integer, Integer>> inputStream = env.addSource(new RandomFibonacciSource());
```
该source函数会生成二元组序列，二元组的两个字段值是随机生成的作为斐波那契数列的初始值：

```java
private static class RandomFibonacciSource        
    implements SourceFunction<Tuple2<Integer, Integer>> {    

    private Random random = new Random();    
    private volatile boolean isRunning = true;    
    private int counter = 0;   

    public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {        
        while (isRunning && counter < MAX_RANDOM_VALUE) {            
            int first = random.nextInt(MAX_RANDOM_VALUE / 2 - 1) + 1;            
            int second = random.nextInt(MAX_RANDOM_VALUE / 2 -1) + 1;  

            if (first > second) continue;            

            ctx.collect(new Tuple2<Integer, Integer>(first, second));            
            counter++;            
            Thread.sleep(50);        
        }    
    }    

    public void cancel() {        
        isRunning = false;    
    }
}
```

2. 为了对新计算的斐波那契数列中的值以及累加的迭代次数进行存储，我们须要将二元组数据流转换为五元组数据流，并据此创建迭代对象：

```java
IterativeStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> iterativeStream =        
    inputStream.map(new TupleTransformMapFunction()).iterate(5000);
```
> * 注意上面代码段中iterate API的參数5000，不是指迭代5000次，而是等待反馈输入的最大时间间隔为5秒。
> * 流被觉得是无界的。所以无法像批处理迭代那样指定最大迭代次数。但它同意指定一个最大等待间隔，假设在给定的时间间隔里没有元素到来。那么将会终止迭代。

元组转换的map函数实现：

```java
private static class TupleTransformMapFunction extends RichMapFunction<Tuple2<Integer,        
    Integer>, Tuple5<Integer, Integer, Integer, Integer, Integer>> {    
    public Tuple5<Integer, Integer, Integer, Integer, Integer> map(            
        Tuple2<Integer, Integer> inputTuples) throws Exception {        
        return new Tuple5<Integer, Integer, Integer, Integer, Integer>(                
            inputTuples.f0,                
            inputTuples.f1,                
            inputTuples.f0,                
            inputTuples.f1,                
            0);    
    }
}
```
> 上面五元组中，当中索引为0。1这两个位置的元素，始终都是最初生成的两个元素不会变化，而后三个字段都会随着迭代而变化。

3. 在迭代流iterativeStream创建完毕之后，我们将基于它运行斐波那契数列的步函数并产生斐波那契数列流fibonacciStream：

```java
DataStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> fibonacciStream =        
    iterativeStream.map(new FibonacciCalcStepFunction());
```
> 这里的fibonacciStream仅仅是一个代称，当中的数据并非真正的斐波那契数列，事实上就是上面那个五元组。

当中用于计算斐波那契数列的步函数实现例如以下：

```java
private static class FibonacciCalcStepFunction extends        
    RichMapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>,        
    Tuple5<Integer, Integer, Integer, Integer, Integer>> {    
    public Tuple5<Integer, Integer, Integer, Integer, Integer> map(            
        Tuple5<Integer, Integer, Integer, Integer, Integer> inputTuple) throws Exception {        
        return new Tuple5<Integer, Integer, Integer, Integer, Integer>(                
            inputTuple.f0,                
            inputTuple.f1,                
            inputTuple.f3,                
            inputTuple.f2 + inputTuple.f3,                
            ++inputTuple.f4);    
    }
}
```
> * 正如上文所述。后三个字段会产生变化。在计算之前，数列最后一个元素会被保留。也就是f3相应的元素，然后通过f2元素加上f3元素会产生最新值并更新f3元素。而f4则会累加。

> * 随着迭代次数添加，不是整个数列都会被保留。仅仅有最初的两个元素和最新的两个元素会被保留，这里也不是必需保留整个数列，由于我们不须要完整的数列。我们仅仅须要对最新的两个元素进行推断就可以。

4. 每一个元素计算斐波那契数列的新值并产生了fibonacciStream，可是我们须要对最新的两个值进行推断。看它们是否超过了指定的阈值。超过了阈值的元组将会被输出，而没有超过的则会再次參与迭代。因此这将产生两个不同的分支。我们也为此构建了分支流：

```java
SplitStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> branchedStream =        
    fibonacciStream.split(new FibonacciOverflowSelector());
```
而对是否超过阈值的元组进行推断并分离的实现例如以下：
```java
private static class FibonacciOverflowSelector implements OutputSelector<
    Tuple5<Integer, Integer, Integer, Integer, Integer>> {    
    public Iterable<String> select(            
        Tuple5<Integer, Integer, Integer, Integer, Integer> inputTuple) {        
        if (inputTuple.f2 < OVERFLOW_THRESHOLD && inputTuple.f3 < OVERFLOW_THRESHOLD) {            
            return Collections.singleton(ITERATE_FLAG);        
        }        

        return Collections.singleton(OUTPUT_FLAG);    
    }
}
```
在筛选方法select中，我们对不同的分支以不同的常量标识符进行标识：ITERATE_FLAG（还要继续迭代）和OUTPUT_FLAG（直接输出）。

产生了分支流之后。我们就能够从中检出不同的流分支做迭代或者输出处理。

5. 对须要再次迭代的，就通过迭代流的closeWith方法反馈给迭代头：

```java
iterativeStream.closeWith(branchedStream.select(ITERATE_FLAG));
```

6. 而对于不须要的迭代就直接让其流向下游处理，这里我们仅仅是简单得将流“重构”了一下然后直接输出：

```java
DataStream<Tuple3<Integer, Integer, Integer>> outputStream = branchedStream        
    .select(OUTPUT_FLAG).map(new BuildOutputTupleMapFunction());
outputStream.print();
```
所谓的重构就是将之前的五元组又一次缩减为三元组，实现例如以下：

```java
private static class BuildOutputTupleMapFunction extends RichMapFunction<        
    Tuple5<Integer, Integer, Integer, Integer, Integer>,        
    Tuple3<Integer, Integer, Integer>> {    
    public Tuple3<Integer, Integer, Integer> map(Tuple5<Integer, Integer, Integer, Integer,            
        Integer> inputTuple) throws Exception {        
        return new Tuple3<Integer, Integer, Integer>(                
            inputTuple.f0,                
            inputTuple.f1,                
            inputTuple.f4);    
    }
}
```
完整的主干程序代码例如以下：

```java
public static void main(String[] args) throws Exception {    
    StreamExecutionEnvironment env = StreamExecutionEnvironment            
        .getExecutionEnvironment().setBufferTimeout(1);

    DataStream<Tuple2<Integer, Integer>> inputStream = env.addSource(new RandomFibonacciSource());    

    IterativeStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> iterativeStream =            
        inputStream.map(new TupleTransformMapFunction()).iterate(5000);    

    DataStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> fibonacciStream =            
        iterativeStream.map(new FibonacciCalcStepFunction());    

    SplitStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> branchedStream =            
        fibonacciStream.split(new FibonacciOverflowSelector());    

    iterativeStream.closeWith(branchedStream.select(ITERATE_FLAG));    

    DataStream<Tuple3<Integer, Integer, Integer>> outputStream = branchedStream            
        .select(OUTPUT_FLAG).map(new BuildOutputTupleMapFunction());    

    outputStream.print();    
    env.execute("Streaming Iteration Example");
}
```
### 输出
```shell
1> (2,3,3)
2> (3,4,2)
3> (2,4,2)
4> (1,3,3)
5> (2,4,2)
6> (2,3,3)
7> (4,4,2)
8> (2,3,3)
9> (2,4,2)
10> (2,3,3)

Process finished with exit code 0

```

