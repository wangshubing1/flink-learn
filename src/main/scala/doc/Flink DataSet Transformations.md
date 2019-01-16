### Map
* Map转换在DataSet的每个元素上应用用户定义的map函数。它实现了一对一的映射，也就是说，函数必须返回一个元素。

以下代码将Integer对的DataSet转换为Integers的DataSet：
```scala
val intPairs: DataSet[(Int, Int)] = // [...]
val intSums = intPairs.map { pair => pair._1 + pair._2 }
```
### FlatMap
* FlatMap转换在DataSet的每个元素上应用用户定义的平面映射函数。map函数的这种变体可以为每个输入元素返回任意多个结果元素（包括none）。

以下代码将文本行的DataSet转换为单词的DataSet：

```scala
val textLines: DataSet[String] = // [...]
val words = textLines.flatMap { _.split(" ") }
```
### MapPartition
* MapPartition在单个函数调用中转换并行分区。map-partition函数将分区作为Iterable获取，并且可以生成任意数量的结果值。每个分区中的元素数量取决于并行度和先前的操作。

以下代码将文本行的DataSet转换为每个分区的计数数据集：

```scala
val textLines: DataSet[String] = // [...]
// Some is required because the return value must be a Collection.
// There is an implicit conversion from Option to a Collection.
val counts = texLines.mapPartition { in => Some(in.size) }
```
### Filter
* Filter转换在DataSet的每个元素上应用用户定义的过滤器函数，并仅保留函数返回的元素true。

以下代码从DataSet中删除所有小于零的整数：

```scala
val intNumbers: DataSet[Int] = // [...]
val naturalNumbers = intNumbers.filter { _ > 0 }
```
> 重要信息：系统假定该函数不会修改应用谓词的元素。违反此假设可能会导致错误的结果。
### Projection of Tuple DataSet
* Project转换删除或移动元组DataSet的Tuple字段。该project(int...)方法选择应由其索引保留的元组字段，并在输出元组中定义它们的顺序。

预测不需要定义用户功能。

以下代码显示了在DataSet上应用项目转换的不同方法：
```java
DataSet<Tuple3<Integer, Double, String>> in = // [...]
// converts Tuple3<Integer, Double, String> into Tuple2<String, Integer>
DataSet<Tuple2<String, Integer>> out = in.project(2,0);
```
> scala 不支持
#### Projection with Type Hint
> 请注意，Java编译器无法推断project运算符的返回类型。如果您对运算符的结果调用另一个运算符，则可能会导致问题，project例如：

```java
DataSet<Tuple5<String,String,String,String,String>> ds = ....
DataSet<Tuple1<String>> ds2 = ds.project(0).distinct(0);
```
通过提示返回类型的project运算符可以克服此问题，如下所示：

```java
DataSet<Tuple1<String>> ds2 = ds.<Tuple1<String>>project(0).distinct(0);

```
### 分组数据集的转换
reduce操作可以对分组数据集进行操作。指定用于分组的密钥可以通过多种方式完成：

* 关键表达
* 键选择器功能
* 一个或多个字段位置键（仅限元组数据集）
* 案例类字段（仅限案例类）
请查看reduce示例以了解如何指定分组键。

### 减少分组数据集
* 应用于分组DataSet的Reduce转换使用用户定义的reduce函数将每个组减少为单个元素。对于每组输入元素，reduce函数连续地将元素对组合成一个元素，直到每个组只剩下一个元素。

> 请注意，对于ReduceFunction返回对象的键控字段，应与输入值匹配。这是因为reduce是可隐式组合的，并且从组合运算符发出的对象在传递给reduce运算符时再次按键分组。

#### 减少由键表达式分组的DataSet
键表达式指定DataSet的每个元素的一个或多个字段。每个键表达式都是公共字段的名称或getter方法。点可用于向下钻取对象。关键表达式“*”选择所有字段。以下代码显示如何使用键表达式对POJO DataSet进行分组，并使用reduce函数对其进行缩减。
```scala
// some ordinary POJO
class WC(val word: String, val count: Int) {
  def this() {
    this(null, -1)
  }
  // [...]
}

val words: DataSet[WC] = // [...]
val wordCounts = words.groupBy("word").reduce {
  (w1, w2) => new WC(w1.word, w1.count + w2.count)
}
```
#### 减少由KeySelector函数分组的DataSet
* 键选择器函数从DataSet的每个元素中提取键值。提取的键值用于对DataSet进行分组。以下代码显示如何使用键选择器函数对POJO DataSet进行分组，并使用reduce函数对其进行缩减。

```scala
// some ordinary POJO
class WC(val word: String, val count: Int) {
  def this() {
    this(null, -1)
  }
  // [...]
}

val words: DataSet[WC] = // [...]
val wordCounts = words.groupBy { _.word } reduce {
  (w1, w2) => new WC(w1.word, w1.count + w2.count)
}
```
#### 减少由字段位置键分组的DataSet（仅限元组数据集）
* 字段位置键指定用作分组键的元组数据集的一个或多个字段。以下代码显示如何使用字段位置键并应用reduce函数

```scala
val tuples = DataSet[(String, Int, Double)] = // [...]
// group on the first and second Tuple field
val reducedTuples = tuples.groupBy(0, 1).reduce { ... }
```
#### 按案例类字段分组的DataSet减少
* 使用Case Classes时，您还可以使用字段名称指定分组键：


```scala
case class MyClass(val a: String, b: Int, c: Double)
val tuples = DataSet[MyClass] = // [...]
// group on the first and second field
val reducedTuples = tuples.groupBy("a", "b").reduce { ... }
```
#### GroupReduce在分组数据集上
* 应用于分组DataSet的GroupReduce转换为每个组调用用户定义的group-reduce函数。这与Reduce之间的区别在于用户定义的函数会立即获得整个组。在组的所有元素上使用Iterable调用该函数，并且可以返回任意数量的结果元素。

* 由字段位置键分组的DataSet上的GroupReduce（仅限元组数据集）
以下代码显示如何从按Integer分组的DataSet中删除重复的字符串。

```scala
val input: DataSet[(Int, String)] = // [...]
val output = input.groupBy(0).reduceGroup {
      (in, out: Collector[(Int, String)]) =>
        in.toSet foreach (out.collect)
    }
```
#### 按键表达式，键选择器函数或案例类字段分组的DataSet上的GroupReduce
类似于Reduce转换中的键表达式， 键选择器函数和案例类字段的工作。

#### 对已排序的组进行GroupReduce
* group-reduce函数使用Iterable访问组的元素。可选地，Iterable可以按指定的顺序分发组的元素。在许多情况下，这可以帮助降低用户定义的组减少功能的复杂性并提高其效率。

下面的代码显示了如何删除由Integer分组并按String排序的DataSet中的重复字符串的另一个示例。


```scala
val input: DataSet[(Int, String)] = // [...]
val output = input.groupBy(0).sortGroup(1, Order.ASCENDING).reduceGroup {
      (in, out: Collector[(Int, String)]) =>
        var prev: (Int, String) = null
        for (t <- in) {
          if (prev == null || prev != t)
            out.collect(t)
            prev = t
        }
    }
```
> 注意：如果在reduce操作之前使用运算符的基于排序的执行策略建立分组，则GroupSort通常是免费的。

#### 可组合的GroupReduce函数
* 与reduce函数相比，group-reduce函数不是可隐式组合的。为了使组合 - 缩减功能可组合，它必须实现GroupCombineFunction接口。

> 要点：接口的通用输入和输出类型GroupCombineFunction必须等于GroupReduceFunction以下示例中所示的通用输入类型：

```scala
// Combinable GroupReduceFunction that computes two sums.
class MyCombinableGroupReducer
  extends GroupReduceFunction[(String, Int), String]
  with GroupCombineFunction[(String, Int), (String, Int)]
{
  override def reduce(
    in: java.lang.Iterable[(String, Int)],
    out: Collector[String]): Unit =
  {
    val r: (String, Int) =
      in.iterator.asScala.reduce( (a,b) => (a._1, a._2 + b._2) )
    // concat key and sum and emit
    out.collect (r._1 + "-" + r._2)
  }

  override def combine(
    in: java.lang.Iterable[(String, Int)],
    out: Collector[(String, Int)]): Unit =
  {
    val r: (String, Int) =
      in.iterator.asScala.reduce( (a,b) => (a._1, a._2 + b._2) )
    // emit tuple with key and sum
    out.collect(r)
  }
}

```
### GroupCombine在分组数据集上
* GroupCombine变换是可组合GroupReduceFunction中的组合步骤的一般形式。从某种意义上说，它允许将输入类型组合I到任意输出类型O。相反，GroupReduce中的组合步骤仅允许从输入类型I到输出类型的组合I。这是因为GroupReduceFunction中的reduce步骤需要输入类型I。

* 在一些应用中，期望在执行附加变换（例如，减小数据大小）之前将DataSet组合成中间格式。这可以通过CombineGroup转换以非常低的成本实现。

> 注意： Grouped DataSet上的GroupCombine是在内存中使用贪婪策略执行的，该策略可能不会一次处理所有数据，而是分多步处理。它也可以在各个分区上执行，而无需像GroupReduce转换那样进行数据交换。这可能会导致部分结果。

以下示例演示了如何将CombineGroup转换用于备用WordCount实现。
```scala

val input: DataSet[String] = [..] // The words received as input

val combinedWords: DataSet[(String, Int)] = input
  .groupBy(0)
  .combineGroup {
    (words, out: Collector[(String, Int)]) =>
        var key: String = null
        var count = 0

        for (word <- words) {
            key = word
            count += 1
        }
        out.collect((key, count))
}

val output: DataSet[(String, Int)] = combinedWords
  .groupBy(0)
  .reduceGroup {
    (words, out: Collector[(String, Int)]) =>
        var key: String = null
        var sum = 0

        for ((word, sum) <- words) {
            key = word
            sum += count
        }
        out.collect((key, sum))
}
```
上面的替代WordCount实现演示了GroupCombine在执行GroupReduce转换之前如何组合单词。上面的例子只是一个概念证明。注意，组合步骤如何更改DataSet的类型，这通常需要在执行GroupReduce之前进行额外的Map转换。

### 聚合在分组元组数据集上
有一些常用的聚合操作经常使用。Aggregate转换提供以下内置聚合函数：

* Sum,
* Min, and
* Max.
聚合转换只能应用于元组数据集，并且仅支持字段位置键进行分组。

以下代码显示如何对按字段位置键分组的DataSet应用聚合转换：
```scala
val input: DataSet[(Int, String, Double)] = // [...]
val output = input.groupBy(1).aggregate(SUM, 0).and(MIN, 2)
```
要在DataSet上应用多个聚合，必须.and()在第一个聚合之后使用该函数，这意味着.aggregate(SUM, 0).and(MIN, 2)生成字段0的总和和原始DataSet的字段2的最小值。与此相反，.aggregate(SUM, 0).aggregate(MIN, 2)将在聚合上应用聚合。在给定的示例中，在计算由字段1分组的字段0的总和之后，它将产生字段2的最小值。

> 注意：将来会扩展聚合函数集。

### MinBy / MaxBy在Grouped Tuple DataSet上
8 MinBy（MaxBy）转换为每组元组选择一个元组。选定的元组是一个元组，其一个或多个指定字段的值最小（最大）。用于比较的字段必须是有效的关键字段，即可比较。如果多个元组具有最小（最大）字段值，则返回这些元组的任意元组。

下面的代码显示了如何选择具有最小值的元组，每个元组的字段Integer和Double字段具有相同的String值DataSet<Tuple3<Integer, String, Double>>：

```scala
val input: DataSet[(Int, String, Double)] = // [...]
val output: DataSet[(Int, String, Double)] = input
                                   .groupBy(1)  // group DataSet on second field
                                   .minBy(0, 2) // select tuple with minimum values for first and third field.
```
### 减少完整的DataSet
* Reduce转换将用户定义的reduce函数应用于DataSet的所有元素。reduce函数随后将元素对组合成一个元素，直到只剩下一个元素。

以下代码显示了如何对Integer DataSet的所有元素求和：

```scala
val intNumbers = env.fromElements(1,2,3)
val sum = intNumbers.reduce (_ + _)
```
使用Reduce转换减少完整的DataSet意味着最终的Reduce操作不能并行完成。但是，reduce函数可以自动组合，因此Reduce转换不会限制大多数用例的可伸缩性。

### 完整DataSet上的GroupReduce
* GroupReduce转换在DataSet的所有元素上应用用户定义的group-reduce函数。group-reduce可以迭代DataSet的所有元素并返回任意数量的结果元素。

以下示例显示如何在完整DataSet上应用GroupReduce转换：
```scala
val input: DataSet[Int] = // [...]
val output = input.reduceGroup(new MyGroupReducer())
```
> 注意：如果group-reduce函数不可组合，则无法并行完成对完整DataSet的GroupReduce转换。因此，这可能是计算密集型操作。请参阅上面的“可组合GroupReduceFunctions”一节，了解如何实现可组合的group-reduce功能。

### GroupCombine在完整的DataSet上
* 完整DataSet上的GroupCombine与分组DataSet上的GroupCombine类似。数据在所有节点上分区，然后以贪婪的方式组合（即，只有一次合并到存储器中的数据）。

### 在完整的Tuple DataSet上聚合
有一些常用的聚合操作经常使用。Aggregate转换提供以下内置聚合函数：

* Sum,
* Min, and
* Max.
聚合转换只能应用于元组数据集。

以下代码显示如何在完整DataSet上应用聚合转换：
```scala
val input: DataSet[(Int, String, Double)] = // [...]
val output = input.aggregate(SUM, 0).and(MIN, 2)
```
> 注意：扩展支持的聚合功能集在我们的路线图中。

### 完整的Tuple DataSet上的MinBy / MaxBy
* MinBy（MaxBy）转换从元组的DataSet中选择一个元组。选定的元组是一个元组，其一个或多个指定字段的值最小（最大）。用于比较的字段必须是有效的关键字段，即可比较。如果多个元组具有最小（最大）字段值，则返回这些元组的任意元组。

下面的代码演示如何选择与为最大值的元组Integer，并Double从一个领域DataSet<Tuple3<Integer, String, Double>>：
```scala
val input: DataSet[(Int, String, Double)] = // [...]
val output: DataSet[(Int, String, Double)] = input                          
                                   .maxBy(0, 2) // select tuple with maximum values for first and third field.
```