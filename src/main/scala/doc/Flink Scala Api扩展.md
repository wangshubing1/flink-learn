* 1.Flink Scala Api在默认不启用，使用类似Java的写法，如果需要使用Scala的写法，则需要扩展
* 2. 为了在Scala和Java api之间保持相当的一致性，一些允许Scala高表达性的特性已经从标准api中被忽略了，它们都是批量和流的。
* 3. 如果您希望享受完整的Scala体验，您可以选择通过隐式转换增强Scala API的扩展，要使用所有可用的扩展

#### 只需为DataSet/DataStrea API添加一个简单的导入
##### DataSet API :
```scala
import org.apache.flink.api.scala.extensions._
```
#### DataStream API:
```scala
import org.apache.flink.streaming.api.scala.extensions._
```
> 注意！#Scala Api中经常会在编译期出现类型的检查异常,还需要导入
```scala
import org.apache.flink.api.scala._
//OR:
import org.apache.flink.api.scala.createTypeInformation
```

```scala
import org.apache.flink.api.scala._
/**
  * @Author: king
  * @Datetime: 2018/10/15
  * @Desc: TODO
  *
  */
object Main {

  case class Point(x: Double, y: Double)

  import org.apache.flink.api.scala.extensions._

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
    ds.filterWith {
      case Point(x, _) => x > 1
    }.reduceWith {
      case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
    }.mapWith {
      case Point(x, y) => (x, y)
    }.flatMapWith {
      case (x, y) => Seq("x" -> x, "y" -> y)
    }.groupingBy {
      case (id, value) => id
    }

  }
}
```