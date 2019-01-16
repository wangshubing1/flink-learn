## 批量迭代
* 要创建BulkIteration，请调用iterate(int)迭代的DataSet方法。这将返回一个IterativeDataSet，可以使用常规运算符进行转换。迭代调用的单个参数指定最大迭代次数。

* 要指定迭代的结束，请调用closeWith(DataSet)方法IterativeDataSet以指定应将哪个转换反馈到下一次迭代。closeWith(DataSet, DataSet)如果此DataSet为空，您可以选择指定终止条件，该条件评估第二个DataSet并终止迭代。如果未指定终止条件，则迭代将在给定的最大数量迭代后终止。

以下示例迭代地估计数量Pi。目标是计算落入单位圆的随机点数。在每次迭代中，挑选一个随机点。如果此点位于单位圆内，我们会增加计数。然后估计Pi作为结果计数除以迭代次数乘以4。

```java
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// Create initial IterativeDataSet
IterativeDataSet<Integer> initial = env.fromElements(0).iterate(10000);

DataSet<Integer> iteration = initial.map(new MapFunction<Integer, Integer>() {
    @Override
    public Integer map(Integer i) throws Exception {
        double x = Math.random();
        double y = Math.random();

        return i + ((x * x + y * y < 1) ? 1 : 0);
    }
});

// Iteratively transform the IterativeDataSet
DataSet<Integer> count = initial.closeWith(iteration);

count.map(new MapFunction<Integer, Double>() {
    @Override
    public Double map(Integer count) throws Exception {
        return count / (double) 10000 * 4;
    }
}).print();

env.execute("Iterative Pi Example");
```

## Delta迭代
Delta迭代利用了某些算法在每次迭代中不会更改解决方案的每个数据点的事实。

除了在每次迭代中反馈的部分解决方案（称为工作集）之外，delta迭代还在迭代中维护状态（称为解决方案集），可以通过增量更新。迭代计算的结果是最后一次迭代之后的状态。有关delta迭代的基本原理的概述，请参阅迭代简介。

定义DeltaIteration类似于定义BulkIteration。对于delta迭代，两个数据集构成每次迭代的输入（工作集和解决方案集），并且在每次迭代中生成两个数据集作为结果（新工作集，解决方案集delta）。

创建DeltaIteration调用iterateDelta(DataSet, int, int)（或iterateDelta(DataSet, int, int[])分别）。在初始解决方案集上调用此方法。参数是初始增量集，最大迭代次数和关键位置。返回的 DeltaIteration对象使您可以通过方法iteration.getWorkset()和方式访问表示工作集和解决方案集的DataSet iteration.getSolutionSet()。

下面是delta迭代语法的示例

```java
// read the initial data sets
DataSet<Tuple2<Long, Double>> initialSolutionSet = // [...]

DataSet<Tuple2<Long, Double>> initialDeltaSet = // [...]

int maxIterations = 100;
int keyPosition = 0;

DeltaIteration<Tuple2<Long, Double>, Tuple2<Long, Double>> iteration = initialSolutionSet
    .iterateDelta(initialDeltaSet, maxIterations, keyPosition);

DataSet<Tuple2<Long, Double>> candidateUpdates = iteration.getWorkset()
    .groupBy(1)
    .reduceGroup(new ComputeCandidateChanges());

DataSet<Tuple2<Long, Double>> deltas = candidateUpdates
    .join(iteration.getSolutionSet())
    .where(0)
    .equalTo(0)
    .with(new CompareChangesToCurrent());

DataSet<Tuple2<Long, Double>> nextWorkset = deltas
    .filter(new FilterByThreshold());

iteration.closeWith(deltas, nextWorkset)
	.writeAsCsv(outputPath);
```