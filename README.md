# mapreduceDemo
mapreduce程序，包括了自定义排序，自定义分区，分组的例子程序，方便学习

#mapreduce入门教程和学习技巧

1、MapReduce 入门

1.1、什么是 MapReduce hadoop 的四大组件： HDFS：分布式存储系统 MapReduce：分布式计算系统 YARN：hadoop 的资源调度系统 Common：以上三大组件的底层支撑组件，主要提供基础工具包和 RPC 框架等

MapReduce 是一个分布式运算程序的编程框架，是用户开发“基于 Hadoop 的数据分析应用”的核心框架

1.2、为什么需要 MapReduce

海量数据在单机上处理因为硬件资源限制，无法胜任 而一旦将单机版程序扩展到集群来分布式运行，将极大增加程序的复杂度和开发难度 引入 MapReduce 框架后，开发人员可以将绝大部分工作集中在业务逻辑的开发上，而将分布式计算中的复杂性交由框架来处理 Hadoop 当中的 MapReduce 就是这样的一个分布式程序运算框架，它把大量分布式程序都会 涉及的到的内容都封装进了，让用户只用专注自己的业务逻辑代码的开发。它对应以上问题 的整体结构如下：

MRAppMaster：MapReduce Application Master，分配任务，协调任务的运行 MapTask：阶段并发任，负责 mapper 阶段的任务处理 YARNChild ReduceTask：阶段汇总任务，负责 reducer 阶段的任务处理 YARNChild

2、MapReduce 程序的核心运行

2.1、概述 一个完整的 MapReduce 程序在分布式运行时有两类实例进程： 1、MRAppMaster：负责整个程序的过程调度及状态协调 2、Yarnchild：负责 map 阶段的整个数据处理流程 3、Yarnchild：负责 reduce 阶段的整个数据处理流程以上两个阶段 MapTask 和 ReduceTask 的进程都是 YarnChild，并不是说这 MapTask 和ReduceTask 就跑在同一个 YarnChild 进行里

2.2、MapReduce 程序的运行流程 1、一个 mr 程序启动的时候，最先启动的是 MRAppMaster，MRAppMaster 启动后根据本次job 的描述信息，计算出需要的 maptask 实例数量，然后向集群申请机器启动相应数量的 maptask 进程

2、 maptask 进程启动之后，根据给定的数据切片(哪个文件的哪个偏移量范围)范围进行数据处理，主体流程为： A、利用客户指定的 InputForm来获取 RecordReader 读取数据，形成输入 KV 对 B、将输入 KV 对传递给客户定义的 map()方法，做逻辑运算，并将 map()方法输出的 KV 对收 集到缓存 C、将缓存中的 KV 对按照 K分区排序后不断溢写到磁盘文件

3、 MRAppMaster 监控到所有 maptask 进程任务完成之后（真实情况是，某些 maptask 进程处理完成后，就会开始启动 reducetask 去已完成的 maptask 处 fetch 数据），会根据客指定的参数启动相应数量的 reducetask 进程，并告知 reducetask 进程要处理的数据范围（数据分区） 4、Reducetask 进程启动之后，根据 MRAppMaster 告知的待处理数据所在位置，从若干台maptask 运行所在机器上获取到若干个 maptask 输出结果文件，并在本地进行重新归并序，然后按照相同 key 的 KV 为一个组，调用客户定义的 reduce()方法进行逻辑运算，并收集运算输出的结果 KV，然后调用客户指定的 OutputFormat 将结果数据输出到外部存储

2.3、MapTask 并行度决定机制

maptask 的并行度决定 map 阶段的任务处理并发度，进而影响到整个 job 的处理速度那么，mapTask 并行实例是否越多越好呢？其并行度又是如何决定呢？一个 job 的 map 阶段并行度由客户端在提交 job 时决定，客户端对 map 阶段并行度的规划的基本逻辑为： 将待处理数据执行逻辑切片（即按照一个特定切片大小，将待处理数据划分成逻辑上的多个 split），然后每一个 split 分配一个 mapTask 并行实例处理

2.4、切片机制 FileInputFormat 中默认的切片机制 1、简单地按照文件的内容长度进行切片 2、切片大小，默认等于 block 大小 3、切片时不考虑数据集整体，而是逐个针对每一个文件单独切片 比如待处理数据有两个文件： File1.txt 200M File2.txt 100M 经过 getSplits()方法处理之后，形成的切片信息是： File1.txt-split1 0-128M File1.txt-split2

2.5、ReduceTask 并行度决定机制 reducetask 的并行度同样影响整个 job 的执行并发度和执行效率，但与 maptask 的并发数由切片数决定不同，Reducetask 数量的决定是可以直接手动设置：job.setNumReduceTasks(4);默认值是 1，手动设置为 4，表示运行 4 个 reduceTask，设置为 0，表示不运行 reduceTask 任务，也就是没有 reducer 阶段，只有 mapper 阶段 如果数据分布不均匀，就有可能在 reduce 阶段产生数据倾斜 注意：reducetask 数量并不是任意设置，还要考虑业务逻辑需求，有些情况下，需要计算全局汇总结果，就只能有 1 个 reducetask。尽量不要运行太多的 reducetask。对大多数 job 来说，最好 rduce 的个数最多和集群中的reduce 持平，或者比集群的 reduce slots 小。这个对于小集群而言，尤其重要。


