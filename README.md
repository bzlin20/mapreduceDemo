# mapreduceDemo
mapreduce程序，包括了自定义排序，自定义分区，分组的例子程序，方便学习  

**自定义combiner:**     
  减少reduce端的数据量   提升性能      
       在实际应用中一般很少去加上combiner组件，没有特殊需求，我们最好别加上  
    1）继承Reducer   前两个=后两个  
	2）重写reduce  
	3)job中设置   job.setCombinerClass()  
**自定义排序类**        
   1、实现WritableComparable类    
   2、重写 write和 readFields方法    
   3、重写compareTo  (写的是排序规则)    
   4、job设置：     
**自定义分组类：**
  （默认是按照key的字典进行分组）        
    1）继承 WritableComparator  
    2）重写compare()  
    3)写构造方法   调用父类的构造方法  帮助我们构建参数对象  
    4）job设置    job.setGroupingComparatorClass()  
   只要分组：  一定写排序  
   排序  a  
   分组  b  
   sort：b   a  
   group b  
**自定义分区类:**
    相当于把输出的文件拆分为几个分区文件，一个分区一个文件  
   1、继承Partitioner类  
   2、重写getPartition方法  
   3、job设置：   job.setPartitionerClass(MyPartition.class);   //指定分区类  
                 job.setNumReduceTasks(3);  //设置reduce的个数，一般和分区的个数保持一致  



