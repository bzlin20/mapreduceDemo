package mapreduceCustomPartition;

import mapreduceCustomSort.StudentBean;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/*
* 自定义分区，相当于把输出的文件拆分为几个分区文件，一个分区一个文件
* 1、继承Partitioner类
* 2、重写getPartition方法
* 本代码的分区字段为班级(枚举字段)
* */
public class MyPartition  extends Partitioner<StudentBean, NullWritable> {

    public int getPartition(StudentBean key, NullWritable nullWritable, int i) {
            if(key.getCourse().equals("IS")){
                return 0;
            }
            if(key.getCourse().equals("MA")){
                return 1;
            }
            if(key.getCourse().equals("CS")){
                return  2;
            } else{
                return 3;

        }
    }
}
