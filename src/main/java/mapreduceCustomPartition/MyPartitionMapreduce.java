package mapreduceCustomPartition;

import mapreduceCustomSort.StudentBean;
import mapreduceCustomSort.StudentMrSort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
* 需求：将学生成绩按科目输出到各个文件中
* */
public class MyPartitionMapreduce  {
    public static class MyMapper extends Mapper<LongWritable, Text, StudentBean, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] split = value.toString().split(",");
            StudentBean sb = new StudentBean(split[4],split[1],Integer.parseInt(split[3]));
            context.write(sb,NullWritable.get());
        }
    }

    public static class MyReduce extends Reducer<StudentBean,NullWritable,StudentBean,NullWritable>{
        //reduce程序不进行任何处理
        @Override
        protected void reduce(StudentBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                  for(NullWritable  i :values){
                      context.write(key,i);
                  }
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        System.setProperty("'HADOOP_USER_NAME","qyl");

        conf.set("fs.defaultFS","hdfs://qyl01:9000");
        try {
            //启动一个job  这里的mr任务叫做job   这个job作用  封装mr任务
            Job job = Job.getInstance(conf);
            job.setJarByClass(MyPartitionMapreduce.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReduce.class);

            //指定map输出的key value的类型
            job.setMapOutputKeyClass(StudentBean.class);
            job.setMapOutputValueClass(NullWritable.class);

            //指定reduce 输出的key  value的类型
            job.setOutputKeyClass(StudentBean.class);
            job.setOutputValueClass(NullWritable.class);

            job.setPartitionerClass(MyPartition.class);   //指定分区类

            job.setNumReduceTasks(3);  //设置reduce的个数，一般和分区的个数保持一致

            Path inpath = new Path("/mapreduce_demo/student/");
            FileInputFormat.addInputPath(job,inpath);

            Path outpath = new Path("/mapreduce_demo/partition_result");
            if(outpath.getFileSystem(conf).exists(outpath)){
                outpath.getFileSystem(conf).delete(outpath,true);
            }

            FileOutputFormat.setOutputPath(job,outpath);
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
