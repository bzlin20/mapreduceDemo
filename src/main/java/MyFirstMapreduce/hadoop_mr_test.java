package MyFirstMapreduce;
/*
 * mr 练习程序，统计wordcount,可以打成jar包放在集群里运行
 * */

/*
* 数据格式：
* hello world hi you hi me
  fauck you hit you meet
   to me hello his hi job
* */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class hadoop_mr_test {
    public static class MyMap extends Mapper<LongWritable, Text,Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields =value.toString().split(" ");
            for(String s: fields){
                context.write(new Text(s),new IntWritable(1));
            }
        }
    }
    public static class MyReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable i :values){
                count += i.get();
            }
            context.write(key,new IntWritable(count));
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        System.setProperty("'HADOOP_USER_NAME","qyl");

        conf.set("fs.defaultFS","hdfs://qyl01:9000");
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(hadoop_mr_test.class);
            job.setMapperClass(MyMap.class);
            job.setReducerClass(MyReduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);


            Path inpath =new Path("/mapreduce_demo");
            FileInputFormat.addInputPath(job,inpath);

            Path outpath = new Path("/mapreduce_demo/wordcount_result");
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
