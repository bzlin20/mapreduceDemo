package mapreduceCustomSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
/*
* 学生成绩排序  map key自定义类
*
* 自定义排序一般很少单独使用，一版都是和自定义的分组一起使用
* */
public class StudentMrSort {
    public static class MyMapper extends Mapper<LongWritable, Text,StudentBean,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
              String [] split = value.toString().split(",");
            //数据格式  95002,刘晨,女,19,IS
              StudentBean sb = new StudentBean(split[4],split[1],Integer.parseInt(split[3]));
              context.write(sb,new Text(split[0]+"\t"+split[2]));
        }
    }
    public static class MyReduce extends Reducer<StudentBean,Text,StudentBean, Text>{
        @Override
        protected void reduce(StudentBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //一组调用一次   分组规则：   默认map输出的key相同的为一组  目前  map输出的key是一个自定义的类型
            //自定义的类型如何判断是否相同   按照你的排序规则进行判断是否相同
             System.out.println("--------------------开始执行reduce程序-----------------------");
             System.out.println(key);
             for (Text s :values){
                 context.write(key,s);
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
            job.setJarByClass(StudentMrSort.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReduce.class);

            //指定map输出的key value的类型
            job.setMapOutputKeyClass(StudentBean.class);
            job.setMapOutputValueClass(Text.class);

            //指定reduce 输出的key  value的类型
            job.setOutputKeyClass(StudentBean.class);
            job.setOutputValueClass(Text.class);

            Path inpath = new Path("/mapreduce_demo/student/");
            FileInputFormat.addInputPath(job,inpath);



            Path outpath = new Path("/mapreduce_demo/sort_result");
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
