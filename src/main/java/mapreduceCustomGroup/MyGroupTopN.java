package mapreduceCustomGroup;

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
* 需求：根据学生的班级进行分组，求每个班级成绩最高的top3
* 相当于执行 select * from  (select  姓名，年龄，row_number() over(partition by 班级 order by 分数 desc) rk from table ) t  where rk<=3
* */
public class MyGroupTopN {
    public static class MyMapper extends Mapper<LongWritable, Text, StudentBean, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] split = value.toString().split(",");
            //数据格式  95002,刘晨,女,19,IS
            StudentBean sb = new StudentBean(split[4],split[1],Integer.parseInt(split[3]));
            context.write(sb,NullWritable.get());
        }
    }

    public static class MyReduce extends Reducer<StudentBean,NullWritable,StudentBean,NullWritable>{
        //默认情况下  当前的key指的是一组中的第一个key
        //关键问题就出在分组上
        //reduce方法是一个分组调用一次

        @Override
        protected void reduce(StudentBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
           int  i=0;
             for (NullWritable n : values) {
                     context.write(key, n);
                     if(++i == 3) return ;   //取前三

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
            job.setJarByClass(MyGroupTopN.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReduce.class);

            //指定map输出的key value的类型
            job.setMapOutputKeyClass(StudentBean.class);
            job.setMapOutputValueClass(NullWritable.class);

            //指定reduce 输出的key  value的类型
            job.setOutputKeyClass(StudentBean.class);
            job.setOutputValueClass(NullWritable.class);

            //指定分组规则
            job.setGroupingComparatorClass(MyGroup.class);


            Path inpath = new Path("/mapreduce_demo/student/");
            FileInputFormat.addInputPath(job,inpath);


            Path outpath = new Path("/mapreduce_demo/group_result");
            if(outpath.getFileSystem(conf).exists(outpath)){
                outpath.getFileSystem(conf).delete(outpath,true);
            }

            FileOutputFormat.setOutputPath(job,outpath);
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
/**
 * 结果
 *
 * CS      孙庆    23.0
 * CS      冯伟    21.0
 * CS      李勇    20.0
 * IS      赵钱    21.0
 * IS      刘晨    19.0
 * IS      张立    19.0
 * MA      王敏    22.0
 * MA      郑明    20.0
 * MA      易思玲  19.0
 *
 * */

}
