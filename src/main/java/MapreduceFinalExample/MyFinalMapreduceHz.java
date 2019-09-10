package MapreduceFinalExample;


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

//           班级   学号    姓名    语文 数学  英语
//  数据格式 1303	3001 	谢雨泽	95	  96	98
/*
* 需求：求每个班级总分最高的前5个学生，不同班级的结果输出到不同的文件中

排序字段：班级，总分
分组字段：班级
分区字段：班级
* */
public class MyFinalMapreduceHz {
    public static class HzMapper extends Mapper<LongWritable, Text,StudentScore,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] split = value.toString().split("\t");
            int sumscore= 0;
            sumscore=Integer.parseInt(split[3])+Integer.parseInt(split[4])+Integer.parseInt(split[5]);
            StudentScore ss= new StudentScore(split[0],sumscore);
            String  ElseInformation =  split[1]+"\t"+split[2];
            context.write(ss,new Text(ElseInformation));
        }
    }

    public static class HzReduce extends Reducer<StudentScore,Text,StudentScore,Text>{
        @Override
        protected void reduce(StudentScore key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int  i=0;
            for (Text t : values){
                i++;
                if(i<=1) {
                    context.write(key, t);
                }
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
            job.setJarByClass(MyFinalMapreduceHz.class);
            job.setMapperClass(HzMapper.class);
            job.setReducerClass(HzReduce.class);

            //指定map输出的key value的类型
            job.setMapOutputKeyClass(StudentScore.class);
            job.setMapOutputValueClass(Text.class);

            //指定reduce 输出的key  value的类型
            job.setOutputKeyClass(StudentScore.class);
            job.setOutputValueClass(Text.class);

            //指定分组规则
            job.setGroupingComparatorClass(MyScoreGroup.class);

            //指定分区
            job.setPartitionerClass(MyScorePartition.class);
            job.setNumReduceTasks(5);


            Path inpath = new Path("/mapreduce_demo/score/");
            FileInputFormat.addInputPath(job,inpath);


            Path outpath = new Path("/mapreduce_demo/final_hz_result");
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
