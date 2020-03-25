package topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Copyright (C), 2019-2020
 * Author: Administrator
 * Date: 2020/3/25 10:29
 * FileName: MainTopk
 * Description: main
 */
public class MainTopk {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /**
         *   利用本地数据进行跑结果
         * */
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);
        job.setJarByClass(MainTopk.class);
        job.setMapperClass(MyMapTop.class);
        job.setReducerClass(MyReduceTop.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path("src/data/in/httpdata.csv"));
        Path outpath = new Path("src/data/out/result");
        if(outpath.getFileSystem(conf).exists(outpath)){
            outpath.getFileSystem(conf).delete(outpath,true);
        }

        FileOutputFormat.setOutputPath(job,new Path("src/data/out/result"));
        job.waitForCompletion(true) ; //提交job
    }
}
