package topk;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Copyright (C), 2019-2020
 * Author: Administrator
 * Date: 2020/3/25 9:48
 * FileName: MyMapTop
 * Description: map函数
 */
 //数据格式：
 //   1363157993055  13560436666 C4-17-FE-BA-DE-D9:CMCC 120.196.100.99   18 15 1116 954 200
public class MyMapTop extends Mapper<LongWritable, Text,Text,FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String [] fields = value.toString().split(",");
        //拆分数据
        String phoneNB = fields[0];
        long up_flow = Long.parseLong(fields[7]) ;
        long down_flow = Long.parseLong(fields[8]);
        FlowBean fb= new FlowBean(phoneNB,up_flow,down_flow);
        context.write(new Text(phoneNB),fb);
    }
}
