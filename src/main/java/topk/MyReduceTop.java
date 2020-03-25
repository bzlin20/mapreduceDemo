package topk;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Copyright (C), 2019-2020
 * Author: Administrator
 * Date: 2020/3/25 10:00
 * FileName: MyReduceTop
 * Description: reduce聚合，clean 求topn
 */
public class MyReduceTop  extends Reducer<Text,FlowBean,Text, LongWritable> {
    //利用TreeMap的排序功能，将FlowBean对象按总流量降序排序
    private Map<FlowBean,String> treemap =  new TreeMap<FlowBean,String>() ;
    private double globalFlow = 0 ; //全局流量计数器，初始值为0
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long up_sum =0 ;
        long down_sum =0;
        for(FlowBean bean : values){
            up_sum += bean.getUp_flow();
            down_sum += bean.getDown_flow();
        }
        globalFlow += (up_sum + down_sum);
        //将FlowBean对象按总流量降序排序
        treemap.put(new FlowBean("",up_sum,down_sum),key.toString());
    }
    //cleanup 方法是在reduce阶段推出前被调用一次
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      for(Map.Entry<FlowBean,String> item : treemap.entrySet()){
          context.write(new Text(item.getValue()),new LongWritable(item.getKey().getSum_flow())) ;

      }
    }
}
