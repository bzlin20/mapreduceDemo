package MapreduceFinalExample;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyScorePartition extends Partitioner<StudentScore, Text> {
    public int getPartition(StudentScore key, Text ss, int i) {
        if(key.getClassname().equals("1303")){
            return 0 ;
        }
        if(key.getClassname().equals("1304")){
            return 1;
        }
        if(key.getClassname().equals("1305")){
            return 2;
        }
        if(key.getClassname().equals("1306")){
            return 3;
        }
        else{
            return 4;
        }
    }
}
