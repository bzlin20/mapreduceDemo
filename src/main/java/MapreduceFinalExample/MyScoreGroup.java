package MapreduceFinalExample;

import org.apache.hadoop.io.WritableComparator;
/*
* 分组字段：班级
 */

public class MyScoreGroup extends WritableComparator {
    public MyScoreGroup(){
        super(StudentScore.class,true);
    }

    @Override
    public int compare(Object a, Object b) {
        StudentScore  ss1 =(StudentScore)a;
        StudentScore  ss2 =(StudentScore)b;
        return ss1.getClassname().compareTo(ss2.getClassname());  //定义分组规则：按照班级进行分组
    }
}
