package mapreduceCustomGroup;

import mapreduceCustomSort.StudentBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
* 自定义分组
*    继承 WritableComparator类
*    重写compare方法
* */
public class MyGroup  extends WritableComparator {
     public MyGroup(){
         super(StudentBean.class,true);
     }
    //运行的过程中  报空指针异常错误   原因不会自动创建WritableComparable a, WritableComparable b  两个对象
    //要求参数都是  WritableComparable  自定义的类


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //定义分组条件
        StudentBean sb1 = (StudentBean) a;
        StudentBean sb2 = (StudentBean) b;
        return sb1.getCourse().compareTo(sb2.getCourse());
    }
}
