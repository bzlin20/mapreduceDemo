package mapreduceCustomSort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//首先实现WritableComparable类
public class StudentBean  implements WritableComparable<StudentBean> {

    private String  course;
    private String  name;
    private Double avg;

    public String getCourse(){
        return course;
    }
    public String getName(){
        return name;
    }

    public void setName(String name){
        this.name = name;
    }

    public double getAvg(){
        return avg;
    }

    public void setAvg(double avg){
        this.avg = avg;
    }

    public StudentBean(){}
    public StudentBean(String course,String name,double avg){
        super();
        this.course = course;
        this.name = name;
        this.avg = avg;
    }

    public String toString(){
        return course +"\t"+name +"\t"+ avg;
    }

    //序列化字段
    public void write(DataOutput out) throws IOException {
        out.writeUTF(course);
        out.writeUTF(name);
        out.writeDouble(avg);
    }

    public void readFields(DataInput in) throws IOException {
        this.course = in.readUTF();
        this.name = in.readUTF();
        this.avg = in.readDouble();

    }
    //指的是排序规则，按照平均分进行排序，倒叙
    public int compareTo(StudentBean sb) {
        //先比较科目
        int tmp =this.getCourse().compareTo(sb.getCourse());
        if(tmp == 0){
            //再比较分数
            double temp = sb.getAvg() - this.getAvg();
            if(temp>1){
                return 1;
            }else if (temp == 0){
                //最后比较姓名，按照拼音先后顺序
               int tname=this.getName().compareTo(sb.getName());
                if (tname>1){
                    return 1 ;
                } else if(tname == 0){
                    return 0;
                }else{
                    return -1;
                }
            }else{
                return -1;
            }
        }
        return tmp;
    }
}
