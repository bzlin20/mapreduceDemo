package MapreduceFinalExample;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//           班级   学号    姓名    语文 数学  英语
//  数据格式 1303	3001 	谢雨泽	95	  96	98
public class StudentScore  implements WritableComparable<StudentScore> {

    private String classname;
    private int    sumscore;
    public StudentScore(){ }

    public String getClassname(){
        return classname;
    }
    public Integer getSumscore(){
        return sumscore;
    }

    public StudentScore(String classname,int sumscore){
        super();
        this.classname = classname;
        this.sumscore = sumscore;
    }
    public String toString(){
        return classname+'\t'+sumscore;
    }

    public void write(DataOutput out) throws IOException {
            out.writeUTF(classname);
            out.writeInt(sumscore);
    }

    public void readFields(DataInput in) throws IOException {
         this.classname=in.readUTF();
         this.sumscore = in.readInt();
    }
    public int compareTo(StudentScore o) {  //定义比较规则  如果是同一班级，则按总分倒序
        int tep = o.getClassname().compareTo(this.getClassname());
        if(tep == 0){
            tep = o.getSumscore() - this.getSumscore();
        }
        return tep;
    }
}
