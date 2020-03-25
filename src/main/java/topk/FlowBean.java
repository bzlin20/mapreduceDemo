package topk;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Copyright (C), 2019-2020
 * Author: Administrator
 * Date: 2020/3/25 9:35
 * FileName: FlowBean
 * Description: 自定义类 流量
 */
public class FlowBean  implements WritableComparable<FlowBean> {
    private String phoneNe;
    private long up_flow;
    private  long down_flow;
    private long sum_flow;

    public void setPhoneNe(String phoneNe) {
        this.phoneNe = phoneNe;
    }

    public void setUp_flow(long up_flow) {
        this.up_flow = up_flow;
    }

    public void setDown_flow(long down_flow) {
        this.down_flow = down_flow;
    }

    public void setSum_flow(long sum_flow) {
        this.sum_flow = sum_flow;
    }

    public String getPhoneNe() {
        return phoneNe;
    }

    public long getUp_flow() {
        return up_flow;
    }

    public long getDown_flow() {
        return down_flow;
    }

    public long getSum_flow() {
        return sum_flow;
    }

    public FlowBean() {
    }

    public FlowBean(String phoneNe, long up_flow, long down_flow) {
        this.phoneNe = phoneNe;
        this.up_flow = up_flow;
        this.down_flow = down_flow;
        this.sum_flow = up_flow + down_flow ;
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "phoneNe='" + phoneNe + '\'' +
                ", up_flow=" + up_flow +
                ", down_flow=" + down_flow +
                ", sum_flow=" + sum_flow +
                '}';
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(phoneNe);
        out.writeLong(up_flow);
        out.writeLong(down_flow);
        out.writeLong(sum_flow);
    }

    public void readFields(DataInput in) throws IOException {
       phoneNe = in.readUTF();
       up_flow = in.readLong();
       down_flow = in.readLong();
       sum_flow = in.readLong();
    }
      /**
       *   按流量倒序排序
       * */
     public int compareTo(FlowBean o) {
        if (this.sum_flow < o.sum_flow){
            return 1;
        }else if (this.sum_flow == o.sum_flow){
            return 0;
        } else {
            return -1;
        }
    }
}
