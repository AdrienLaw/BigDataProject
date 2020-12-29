package com.adrien.mapreduce.comparable;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 排序案例实操（全排序）
 */
@Data
@Setter
@Getter
public class ComFlowBean implements WritableComparable<ComFlowBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public ComFlowBean() {
    }

    public ComFlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        sumFlow = upFlow + downFlow;
    }

    @Override
    public int compareTo(ComFlowBean o) {
        int result;
        if (sumFlow > o.getSumFlow()) {
            result = -1;
        } else if (sumFlow < o.getSumFlow()) {
            result = 1;
        } else {
            result = 0;
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return "ComFlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", sumFlow=" + sumFlow +
                '}';
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
}
