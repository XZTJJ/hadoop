package com.zhouhc.phoneCount;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//自定义的 hadoop 的序列化 Bean
public class FlowBean implements Writable {
    private long flowUp;
    private long flowDown;
    private long flowSum;


    public void set(long flowUp, long flowDown) {
        this.flowUp = flowUp;
        this.flowDown = flowDown;
        this.flowSum = flowUp + flowDown;

    }

    public long getFlowUp() {
        return flowUp;
    }

    public void setFlowUp(long flowUp) {
        this.flowUp = flowUp;
    }

    public long getFlowDown() {
        return flowDown;
    }

    public void setFlowDown(long flowDown) {
        this.flowDown = flowDown;
    }

    public long getFlowSum() {
        return flowSum;
    }

    public void setFlowSum(long flowSum) {
        this.flowSum = flowSum;
    }

    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(flowUp);
        out.writeLong(flowDown);
        out.writeLong(flowSum);
    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.flowUp = in.readLong();
        this.flowDown = in.readLong();
        this.flowSum = in.readLong();
    }

    @Override
    public String toString() {
        return String.format("%s\t%s\t%s", this.flowUp, this.flowDown, this.flowSum);
    }
}
