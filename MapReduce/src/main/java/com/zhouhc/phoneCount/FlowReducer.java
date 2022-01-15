package com.zhouhc.phoneCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        long sumlowUp = 0;
        long sumlowDown = 0;

        for (FlowBean value : values) {
            sumlowUp += value.getFlowUp();
            sumlowDown += value.getFlowDown();
        }
        flowBean.set(sumlowUp, sumlowDown);
        context.write(key, flowBean);
    }
}
