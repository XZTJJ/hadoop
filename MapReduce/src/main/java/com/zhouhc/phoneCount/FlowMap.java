package com.zhouhc.phoneCount;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//Map框架,一个实例一个线程
public class FlowMap extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text phone = new Text();
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        //获取手机号
        String[] fields = StringUtils.split(value.toString(), "\t");
        phone.set(fields[1]);
        flowBean.set(Long.valueOf(fields[fields.length - 3]), Long.valueOf(fields[fields.length - 2]));
        context.write(phone, flowBean);
    }
}
