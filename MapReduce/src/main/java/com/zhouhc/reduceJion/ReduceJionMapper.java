package com.zhouhc.reduceJion;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

//分组和区分
public class ReduceJionMapper extends Mapper<LongWritable, Text, PhoneBean, NullWritable> {
    //文件名
    private String fileName;
    private PhoneBean phoneBean = new PhoneBean();

    //获取文件名，用于比较文件
    @Override
    protected void setup(Mapper<LongWritable, Text, PhoneBean, NullWritable>.Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, PhoneBean, NullWritable>.Context context) throws IOException, InterruptedException {
        String[] split = StringUtils.split(value.toString(), "\t");
        if (StringUtils.equalsIgnoreCase(fileName, "order.txt")) {
            phoneBean.set(split[0],split[1], Integer.valueOf(split[2]), "");
        } else {
            phoneBean.set("", split[0], 0, split[1]);
        }
        context.write(phoneBean, NullWritable.get());
    }
}
