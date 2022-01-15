package com.zhouhc.phoneCount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


// 上传客户端 和 驱动程序
public class FlowDriver {

    //配置中心
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //配置和job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //设置加载jar
        job.setJarByClass(FlowDriver.class);
        //设置Map ，Reducer 以及他们对应的类型
        job.setMapperClass(FlowMap.class);
        job.setReducerClass(FlowReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //设置输入文件
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
