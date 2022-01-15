package com.zhouhc.reduceJion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class ReduceJionDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //因为本地任务提交到
        Configuration configuration = new Configuration();
        //提交到远程服务器配置
//        configuration.set("fs.defaultFS", "hdfs://had1:8020");
//        configuration.set("mapreduce.framework.name","yarn");
//        configuration.set("mapreduce.app-submission.cross-platform","true");
//        configuration.set("yarn.resourcemanager.hostname","had2");
//
//        configuration.set("mapred.job.queue.name", "Test");
        //任务设置
        Job job = Job.getInstance(configuration);
        //提交到远程服务器不能这么做
       job.setJarByClass(ReduceJionDriver.class);
       //提交远程服务器配置
        //job.setJar("F:\\ProWorkSpace\\IDEA\\Hadoop\\MapReduce\\target\\MapReduce-1.0-SNAPSHOT.jar");

        job.setMapperClass(ReduceJionMapper.class);
        job.setReducerClass(ReduceJionReduce.class);
        job.setGroupingComparatorClass(PhoneBeanComparator.class);

        job.setMapOutputKeyClass(PhoneBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(PhoneBean.class);
        job.setOutputKeyClass(NullWritable.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);

    }
}
