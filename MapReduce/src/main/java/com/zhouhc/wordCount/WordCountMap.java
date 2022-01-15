package com.zhouhc.wordCount;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

//单词统计,每行， 一个Map实例对应一个java线程，所以不用担心线程安全性问题
public class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    //技术
    private Text KeyStr = new Text();
    private final IntWritable intWritable = new IntWritable(1);

    /**
     * @param key     文件的字节偏移量
     * @param value   一行的内容
     * @param context 框架
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //获取对应这一行的数据
        String words = value.toString();
        //单词分解
        String[] wordArrays = StringUtils.split(words, " ");
        //数据分解
        for (String word : wordArrays) {
            KeyStr.set(word);
            context.write(KeyStr, intWritable);
        }
    }


}
