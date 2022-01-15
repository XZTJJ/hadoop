package com.zhouhc.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


//单词统计和 Reduce阶段
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * @param key     单词
     * @param values  每个map统计的单词的技术
     * @param context 框架本身
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        //统计
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
