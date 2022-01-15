package com.zhouhc.reduceJion;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

//比较器
public class ReduceJionReduce extends Reducer<PhoneBean, NullWritable, PhoneBean, NullWritable> {

    @Override
    protected void reduce(PhoneBean key, Iterable<NullWritable> values, Reducer<PhoneBean, NullWritable, PhoneBean, NullWritable>.Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iterator = values.iterator();
        NullWritable next = iterator.next();
        String pname = key.getPname();

        //开始设置,写入文件
        while (iterator.hasNext()) {
            iterator.next();
            key.setPname(pname);
            context.write(key, NullWritable.get());
        }
    }
}
