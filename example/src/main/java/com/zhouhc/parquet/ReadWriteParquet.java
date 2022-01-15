package com.zhouhc.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.*;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadWriteParquet {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadWriteParquet.class);


    private static MessageType getMessageTypeFromCode() {
        MessageType messageType = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("id")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("name")
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("age")
                .requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("test1")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("test2")
                .named("group1")
                .named("trigger");
        return messageType;
    }

    //往hdfs中写数据
    public static void writeParquetToHDFS(String ipAddr, String port, String filePath, String fileName) {
        //申明schema
        MessageType messageType = getMessageTypeFromCode();
        //申明 parquetWriter
        Path path = new Path(String.format("hdfs://%s:%s/%s/%s", ipAddr, port, filePath, fileName));
        Configuration configuration = new Configuration();
        GroupWriteSupport.setSchema(messageType, configuration);
        GroupWriteSupport writeSupport = new GroupWriteSupport();

        //写数据
        ParquetWriter<Group> writer = null;
        try {
            writer = new ParquetWriter<Group>(path, ParquetFileWriter.Mode.OVERWRITE, writeSupport, CompressionCodecName.UNCOMPRESSED,
                    128 * 1024 * 1024, 5 * 1024 * 1024, 5 * 1024 * 1024,
                    ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED, ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED, ParquetWriter.DEFAULT_WRITER_VERSION, configuration);

            //构造数据，封装Group
            for (int i = 0; i < 10; i++) {
                Group group = new SimpleGroupFactory(messageType).newGroup();
                group.append("name", "nameFor" + i).append("id", "id" + i)
                        .append("age", i).addGroup("group1").append("test1", "test1" + i)
                        .append("test2", "test2" + i);
                writer.write(group);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (writer != null)
                try {
                    writer.close();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
        }
    }


    //往hdfs中读数据
    public static void readParquetFromHDFS(String ipAddr, String port, String filePath, String fileName) {

        //申明 readSupport
        GroupReadSupport groupReadSupport = new GroupReadSupport();
        Path path = new Path(String.format("hdfs://%s:%s/%s/%s", ipAddr, port, filePath, fileName));

        //通过read读文件
        ParquetReader<Group> reader = null;
        try {
            reader = ParquetReader.builder(groupReadSupport, path).build();
            Group group = null;
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            while ((group = reader.read()) != null) {
                System.out.print(group);
                long starttime = ParquetTimestampUtils.getTimestampMillis(group.getInt96("starttime", 0));
                long stoptime = ParquetTimestampUtils.getTimestampMillis(group.getInt96("stoptime", 0));
                System.out.print(String.format("starttime : %s ,  stoptime: %s %n%n", dateFormat.format(starttime), dateFormat.format(stoptime)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null)
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    //如果没有权限的话，可以在idea中为系统设置环境变量 HADOOP_USER_NAME=zhouhc
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "rapids");
        //writeParquetToHDFS("had1","9000","/tmp/parquettest","test1.parq");
        //readParquetFromHDFS("flow0", "8020", "/zhouhc/hadoop_citibike_parquet_application", "part-ce85627e-34b7-4bf0-9442-2c9d9149b9e1-0");
    }


}
