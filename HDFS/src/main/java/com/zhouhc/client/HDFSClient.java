package com.zhouhc.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.net.URI;

//客户端编写
public class HDFSClient {

    public static void main(String[] args) throws Exception {
        //创建连接
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://had1:8020"), new Configuration(), "zhouhc");
        //操作文件
        fileSystem.copyFromLocalFile(new Path("D:\\GoogleBrower\\apache-zookeeper-3.5.7-bin.tar.gz"),
                new Path("/"));
        fileSystem.delete( new Path("/xsync") ,true);
        //关闭连接
        fileSystem.close();
    }
}
