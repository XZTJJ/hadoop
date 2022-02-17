package com.zhouhc.readLog;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import com.google.gson.Gson;
import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;


//读写日志
public class ReadWriteLog {
    //全局变量的初始化
    private static final GrokCompiler GROKCOMPILER = GrokCompiler.newInstance();
    private static final DateTimeFormatter DATETIMEFORMATTER = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
    private static final String DATETIMEPATTER = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}";
    //临时处理函数
    private static final List<Map<String, Object>> dateTempMap = new ArrayList<Map<String, Object>>();
    //全局线程工厂
    private static final ExecutorService exec = Executors.newFixedThreadPool(10);


    //注册对应的数据
    static {
        GROKCOMPILER.registerDefaultPatterns();
        GROKCOMPILER.register("DATETIME", DATETIMEPATTER);
    }

    public static void main(String[] args) {
        long starTime = System.currentTimeMillis();
        start(args);
        long endTime = System.currentTimeMillis();
        System.out.println("处理耗时为 : " + (endTime - starTime) + " ms");
    }


    //开始验证
    public static void start(String[] args) {
        try {
            //hadoop相关信息
            String hdfsHost = args[0];
            String hdfsPort = args[1];
            String hdfsPath = args[2];
            String hdfsUsername = args[3];
            //es相关信息
            String esHost = args[4];
            String esPort = args[5];
            String esIndex = args[6];
            //判断信息是否为空
            if (StringUtils.isBlank(hdfsHost) || StringUtils.isBlank(hdfsPort) || StringUtils.isBlank(hdfsPath)
                    || StringUtils.isBlank(hdfsUsername) || StringUtils.isBlank(esHost) || StringUtils.isBlank(esPort)
                    || StringUtils.isBlank(esIndex)) {
                System.out.println("hdfsHost,hdfsPort,hdfsPath,hdfsUsername,esHost,esPort,esIndex不能为空");
                return;
            }
            ConfigPO configPO = new ConfigPO(hdfsHost, hdfsPort, hdfsPath, hdfsUsername, esHost, esPort, esIndex);
            readFileForHDFS(configPO);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //任务已经已经提交完成，等待执行完毕就好
            exec.shutdown();
            //等待线程完成就好了
            while (!exec.isTerminated()) {
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    //从hdfs上读取文件
    private static void readFileForHDFS(ConfigPO configPO) {
        //连接地址
        URI hdfsUri = URI.create(String.format("hdfs://%s:%s", configPO.hdfsHost, configPO.hdfsPort));
        Path hadfsPath = new Path(configPO.hdfsPath);
        //文件系统
        FileSystem fileSystem = null;
        FSDataInputStream fsDataInputStream = null;
        BufferedReader br = null;
        //创建连接
        try {
            //获取hdfs的文件系统
            fileSystem = FileSystem.get(hdfsUri, new Configuration(), configPO.hdfsUsername);
            //获取文件，除去目录
            List<Path> allFilePaths = new ArrayList<Path>();
            getFilePath(fileSystem, hadfsPath, allFilePaths);
            System.out.println(String.format("获取到的所有文件path为:%s", Arrays.toString(allFilePaths.toArray())));
            //逐一解析文件
            for (Path filePath : allFilePaths) {
                //获取applicationId
                String applicationId = filePath.getParent().getName();
                configPO.applicationId = applicationId;
                configPO.jobName = "jobName-" + Math.abs(applicationId.hashCode());
                configPO.username = "use" + Math.abs(applicationId.hashCode());
                //开始读取文件
                fsDataInputStream = fileSystem.open(filePath);
                br = new BufferedReader(new InputStreamReader(fsDataInputStream, Charset.forName("UTF-8")));
                String result = "";
                String str = "";
                while ((str = br.readLine()) != null) {
                    if (str.matches("^" + DATETIMEPATTER + ".*")) {
                        //处理逻辑
                        grokString(result, configPO, false);
                        result = str;
                    } else if (isNotSpecialChar(str) && StringUtils.isNotBlank(result)) {
                        //非特殊字符，并且上一行结果不为空才拼接
                        result = result + str;
                    } else {
                        grokString(result, configPO, false);
                        result = "";
                    }
                }
                //该文件已经解析完了，直接强制刷新
                grokString(result, configPO, true);
                //解析完成后关闭流
                IOUtils.closeQuietly(br);
                IOUtils.closeQuietly(fsDataInputStream);
            }
            //逐一解析
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            IOUtils.closeQuietly(br);
            IOUtils.closeQuietly(fsDataInputStream);
            IOUtils.closeQuietly(fileSystem);
        }
    }

    //解析字符,开始正则解析,有一个强制选项是否强制推送
    private static void grokString(String gorkStr, ConfigPO configPO, boolean isFlush) {
        if (StringUtils.isBlank(gorkStr) && !isFlush)
            return;
        if (StringUtils.isBlank(gorkStr) && isFlush) {
            exec.submit(new MyIndexEsTasking(new ArrayList<Map<String, Object>>(dateTempMap), configPO.esHost, configPO.esPort,
                    configPO.esIndex, configPO.applicationId, configPO.jobName, configPO.username));
            dateTempMap.clear();
            return;
        }
        //这里面转成json数据, 注意这里的Log4j的表达式为 :  %d{ISO8601} %p %c: %m%n
        Grok grok = GROKCOMPILER.compile("%{DATETIME:dtm}\\s* %{LOGLEVEL:lev}\\s* %{NOTSPACE:cln}\\s* \\[\\] -\\s* %{GREEDYDATA:dmsg}");
        //匹配并且转成对应的数据
        Match match = grok.match(gorkStr);
        Map<String, Object> dateMap = match.capture();
        //处理信息
        dateTempMap.add(dateMap);
        if (dateMap.size() > 50 || isFlush) {
            exec.submit(new MyIndexEsTasking(new ArrayList<Map<String, Object>>(dateTempMap), configPO.esHost, configPO.esPort,
                    configPO.esIndex, configPO.applicationId, configPO.jobName, configPO.username));
            dateTempMap.clear();
        }
    }

    //获取hdfs的文件夹中所有的文件，不包含目录
    private static void getFilePath(FileSystem fileSystem, Path hadfsPath, List<Path> paths) throws FileNotFoundException,
            IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(hadfsPath);
        for (FileStatus fileStatus : fileStatuses) {
            //既不是文件，也不是目录，直接跳过
            if (!(fileStatus.isFile() || fileStatus.isDirectory()))
                continue;
                //文件的情况
            else if (fileStatus.isFile())
                paths.add(fileStatus.getPath());
                //目录情况
            else
                getFilePath(fileSystem, fileStatus.getPath(), paths);
        }
    }

    //判断是否为特殊字符
    private static boolean isNotSpecialChar(String str) {
        if (StringUtils.isBlank(str))
            return false;
        int number = (int) str.charAt(0);
        return number != 0 && number != 65533 && number != 16;
    }


    //推送任务的简单内部类
    private static class MyIndexEsTasking implements Callable<Void> {
        //数据和序列化工具
        private final List<Map<String, Object>> tempMaps;
        private final Gson gson;
        //es对应的信息
        private final String esHost;
        private final String esPort;
        private final String esIndex;
        //需要填充的信息
        private final String applicationId;
        private final String jobName;
        private final String userName;

        public MyIndexEsTasking(List<Map<String, Object>> tempMaps, String esHost, String esPort, String esIndex, String applicationId, String jobName, String userName) {
            this.tempMaps = tempMaps;
            this.esHost = esHost;
            this.esPort = esPort;
            this.esIndex = esIndex;
            this.applicationId = applicationId;
            this.jobName = jobName;
            this.userName = userName;
            gson = new Gson();
        }

        //进行数据的处理
        @Override
        public Void call() throws Exception {
            StringBuilder sb = new StringBuilder();
            //进行数据处理工作
            for (Map<String, Object> oriMap : tempMaps) {
                Map<String, Object> tempMap = new HashMap<String, Object>(oriMap);
                //填充信息
                tempMap.put("aid", applicationId);
                tempMap.put("jbn", jobName);
                tempMap.put("unm", userName);
                //填充错误信息
                if (tempMap.get("lev").toString().equals("ERROR")) {
                    tempMap.put("loc", "BlockContext" + ThreadLocalRandom.current().nextInt() + ".java:" + ThreadLocalRandom.current().nextInt());
                    tempMap.put("ert", "org.apache.flink.runtime.client.JobCancellationException" + ThreadLocalRandom.current().nextInt());
                    tempMap.put("erm", "Job was cancelled For reason" + ThreadLocalRandom.current().nextInt());
                }
                sb.append("{ \"index\":  {}}").append(System.lineSeparator()).append(gson.toJson(tempMap)).append(System.lineSeparator());
            }
            //开始推送数据
            if (sb.length() <= 0)
                return null;
            sb.append(System.lineSeparator());
            //开始推送数据
            String esUri = String.format("http://%s:%s/%s/_doc/_bulk", esHost, esPort, esIndex);
            HttpResponse execute = HttpRequest.post(esUri).body(sb.toString(), "application/json").execute();
            sb = new StringBuilder("写入es");
            if (execute.getStatus() < 300)
                sb.append("成功");
            else
                sb.append("失败").append(execute.body());
            System.out.println(sb.toString());
            return null;
        }
    }

    //简单的pojo内,保存配置类
    public static class ConfigPO {
        //简单的pojo
        String hdfsHost;
        String hdfsPort;
        String hdfsPath;
        String hdfsUsername;
        String esHost;
        String esPort;
        String esIndex;
        //其他信息
        String applicationId;
        String username;
        String jobName;

        //构造函数
        public ConfigPO(String hdfsHost, String hdfsPort, String hdfsPath, String hdfsUsername, String esHost, String esPort, String esIndex) {
            this.hdfsHost = hdfsHost;
            this.hdfsPort = hdfsPort;
            this.hdfsPath = hdfsPath;
            this.hdfsUsername = hdfsUsername;
            this.esHost = esHost;
            this.esPort = esPort;
            this.esIndex = esIndex;
        }
    }

}
