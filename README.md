# bigdataDemo
大数据基本实现demo
---
**start**

1.本地配置hadoop环境变量
   
   HADOOP_HOME = hadoop文件路径，path 添加 %HADOOP_HOME%\bin
   
   [github](https://github.com/suifeng1314/hadoopwindow) 下载运行配置文件(md有说明)
   
   <font color="red">注意：</font>Windows 10 系统配置后需要重启才能生效   
2.example

    测试文件在com.longtao.file下
    配置Run/Configurations -> Application -> Pragram arguments -> d://in d://out(输入输出路径)
    logs1和log2注意分区数是否一致
    
3. 配置临时hbase环境变量


    export HBASE_HOME=/opt/softword/hbase1.3
    export HADOOP_HOME=/opt/softword/hadoop-2.8.4
    export export HADOOP_CLASSPATH=$(${HBASE_HOME}/bin/hbase mapredcp)

   
    