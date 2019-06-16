package com.longtao.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建job任务
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.指定jar包位置
        job.setJarByClass(TableDriver.class);

        //3.关联使用的Mapper类
        job.setMapperClass(TableMapper.class);

        //4.关联使用的Reducer类
        job.setReducerClass(TableReducer.class);

        //5.设置mapper阶段输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        //6.设置reducer阶段输出的数据类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        //7.设置数据输入的路径 默认TextInputFormat
        FileInputFormat.setInputPaths(job,new Path("f://table//in"));

        //8.设置数据输出的路径
        FileOutputFormat.setOutputPath(job,new Path("f://table//out"));

        //9.提交任务
        boolean rs = job.waitForCompletion(true);
        System.exit(rs?0:1);
    }
}
