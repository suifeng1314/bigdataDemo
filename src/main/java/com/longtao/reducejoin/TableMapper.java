package com.longtao.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


public class TableMapper extends Mapper<LongWritable,Text,Text,TableBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        TableBean v = new TableBean();
        Text k = new Text();
        // 1.区分两张表 文件切片
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        // 2.获取表名
        String tableName = inputSplit.getPath().getName();
        // 3.获取数据
        String line = value.toString();
        // 4.判断：是订单表  还是商品表
        if (tableName.contains("order.txt")){
            // 2018 01 1
            // 5.切分字段
            String[] fields = line.split("\t");

            // 6.封装bean对象 设置v
            v.setOrderId(fields[0]);
            v.setPid(fields[1]);
            v.setAmount(Integer.parseInt(fields[2]));
            v.setPname("");
            v.setFlag("0");

            // TODO 一定设置作为reduce输出的key
            k.set(fields[1]);

        } else {
            // 商品表
            String[] fields = line.split("\t");
            v.setOrderId("");
            v.setPid(fields[0]);
            v.setAmount(0);
            v.setPname(fields[1]);
            v.setFlag("1");
            // 设置key
            k.set(fields[0]);
        }

        context.write(k,v);
    }
}
