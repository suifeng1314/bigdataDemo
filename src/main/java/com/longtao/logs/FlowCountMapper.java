package com.longtao.logs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * keyin:
 * valuein:
 *
 * 根据想要的kv类型 手机号总和(上行+下行)
 * keyout:
 * valueout:
 */
public class FlowCountMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 接入数据

        // 切割 \t

        // 拿到关键字段

        // 写出到reducer
    }
}
