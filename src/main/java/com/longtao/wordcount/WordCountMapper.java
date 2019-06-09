package com.longtao.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * keyIn:LongWritable(long) 数据的起始偏移量
 * valueIn:具体数据
 *
 * mapper 把数据传输到reducer阶段(<hehe,1>)
 * keyOut:单词 Text
 * valueOut:出现的次数 IntWritable
 */
public class WordCountMapper extends Mapper<Writable,Text,Text,IntWritable> {

    @Override
    protected void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
        // 1.接入数据
        String line = value.toString();
        // 2.对数据进行切分
        String[] words = line.split(" ");
        // 3.写出以<he,1>
        for (String str:words) {
            context.write(new Text(str),new IntWritable(1));
        }
    }
}
