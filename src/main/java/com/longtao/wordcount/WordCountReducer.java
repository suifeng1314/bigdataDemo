package com.longtao.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * keyIn:mapper输出的ey的类型
 * valueIn:mapper输出value的类型
 *
 * reducer： 接收mapper输出的数据
 * keyOut:单词 Text
 * valueOut:出现的次数 IntWritable
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 1.记录次数
        int sum = 0;
        for (IntWritable v : values){
            sum += v.get();
        }
        // 2.累加求和输出
        context.write(key,new IntWritable(sum));
    }
}
