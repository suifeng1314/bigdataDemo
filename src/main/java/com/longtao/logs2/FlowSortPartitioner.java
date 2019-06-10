package com.longtao.logs2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowSortPartitioner extends Partitioner<FlowBean,Text> {
    @Override
    public int getPartition(FlowBean key, Text value, int numPartitions) {
        //1.获取手机号的前三位 string.substring
        String phoneNum = value.toString().substring(0, 3);

        //2.分区
        int partitioner = 4;

        if("135".equals(phoneNum)){
            return 0;
        }else if("137".equals(phoneNum)){
            return 1;
        }else if("138".equals(phoneNum)){
            return 2;
        }else if("139".equals(phoneNum)){
            return 3;
        }

        return partitioner;
    }
}
