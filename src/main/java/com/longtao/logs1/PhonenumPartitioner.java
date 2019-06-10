package com.longtao.logs1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区：根据手机号前三位
 *  默认分区方式：hash
 */
public class PhonenumPartitioner extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {

        //1.获取手机号的前三位 string.substring
        String phoneNum = key.toString().substring(0, 3);

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
