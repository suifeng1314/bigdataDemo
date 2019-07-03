package com.longtao.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver implements Tool {

    private Configuration conf;

    public int run(String[] strings) throws Exception {
        // 1.创建任务
        Job job = Job.getInstance(conf);

        // 2.指定任务主累
        job.setJarByClass(Driver.class);
        // 3.配置job
        Scan scan = new Scan();
        // 4.设置具体运行的mapper类
        TableMapReduceUtil.initTableMapperJob("love",
                scan,
                ReadLoveMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);

        //5.设置具体运行的reducer类
        TableMapReduceUtil.initTableReducerJob("lovemr",
                WriteLoveReducer.class,
                job
        );

        //6.设置reduceTask
        job.setNumReduceTasks(1);

        boolean rs = job.waitForCompletion(true);
        return rs ? 0 : 1;
    }

    public void setConf(Configuration configuration) {
        this.conf = HBaseConfiguration.create(configuration);
    }

    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {
        // 状态码
        int status = ToolRunner.run(new Driver(),args);
        System.exit(status);
    }
}
