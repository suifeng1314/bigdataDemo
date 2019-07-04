package com.longtao.hbase.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * hdfs -> hbase
 */
public class Driver implements Tool {

    private Configuration conf;

    public int run(String[] strings) throws Exception {
        // 1.创建任务
        Job job = Job.getInstance(conf);
        job.setJarByClass(Driver.class);
        // 2.配置mapper
        job.setMapperClass(ReadHdfsMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        // 3.配置reducer
        TableMapReduceUtil.initTableReducerJob("lovehdfs",WriteHbaseReducer.class,job);
        // 4.输出配置 hdfs读取数据 inputformat
        FileInputFormat.addInputPath(job,new Path("/lovehbase/"));

        // 5.需要配置outputFormat吗？ 不需要 reducer中已指定了表


        return job.waitForCompletion(true) ? 0 : 1;
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
