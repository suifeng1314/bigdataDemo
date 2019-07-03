package com.longtao.hbase.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ReadLoveMapper extends TableMapper<ImmutableBytesWritable,Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // 1.读取数据 拿到rowkey拿到数据
        Put put = new Put(key.get());

        // 2.过滤列
        for(Cell cell : value.rawCells()){
            //3.拿到info列族数据 如果是info列族 取出 如果不是info 过滤掉
            if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                // 过滤列
                if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    put.add(cell);
                }
            }
        }
        // 4.输出到reduce端
        context.write(key,put);
    }
}
