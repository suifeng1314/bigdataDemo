package com.longtao.logs;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Hunter
 * @version 1.0, 21:14 2019/1/30
 *
 * 封装数据类型需要怎么做？
 * hadoop数据类型实现了序列化接口
 * 如果自定义需要实现这个序列化接口
 */
public class FlowBean implements Writable {

    //定义属性：上行流量 下行流量 总流量总和
    private long upFlow;
    private long dfFlow;
    private long flowsum;

    public FlowBean(){}

    public FlowBean(long upFlow,long dfFlow){
        this.upFlow = upFlow;
        this.dfFlow = dfFlow;
        this.flowsum = upFlow + dfFlow;
    }

    public long getUpFlow(){
        return upFlow;
    }

    public void setUpFlow(long upFlow){
        this.upFlow = upFlow;
    }

    public long getDfFlow(){
        return dfFlow;
    }

    public void setDfFlow(long dfFlow){
        this.dfFlow = dfFlow;
    }

    public long getFlowsum(){
        return flowsum;
    }

    public void setFlowsum(long flowsum){
        this.flowsum = flowsum;
    }


    //序列化
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(dfFlow);
        out.writeLong(flowsum);
    }

    //反序列化
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        dfFlow = in.readLong();
        flowsum = in.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + dfFlow + "\t" + flowsum;
    }
}
