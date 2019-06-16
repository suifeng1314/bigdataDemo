package com.longtao.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 使用mr处理 bean需要实现Writable
 */
public class TableBean implements Writable {

    // 订单id
    private String orderId;
    // 商品id
    private String pid;
    // 商品数量
    private int amount;
    // 商品名称
    private String pname;
    // 标记 区分订单表与商品表   0:订单表 1:商品表
    private String flag;

    public TableBean() {

    }

    public TableBean(String orderId, String pid, int amount, String pname, String flag) {
        this.orderId = orderId;
        this.pid = pid;
        this.amount = amount;
        this.pname = pname;
        this.flag = flag;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }
    // 序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(flag);
    }
    // 反序列化
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readUTF();
        pid = dataInput.readUTF();
        amount = dataInput.readInt();
        pname = dataInput.readUTF();
        flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return orderId + "\t" + pname + "\t" + amount;
    }
}
