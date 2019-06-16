package com.longtao.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;


public class TableReducer extends Reducer<Text,TableBean,TableBean,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        // 2018 01 1
        // 01 苹果
        List<TableBean> orderList = new ArrayList<TableBean>();
        TableBean pdBean = new TableBean();

        // 订单表示
        String orderFlag = "0";

        for (TableBean v : values){
            if (orderFlag.equals(v.getFlag())){
                TableBean tableBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tableBean,v);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                // 添加到集合
                orderList.add(tableBean);
            } else {
                // 商品表
                try {
                    BeanUtils.copyProperties(pdBean,v);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

            }
        }
        // 表的join操作
        for (TableBean tableBean:orderList){
            // 2018 01 1 null 加入pname
            tableBean.setPname(pdBean.getPname());
            // 输出 订单表 (pid替换成了pname)
            context.write(tableBean,NullWritable.get());
        }
    }
}
