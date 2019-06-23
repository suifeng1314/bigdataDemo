package com.longtao.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class MyConcat extends UDF {
    // 大写转换为小写
    public String evaluate(String a,String b){
        return a + "*****" + String.valueOf(b);
    }
}
