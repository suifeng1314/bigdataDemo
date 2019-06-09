package com.longtao.server;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class MyInterfaceImpl implements MyInterface {
    public String HelloWorld(String name) {

        return "Hello " + name;
    }
    // 返回版本号
    public long getProtocolVersion(String s, long l) throws IOException {
        return MyInterface.versionID;
    }
    // 返回签名信息
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return new ProtocolSignature(MyInterface.versionID,null);
    }
}
