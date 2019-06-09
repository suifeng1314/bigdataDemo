package com.longtao.main.server;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyInterface extends VersionedProtocol {
    // TODO  版本名称必须是versionID 不能自定义
    public static long versionID = 1001;
    public String HelloWorld(String name);

}
