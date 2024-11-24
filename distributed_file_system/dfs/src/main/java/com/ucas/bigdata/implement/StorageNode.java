package com.ucas.bigdata.implement;

import java.util.HashMap;
import java.util.Map;

public class StorageNode {

    private String name;
    private Map<String, byte[]> fileStorage; // 文件存储，文件路径到文件数据的映射

    public StorageNode(String name) {
        this.name = name;
        fileStorage = new HashMap<>();
    }

    // 上传文件到存储节点
    public boolean uploadFile(String localFilePath, String remoteFilePath) {
        byte[] fileData = readFromFile(localFilePath);
        if (fileData != null) {
            fileStorage.put(remoteFilePath, fileData);
            return true;
        }
        return false;
    }

    // 下载文件数据
    public byte[] downloadFile(String remoteFilePath) {
        return fileStorage.get(remoteFilePath);
    }

    // 删除文件
    public boolean deleteFile(String remoteFilePath) {
        if (fileStorage.containsKey(remoteFilePath)) {
            fileStorage.remove(remoteFilePath);
            return true;
        }
        return false;
    }

    // 从本地读取文件数据
    private byte[] readFromFile(String localFilePath) {
        // 实现从文件读取数据的逻辑
        return null; // 假设读取数据的方法
    }

    public String getName() {
        return name;
    }
}

