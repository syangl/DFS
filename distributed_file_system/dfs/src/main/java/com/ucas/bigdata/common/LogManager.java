package com.ucas.bigdata.common;

import org.rocksdb.*;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class LogManager {

    private static final String DB_PATH = "/rocksdb_log";  // RocksDB 数据库路径
    private RocksDB db;

    public LogManager() {
        try {
            // 初始化 RocksDB
            RocksDB.loadLibrary();
            Options options = new Options();
            options.setCreateIfMissing(true);  // 如果数据库不存在则创建
            db = RocksDB.open(options, DB_PATH);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    // 记录日志操作
    public synchronized void log(String operationKey, String operationValue) {
        try {
            db.put(operationKey.getBytes(), operationValue.getBytes());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    // 读取所有日志
    public List<String> readLogs() {
        List<String> logs = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                logs.add(new String(iterator.key()) + " " + new String(iterator.value()));
            }
        }
        return logs;
    }

    // 关闭 RocksDB 数据库
    public void close() {
        if (db != null) {
            db.close();
        }
    }
}
