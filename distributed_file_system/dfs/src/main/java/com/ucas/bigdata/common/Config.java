package com.ucas.bigdata.common;

public class Config {
    public static String META_SERVRE_HOST = "127.0.0.1";
//    public static String META_SERVRE_HOST = "dfs101";
//    public static String META_SERVRE_HOST = "localost";
    public static int DATA_SERVRE_PORT = 9526;
    public static int META_SERVRE_PORT = 9527;
    public static int TIMEOUT_OF_HEARTBEATS = 20;
    public static int HEARTBEAT_SECS = 3;
    public static String META_DB_PATH = "/opt/meta_data";

    public static String USER = "dfs";
    public static String GROUP = "dfsg";
}
