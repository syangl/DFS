package com.ucas.bigdata.client;

import com.ucas.bigdata.common.Config;
import com.ucas.bigdata.common.FileInfo;
import com.ucas.bigdata.common.MetaOpCode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class MetaServerClient {

    private static final String META_SERVRE_HOST = Config.META_SERVRE_HOST; // 元数据服务器的主机名
    private static final int METADATA_SERVER_PORT = Config.META_SERVRE_PORT; // 元数据服务器的端口号
    private Connection connection;

    public MetaServerClient() throws IOException {
        connection = new Connection(META_SERVRE_HOST, METADATA_SERVER_PORT);
    }
    public static void main(String[] args) {
        MetaServerClient metaServerClient = null;
        try {
            metaServerClient = new MetaServerClient();

            // 发送请求并接收响应
            List<String> files = metaServerClient.listFiles("/");
            metaServerClient.createFile("/home");
            metaServerClient.createFile("/scs");
            metaServerClient.createFile("/home/1/a");
            System.out.println("files:"+files);
            files = metaServerClient.listFiles("/");
            System.out.println("files:"+files);
            metaServerClient.connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public List<String> listFiles(String cur_dir) {
        List<String> fl = new ArrayList<>();
        try {
            MetaOpCode.LIST_FILE.write(connection.getOut()); //
            connection.flush();
            connection.writeUTF(cur_dir); // 客户端发送路径
            connection.flush();



            int size =  connection.readInt();
            System.out.println("list size:" + size);
            for(int i = 0;i<size;i++){
                fl.add(connection.readUTF());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fl;
    }

    public String heartBeat(String nodeName) {
        try {
            MetaOpCode.HEART_BEAT.write(connection.getOut()); //
            connection.flush();
            connection.writeUTF(nodeName); // 读取客户端发送路径
            connection.flush();
            int code = connection.readInt();
            String msg = connection.readUTF();
            return msg;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String createFile(String path) {
        try {
            MetaOpCode.CREATE_FILE.write(connection.getOut()); //
            connection.flush();
            connection.writeUTF(path); // 读取客户端发送路径
            connection.writeUTF(Config.USER); // 读取客户端发送用户
            connection.writeBoolean(false); // 是否为目录

            connection.flush();
            int code = connection.readInt();
            String nodeAndFileId = connection.readUTF();
            String nodeName = nodeAndFileId.split(":")[0];
            String localFileId = nodeAndFileId.split(":")[1];
            System.out.println(nodeName+":"+localFileId);
            return localFileId;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean closeFile(String path) {
        try {
            // 发送操作码
            MetaOpCode.CLOSE_FILE.write(connection.getOut());
            connection.flush();

            // 发送文件路径
            connection.writeUTF(path);
            connection.flush();

            // 读取元数据服务器的响应
            int retCode = connection.readInt();
            String msg = connection.readUTF();

            if (retCode == 0) {
                System.out.println("File closed successfully on metadata server: " + path);
                return true;
            } else {
                System.err.println("Failed to close file on metadata server: " + msg);
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public List<String> getFileLocations(String path) {
        try {
            MetaOpCode.CREATE_FILE.write(connection.getOut()); //
            connection.flush();
            connection.writeUTF(path); // 读取客户端发送路径

            connection.flush();
            // 读取List<String>
            int size = connection.readInt();
            List<String> locations = new ArrayList<>();
            for(int i = 0;i<size;i++){
                locations.add(connection.readUTF());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean deleteFile(String path) {
        try {
            // 1. 发送删除文件的操作码
            MetaOpCode.DEL_FILE.write(connection.getOut());
            connection.flush();

            // 2. 发送文件路径
            connection.writeUTF(path);
            connection.flush();

            // 3. 读取元数据服务器的响应
            int retCode = connection.readInt();
            String msg = connection.readUTF();

            if (retCode == 0) {
                System.out.println("Metadata deleted successfully for file: " + path);
                return true;
            } else {
                System.err.println("Failed to delete metadata for file: " + path + ". Reason: " + msg);
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public void close() throws IOException {
        this.connection.close();
    }

    // 假设还有其他辅助方法的实现...
}

