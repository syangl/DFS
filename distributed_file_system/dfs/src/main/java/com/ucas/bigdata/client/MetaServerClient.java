package com.ucas.bigdata.client;

import com.ucas.bigdata.common.Config;
import com.ucas.bigdata.common.DataOpCode;
import com.ucas.bigdata.common.FileInfo;
import com.ucas.bigdata.common.MetaOpCode;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.SocketException;
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


    public List<String> listFiles(String path) {
        List<String> fileList = new ArrayList<>();
        try {
            MetaOpCode.LIST_FILE.write(connection.getOut());
            connection.writeUTF(path);
            connection.flush();
            // 接收 MetadataServer 返回的文件列表
            int size = connection.readInt();
            for (int i = 0; i < size; i++) {
                fileList.add(connection.readUTF());
            }
        } catch (IOException e) {
            System.err.println("Error fetching file list for directory: " + path);
            e.printStackTrace();
        }
        return fileList;
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

    public Integer createFile(String path) throws IOException {
        try {
        DataOutputStream out = connection.getOut();
        DataInputStream in = connection.getIn();
            ArrayList<String> pathlist = new ArrayList<>();
            StringBuilder currentPath = new StringBuilder();

            // 按 "/" 拆分路径
            String[] parts = path.split("/");
            for (String part : parts) {
                if (!part.isEmpty()) { // 忽略空部分
                    currentPath.append("/").append(part);
                    pathlist.add(currentPath.toString());
                }
            }

            // 如果路径为 "/", 保证至少包含根路径
            if (pathlist.isEmpty() && path.equals("/")) {
                pathlist.add("/");
            }
        for (int i=0 ; i<pathlist.size() ; i++){
                // 发送 CREATE_FILE 操作码
            String specificPath = pathlist.get(i);
            if (i==pathlist.size()-1) {
                MetaOpCode.CREATE_FILE.write(out);
                out.writeUTF(specificPath);       // 文件路径
                out.writeUTF(Config.USER); // 用户
                out.writeBoolean(false);  // 标记为文件
                out.flush();
            } else {
                MetaOpCode.CREATE_FILE.write(out);
                out.writeUTF(specificPath);       // 文件路径
                out.writeUTF(Config.USER); // 用户
                out.writeBoolean(true);  // 标记为文件
                out.flush();
            }

                // 接收响应
                int retCode = in.readInt();
                String msg = in.readUTF();
                if (i==pathlist.size()-1) {
                    if (retCode == 0) {
                        System.out.println("Metadata server success: " + msg);
                        return retCode; // 返回存储节点信息
                    } else {
                        System.err.println("Metadata server error: " + msg);
                        return retCode;
                    }
                } else {
                    if (retCode == 0) {
                        System.out.println("Metadata server success: " + msg);
                    } else {
                        System.err.println("Metadata server error: " + msg);
                    }
                }
            }
        return 0;
        } catch (IOException e) {
            System.err.println("Error communicating with metadata server: " + e.getMessage());
            throw e;
        }
    }


    public boolean closeFile(String path) throws IOException {
        DataOutputStream out = connection.getOut();
        DataInputStream in = connection.getIn();

        try {
            // 发送 CLOSE_FILE 操作码
            MetaOpCode.CLOSE_FILE.write(out);
            out.writeUTF(path); // 发送文件路径
            out.flush();

            // 接收响应
            int retCode = in.readInt();
            String message = in.readUTF();
            if (retCode == 0) {
                System.out.println("Metadata server response: " + message);
                return true;
            } else {
                System.err.println("Metadata server error: " + message);
                return false;
            }
        } catch (IOException e) {
            System.err.println("Error communicating with metadata server: " + e.getMessage());
            throw e;
        }
    }


    public List<String> getFileLocations(String path) {
        List<String> locations = null;
        try {
            MetaOpCode.GET_FILE_LOCATIONS.write(connection.getOut()); //
            connection.flush();
            connection.writeUTF(path); // 读取客户端发送路径
            connection.flush();
            // 读取List<String>
            int size = connection.readInt();
            locations = new ArrayList<>();
            for(int i = 0;i<size;i++){
                locations.add(connection.readUTF());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return locations;
    }

    public boolean deleteFile(String path) throws IOException {
        DataOutputStream out = connection.getOut();
        DataInputStream in = connection.getIn();

        try {
            // 发送 DEL_FILE 操作码
            MetaOpCode.DEL_FILE.write(out);
            out.writeUTF(path); // 发送文件路径
            out.writeBoolean(false); // 标记为文件删除
            out.flush();

            // 接收响应
            int retCode = in.readInt();
            String message = in.readUTF();
            if (retCode == 0) {
                System.out.println("Metadata server response: " + message);
                return true;
            } else {
                System.err.println("Metadata server error: " + message);
                return false;
            }
        } catch (IOException e) {
            System.err.println("Error communicating with metadata server: " + e.getMessage());
            throw e;
        }
    }


    public boolean createDirectory(String path) {
        try {
            DataOutputStream out = connection.getOut();
            DataInputStream in = connection.getIn();

            ArrayList<String> pathlist = new ArrayList<>();
            StringBuilder currentPath = new StringBuilder();

            // 按 "/" 拆分路径
            String[] parts = path.split("/");
            for (String part : parts) {
                if (!part.isEmpty()) { // 忽略空部分
                    currentPath.append("/").append(part);
                    pathlist.add(currentPath.toString());
                }
            }

            // 如果路径为 "/", 保证至少包含根路径
            if (pathlist.isEmpty() && path.equals("/")) {
                pathlist.add("/");
            }
            for(int i=0;i<pathlist.size();i++){
                // 发送 CREATE_FILE 操作码
                String specificPath = pathlist.get(i);
                MetaOpCode.CREATE_FILE.write(out);
                out.flush();
                out.writeUTF(specificPath);       // 目录路径
                out.writeUTF(Config.USER); // 用户
                out.writeBoolean(true);   // 标记为目录
                out.flush();

                // 读取响应
                int retCode = in.readInt();
                String msg = in.readUTF();

                if (i!=pathlist.size()-1) {
                    if (retCode == 0) {
                        System.out.println("Directory created successfully: " + path);
                    } else {
                        System.err.println("Failed to create directory: " + msg);
                    }
                } else {
                    if (retCode == 0) {
                        System.out.println("Directory created successfully: " + path);
                        return true;
                    } else {
                        System.err.println("Failed to create directory: " + msg);
                        return false;
                    }
                }
            }
            return true;
        } catch (IOException e) {
            System.err.println("Failed to create directory(IOException): " + path);
            e.printStackTrace();
            return false;
        }
    }

    public boolean deleteDirectory(String path) {
        try {
            DataOutputStream out = connection.getOut();
            DataInputStream in = connection.getIn();

            // 发送 DEL_FILE 操作码
            MetaOpCode.DEL_FILE.write(out);
            out.flush();
            out.writeUTF(path);       // 目录路径
//            out.writeBoolean(true);   // 标记为目录删除
            out.flush();

            // 读取响应
            int retCode = in.readInt();
            String msg = in.readUTF();

            if (retCode == 0) {
                System.out.println("Directory deleted successfully: " + path);
                return true;
            } else {
                System.err.println("Failed to delete directory: " + msg);
                return false;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long getFileSize(String path) throws IOException {
        DataOutputStream out = connection.getOut();
        DataInputStream in = connection.getIn();

        // 发送 GET_FILE_SIZE 操作码
        MetaOpCode.GET_FILE_SIZE.write(out);
        out.writeUTF(path); // 文件或目录路径
        out.flush();

        // 读取响应
        int retCode = in.readInt();
        if (retCode == 0) {
            return in.readLong(); // 返回大小
        } else {
            String errorMsg = in.readUTF();
            System.err.println("Failed to get file size: " + errorMsg);
            return -1;
        }
    }

    public boolean setFileSize(String path,long size) throws IOException {
        DataOutputStream out = connection.getOut();
        DataInputStream in = connection.getIn();
        MetaOpCode.SET_FILE_SIZE.write(out);
        out.writeUTF(path);
        out.writeLong(size);
        out.flush();
        int retCode = in.readInt();
        String errorMsg = in.readUTF();
        if (retCode == 0) {
            return true; // 返回大小
        } else {
            System.err.println("Failed to get file size: " + errorMsg);
            return false;
        }
    }

    public FileInfo getFileInfo(String path) throws IOException {
        DataOutputStream out = connection.getOut();
        DataInputStream in = connection.getIn();

        // 发送 GET_FILE_INFO 操作码
        MetaOpCode.GET_FILE_INFO.write(out);
        out.writeUTF(path); // 发送文件或目录路径
        out.flush();

        // 读取响应
        int retCode = in.readInt();
        String serializedFileInfo = in.readUTF();// 接收序列化的 FileInfo 对象
        if (retCode == 0) {
            return FileInfo.deserialize(serializedFileInfo); // 反序列化为 FileInfo 对象
        } else {
//            String errorMsg = in.readUTF();
            System.err.println("Failed to get file info ");
            return null;
        }
    }



    public boolean copyFile(String sourcePath, String destPath) throws IOException {
        DataOutputStream out = connection.getOut();
        DataInputStream in = connection.getIn();

        // 发送 COPY_FILE 操作码
        MetaOpCode.COPY_FILE.write(out);
        out.writeUTF(sourcePath); // 源文件路径
        out.writeUTF(destPath);   // 目标文件路径
        out.flush();

        // 读取响应
        int retCode = in.readInt();
        String msg = in.readUTF();
        if (retCode == 0) {
            System.out.println("File copied successfully from " + sourcePath + " to " + destPath);
            return true;
        } else {
            System.err.println("Failed to copy file: " + msg);
            return false;
        }
    }

    public boolean moveFile(String sourcePath, String destPath) throws IOException {
        DataOutputStream out = connection.getOut();
        DataInputStream in = connection.getIn();

        // 发送 MOVE_FILE 操作码
        MetaOpCode.MOVE_FILE.write(out);
        out.writeUTF(sourcePath); // 源文件路径
        out.writeUTF(destPath);   // 目标文件路径
        out.flush();

        // 读取响应
        int retCode = in.readInt();
        String msg = in.readUTF();
        if (retCode == 0) {
            System.out.println("File moved successfully from " + sourcePath + " to " + destPath);
            return true;
        } else {
            System.err.println("Failed to move file: " + msg);
            return false;
        }
    }

    public byte[] downloadFile(String remotePath) throws IOException {
        DataOutputStream out = connection.getOut();
        DataInputStream in = connection.getIn();

        // 发送 GET_FILE_LOCATIONS 操作码
        MetaOpCode.GET_FILE_LOCATIONS.write(out);
        out.writeUTF(remotePath); // 发送文件路径
        out.flush();

        // 读取存储节点位置
        int locationSize = in.readInt();
        if (locationSize == 0) {
            System.err.println("File not found or no storage nodes available: " + remotePath);
            return null;
        }

        // 使用第一个位置
        String location = in.readUTF();
        String[] nodeInfo = location.split(":");
        String nodeHost = nodeInfo[0];
        String fileId = nodeInfo[1];

        // 转发下载请求到存储节点
        try (Connection connection = new Connection(nodeHost, Config.DATA_SERVRE_PORT)) {
            DataOutputStream dataOut = connection.getOut();
            DataInputStream dataIn = connection.getIn();

            DataOpCode.READ_FILE.write(dataOut); // 发送读取文件的操作码
            dataOut.writeUTF(fileId);            // 发送文件 ID
            dataOut.flush();

            int retCode = dataIn.readInt();
            if (retCode != 0) {
                System.err.println("Failed to download file: " + dataIn.readUTF());
                return null;
            }

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            int chunkSize;
            while ((chunkSize = dataIn.readInt()) != -1) {
                byte[] buffer = new byte[chunkSize];
                dataIn.readFully(buffer);
                bos.write(buffer);
            }

            return bos.toByteArray();
        } catch (IOException e) {
            System.err.println("Error during file download: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }


    public void close() throws IOException {
        this.connection.close();
    }

    // 假设还有其他辅助方法的实现...
}

