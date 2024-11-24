package com.ucas.bigdata.client;

import com.ucas.bigdata.common.Config;
import com.ucas.bigdata.common.DataOpCode;
import com.ucas.bigdata.common.FileInfo;
import com.ucas.bigdata.implement.DataServer;
import com.ucas.bigdata.implement.StorageNode;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class DistributedFileSystemClient {
    private static Logger log = LogManager.getLogger(DataServer.class);

    private MetaServerClient metaDataClient;
    private List<StorageNode> storageNodes;
    private String cur_dir = "/";

    public DistributedFileSystemClient() throws IOException {
        metaDataClient = new MetaServerClient();
        storageNodes = new ArrayList();
        // 初始化存储节点列表
//        storageNodes.add(new StorageNode("storage-node-1"));
        //storageNodes.add(new StorageNode("storage-node-2"));
        // ...
    }


    public void disconnect() throws IOException {
        // 客户端连接关闭操作
        metaDataClient.close();
    }

    public DataInputStream openFile(String path) {
        try {
            // 获取文件存储位置
            List<String> locations = metaDataClient.getFileLocations(path);
            if (locations.isEmpty()) {
                System.err.println("File not found or no storage nodes available: " + path);
                return null;
            }

            // 连接到第一个存储节点
            String[] nodeInfo = locations.get(0).split(":");
            String nodeHost = nodeInfo[0];
            int nodePort = Config.DATA_SERVRE_PORT;

            Connection connection = new Connection(nodeHost, nodePort);
            DataOutputStream out = connection.getOut();
            DataInputStream in = connection.getIn();

            // 发送打开文件请求
            DataOpCode.READ_FILE.write(out);
            out.writeUTF(path); // 发送文件路径
            out.writeLong(0);   // 从文件头开始读取
            out.flush();

            // 检查响应状态
            int retCode = in.readInt();
            if (retCode != 0) {
                System.err.println("Failed to open file: " + in.readUTF());
                connection.close();
                return null;
            }

            // 返回文件数据流
            return in;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public byte[] readFile(String path, long offset, int len) {
        // 实现从文件读取数据的逻辑
        try {
            // 获取文件存储位置
            List<String> locations = metaDataClient.getFileLocations(path);
            if (locations.isEmpty()) {
                System.err.println("No storage nodes available for file: " + path);
                return null;
            }
            String[] nodeInfo = locations.get(0).split(":");
            String nodeHost = nodeInfo[0];

            // 连接到数据服务器请求文件数据
            Connection connection = new Connection(nodeHost, Config.DATA_SERVRE_PORT);
            DataOutputStream out = connection.getOut();
            DataInputStream in = connection.getIn();

            // 读文件请求
            DataOpCode.READ_FILE.write(out);
            out.writeUTF(path);  // 文件路径
            out.writeLong(offset); // 读取的起始位置
            out.flush();

            // 接收服务器响应
            int retCode = in.readInt();
            if (retCode != 0) {
                System.err.println("Failed to read file: " + in.readUTF());
                connection.close();
                return null;
            }

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            int chunkSize;
            while ((chunkSize = in.readInt()) != -1) {
                byte[] buffer = new byte[chunkSize];
                in.readFully(buffer);
                bos.write(buffer);
            }

            connection.close();
            return bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 将data数据写至分布式文件系统路径path
     * @param path
     * @param data
     * @return
     */
    public boolean writeFile(String path, byte[] data) {
        try {

            // 向元数据服务器发送新建文件请求，返回fileId
            String fileId = metaDataClient.createFile(path);

            // 获取存储节点地址
            List<String> locations = metaDataClient.getFileLocations(path);
            if (locations.isEmpty()) {
                System.err.println("No storage nodes available for file: " + path);
                return false;
            }
            String[] nodeInfo = locations.get(0).split(":");
            String nodeHost = nodeInfo[0];

            // 创建套接字连接到数据服务器
            Connection connection = new Connection(nodeHost, Config.DATA_SERVRE_PORT);


            DataOpCode.WRITE_FILE.write(connection.getOut());//0.发送写文件的OPCode
            connection.writeUTF(fileId);//1.发送文件ID
            connection.write(data);//2.遍历写入

            int retCode = connection.readInt();//3.回写返回码
            String msg = connection.readUTF();//4.回写消息
            if(retCode == 0){
                log.info("写入成功，"+msg);
            }else{
                log.error("写入失败，"+msg);
            }
            connection.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean closeFile(String path) {
        // 调用元数据服务器，通知关闭文件
        boolean result = metaDataClient.closeFile(path);
        if (!result) {
            System.err.println("Failed to close file on metadata server: " + path);
            return false;
        }
        System.out.println("File closed successfully: " + path);
        return true;
    }

    private void listFiles(String path) {
        List<String> fileList = metaDataClient.listFiles(path); // 假设有这个方法来获取文件列表

        if (fileList.isEmpty()) {
            System.out.println("No files found.");
        } else {
            System.out.println("Files in the system:");
            for (String file : fileList) {
                System.out.println(file);
            }
        }
    }

    // 获取文件大小
    private long getFileSize(String filePath) {
        // 实现获取文件大小的逻辑
        return 0; // 假设获取文件大小的方法
    }


    public boolean downloadFile(String remoteFilePath, String localFilePath) {
        // 实现下载文件逻辑
        return false;
    }


    public boolean deleteFile(String path) {
        try {
            // 向元数据服务器发送删除请求
            boolean metaDeleteSuccess = metaDataClient.deleteFile(path);
            if (!metaDeleteSuccess) {
                System.err.println("Failed to delete file metadata: " + path);
                return false;
            }

            // 获取文件的存储位置
            List<String> locations = metaDataClient.getFileLocations(path);
            if (locations.isEmpty()) {
                System.err.println("No storage nodes found for file: " + path);
                return true; // 如果没有存储位置，说明文件数据可能已被清理
            }

            // 通知所有存储节点删除文件
            for (String location : locations) {
                String[] nodeInfo = location.split(":");
                String nodeHost = nodeInfo[0];
                int nodePort = Config.DATA_SERVRE_PORT;

                Connection connection = new Connection(nodeHost, nodePort);
                DataOutputStream out = connection.getOut();
                DataInputStream in = connection.getIn();

                // 删除文件请求
                DataOpCode.DEL_FILE.write(out);
                out.writeUTF(path);
                out.flush();

                // 读取响应
                int retCode = in.readInt();
                String msg = in.readUTF();
                connection.close();

                if (retCode != 0) {
                    System.err.println("Failed to delete file on node " + nodeHost + ": " + msg);
                } else {
                    System.out.println("File deleted successfully on node " + nodeHost + ": " + msg);
                }
            }

            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }



    public boolean createDirectory(String remoteDirectoryPath) {
        // 实现创建目录逻辑
        return false;
    }

    public boolean deleteDirectory(String remoteDirectoryPath) {
        // 实现删除目录逻辑
        return false;
    }

    public FileInfo getFileInfo(String remoteFilePath) {
        // 实现获取文件信息逻辑
        return null;
    }
    public boolean copyFile(String sourcePath, String destinationPath) {
        // 实现复制文件逻辑
        return false;
    }
    public boolean moveFile(String sourcePath, String destinationPath) {
        // 实现移动文件逻辑
        return false;
    }

    // 与存储节点断开连接
    private void disconnectFromStorageNodes() {
        // 实现断开连接逻辑
    }

    // 其他辅助方法的实现...

    public static void main(String[] args) throws IOException {
        DistributedFileSystemClient client = new DistributedFileSystemClient();

        Scanner scanner = new Scanner(System.in);

        boolean running = true;
        while (running) {
            System.out.println("Enter command (open, read, write, close, exit):");
            String command = scanner.nextLine();

            switch (command.toLowerCase()) {
                case "open":
                    System.out.println("Enter file path:");
                    String path = scanner.nextLine();
                    DataInputStream fileStream = client.openFile(path);
                    if (fileStream != null) {
                        System.out.println("File opened.");
                    } else {
                        System.out.println("Failed to open file.");
                    }
                    break;
                case "cd":
                    System.out.println("Enter path:");
                    String rel_path = scanner.nextLine().trim();
                    if(!rel_path.startsWith("/")) {
                        //@todo 检查路径
                        client.cur_dir = client.cur_dir + File.separator + rel_path;
                    }
                case "read":
                    // 实现从文件读取数据的逻辑
                    System.out.println("Enter file path:");
                    String readPath = scanner.nextLine().trim();
                    byte[] fileData = client.readFile(readPath, 0, 1024);
                    if (fileData != null) {
                        System.out.println("File content: " + new String(fileData));
                    } else {
                        System.out.println("Failed to read file or file does not exist.");
                    }
                    break;

                case "write":
                    // 实现向文件写入数据的逻辑
                    System.out.println("Enter file path:");
                    String writePath = scanner.nextLine().trim();
                    System.out.println("Enter file content:");
                    String fcontent = scanner.nextLine();
                    boolean writeSuccess = client.writeFile(writePath, fcontent.getBytes());
                    System.out.println("Write result: " + (writeSuccess ? "Success" : "Failed"));
                    break;

                case "ls":
                    client.listFiles(client.cur_dir);
                    break;

                case "cat":
                    System.out.println("Enter file path:");
                    String f = scanner.nextLine();
                    byte[] content = client.readFile(f,0,0);
                    System.out.println(new String(content));
                    break;

                case "close":
                    // 实现关闭文件的逻辑
                    System.out.println("Enter file path:");
                    String closePath = scanner.nextLine().trim();
                    boolean closeSuccess = client.closeFile(closePath);
                    System.out.println("Close result: " + (closeSuccess ? "Success" : "Failed"));
                    break;

                case "delete":
                    System.out.println("Enter file path:");
                    String deletePath = scanner.nextLine().trim();
                    boolean deleteSuccess = client.deleteFile(deletePath);
                    System.out.println("Delete result: " + (deleteSuccess ? "Success" : "Failed"));
                    break;

                case "exit":
                    running = false;
                    break;

                default:
                    System.out.println("Invalid command.");
                    break;
            }
        }

        client.disconnect();
        scanner.close();
    }
}

