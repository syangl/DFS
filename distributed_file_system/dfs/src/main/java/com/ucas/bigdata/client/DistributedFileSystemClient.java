package com.ucas.bigdata.client;

import com.ucas.bigdata.common.Config;
import com.ucas.bigdata.common.DataOpCode;
import com.ucas.bigdata.common.MetaOpCode;
import com.ucas.bigdata.common.FileInfo;
import com.ucas.bigdata.implement.DataServer;
import com.ucas.bigdata.implement.StorageNode;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public class DistributedFileSystemClient {
    private static Logger log = LogManager.getLogger(DataServer.class);

    private MetaServerClient metaDataClient;
    private List<Connection> storageNodeConnections;
    private List<StorageNode> storageNodes;
    private String cur_dir = "/";

    public DistributedFileSystemClient() throws IOException {
        metaDataClient = new MetaServerClient();
        storageNodes = new ArrayList();
        storageNodeConnections = new ArrayList<>();
        // 初始化存储节点列表
//        storageNodes.add(new StorageNode("storage-node-1"));
        //storageNodes.add(new StorageNode("storage-node-2"));
        // ...
    }


    public void disconnect() throws IOException {
        // 客户端连接关闭操作
        metaDataClient.close();
    }


    private boolean checkInfo(String relPath) throws IOException {
        FileInfo info = metaDataClient.getFileInfo(relPath);
        if (info == null) {
            System.err.println("No storage nodes available for directory: " + relPath);
            return false;
        } else {
            System.out.println("File info: " + info.serialize());
            return true;
        }
    }

    public boolean createFile(String path) {
        try {
            // 向元数据服务器发送创建文件请求，返回存储节点信息
            Integer metaResult = metaDataClient.createFile(path);
            if (metaResult == -1) {
                System.err.println("Failed to create file on metadata server: " + path);
//                return false;
            }

            List<String> locations = metaDataClient.getFileLocations(path);
            if (locations.isEmpty()) {
                System.err.println("No storage nodes available for directory: " + path);
                return false;
            }

            HashMap<String, Integer> operationCodes = new HashMap<>();
            for (String location : locations) {
                // 解析存储节点信息
                String[] nodeInfo = location.split(":");
//            String nodeHost = nodeInfo[0];  TODO
                String nodeHost = "localhost";
                String fileId = nodeInfo[1];

                // 3. 连接数据服务器并初始化文件
                try (Connection connection = new Connection(nodeHost, Config.DATA_SERVRE_PORT)) {
                    DataOutputStream out = connection.getOut();
                    DataInputStream in = connection.getIn();

                    DataOpCode.CREATE_FILE.write(out); // 发送写文件操作码
                    out.writeUTF(fileId);             // 发送文件 ID
//                byte[] buffer = new byte[1024];
                    out.writeInt(0);    // 传输的文件总长度
//                out.write(buffer, 0, 0);           // 写入空文件数据
                    out.write(new byte[0], 0, 0);  // 写入空文件数据
                    out.flush();

                    int retCode = in.readInt();
                    String msg = in.readUTF();
                    operationCodes.put(location, retCode);
                    connection.close();
                    if (retCode == 0) {
                        System.out.println("File created successfully on data server: " + nodeHost + " " + path);
//                        return true;
                    } else {
                        System.err.println("Failed to create file on data server: " + nodeHost + " " + msg);
//                        return false;
                    }
                }
            }
            for (String location : operationCodes.keySet()) {
                int retCode = operationCodes.get(location);
                if (retCode != 0) {
                    String nodeHost = location.split(":")[0];
                    // TODO
                    nodeHost = "localhost";
                    System.err.println("Failed to create file on data server: " + nodeHost);
                    System.err.println("Try to recover: " + nodeHost);
                    if(!deleteFile(path)) {
                        System.err.println("Serious Storage Node Error!");
                        System.exit(-1);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public DataInputStream openFile(String path) {
        try {
            // 1. 获取文件存储位置（调用 MetadataServer 的 GET_FILE_LOCATIONS）
            List<String> locations = metaDataClient.getFileLocations(path);
            if (locations == null || locations.isEmpty()) {
                System.err.println("No storage nodes available for file: " + path);
                return null;
            }

            // 2. 从存储位置中解析第一个 DataServer
            String[] nodeInfo = locations.get(0).split(":");
//            String nodeHost = nodeInfo[0]; TODO
            String nodeHost = "localhost";
            String fileId = nodeInfo[1];

            // 3. 与 DataServer 建立连接
            Connection connection = new Connection(nodeHost, Config.DATA_SERVRE_PORT);
            DataOutputStream out = connection.getOut();
            DataInputStream in = connection.getIn();

            // 4. 向 DataServer 发送 READ_FILE 请求
            DataOpCode.OPEN_FILE.write(out); // 发送操作码
            out.writeUTF(fileId);           // 发送文件 ID
            out.flush();

            // 5. 检查 DataServer 的响应状态
            int retCode = in.readInt();
            if (retCode != 0) {
                String errorMsg = in.readUTF();
                System.err.println("Failed to open file: " + errorMsg);
                connection.close();
                return null;
            }

            // 6. 返回 DataInputStream，以便后续读取操作
            System.out.println("File opened successfully: " + path);
            return in;
        } catch (IOException e) {
            System.err.println("Error during file opening: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }


    public byte[] readFile(String path, long offset, int len) {
        try {
            // 1. 获取文件存储位置
            List<String> locations = metaDataClient.getFileLocations(path);
            if (locations.isEmpty()) {
                System.err.println("No storage nodes available for file: " + path);
                return null;
            }

            for (String location : locations) {
                // 2. 连接到数据服务器
                String[] nodeInfo = location.split(":");
//            String nodeHost = nodeInfo[0];  TODO
                String nodeHost = "localhost";
                String fileId = nodeInfo[1];
                int nodePort = Config.DATA_SERVRE_PORT;

                try (Connection connection = new Connection(nodeHost, nodePort)) {
                    DataOutputStream out = connection.getOut();
                    DataInputStream in = connection.getIn();

                    // 3. 发送 READ_FILE 请求
                    DataOpCode.READ_FILE.write(out);
                    out.writeUTF(fileId);  // 文件路径
                    out.writeLong(offset); // 起始位置
                    out.flush();

                    // 4. 接收响应
                    int retCode = in.readInt();
                    String msg = in.readUTF();
                    if (retCode != 0) {
                        System.err.println("Failed to read file: " + in.readUTF());
                        continue;
                    }

                    // 5. 读取文件内容
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    int chunkSize;
                    while ((chunkSize = in.readInt()) != -1) {
                        byte[] buffer = new byte[chunkSize];
                        in.readFully(buffer);
                        bos.write(buffer);
                    }
                    connection.close();
                    return bos.toByteArray();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return null;
    }

    /**
     * 将data数据写至分布式文件系统路径path
     * @param path
     * @param data
     * @return
     */
    public boolean writeFile(String path, byte[] data) {
        try {
            // 获取存储节点地址
            List<String> locations = metaDataClient.getFileLocations(path);
            if (locations.isEmpty()) {
                System.err.println("No storage nodes available for file: " + path);
                return false;
            }

            for (String location : locations) {
                String[] nodeInfo = location.split(":");
//            String nodeHost = nodeInfo[0];  TODO
                String fileId = nodeInfo[1];
                String nodeHost = "localhost";

                boolean setResult = metaDataClient.setFileSize(path, data.length);
                if (!setResult) {
                    System.err.println("Failed to set file size: " + path);
                    return false;
                }


                // 创建套接字连接到数据服务器
                Connection connection = new Connection(nodeHost, Config.DATA_SERVRE_PORT);


                DataOpCode.WRITE_FILE.write(connection.getOut());//0.发送写文件的OPCode
                connection.writeUTF(fileId);//1.发送文件ID
                connection.write(data);//2.遍历写入


                int retCode = connection.readInt();//3.回写返回码
                String msg = connection.readUTF();//4.回写消息
                if (retCode == 0) {
                    log.info("写入成功，" + msg);
                } else {
                    log.error("写入失败，" + msg);
                    System.err.println("Serious Storage Node Error!");
                    System.exit(-1);
                }
                connection.close();
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public boolean closeFile(String path) {
        try {
            boolean result = metaDataClient.closeFile(path); // 调用 MetaServerClient 的逻辑
            if (result) {
                System.out.println("File closed successfully: " + path);
            } else {
                System.err.println("Failed to close file: " + path);
            }
            return result;
        } catch ( Exception e) {
            System.err.println("Error closing file: " + path);
            e.printStackTrace();
            return false;
        }
    }


    public void listFiles(String path) {
        try {
            // 调用 MetaServerClient 获取文件列表
            List<String> fileList = metaDataClient.listFiles(path);
            if (fileList.isEmpty()) {
                System.out.println("No files found in directory: " + path);
            } else {
                System.out.println("Files in directory " + path + ":");
                for (String file : fileList) {
                    int lastSeparatorIndex = file.lastIndexOf('/');
                    file = file.substring(lastSeparatorIndex+1, file.length());
                    System.out.println(file);
                }
            }
        } catch (Exception e) {
            System.err.println("Error listing files in directory: " + path);
            e.printStackTrace();
        }
    }


    // 获取文件大小
    public long getFileSize(String path) {
        try {
            // 1. 向元数据服务器请求文件或目录大小
            long size = metaDataClient.getFileSize(path);
            if (size >= 0) {
                System.out.println("Size of " + path + ": " + size + " bytes");
                return size;
            } else {
                System.err.println("Failed to get size from MetadataServer for: " + path);
                return -1;
            }
        } catch (IOException e) {
            System.err.println("Error while getting file size for: " + path);
            e.printStackTrace();
            return -1;
        }
    }



    public boolean downloadFile(String remotePath, String localPath) {
        try {
            // 获取文件存储位置
            List<String> locations = metaDataClient.getFileLocations(remotePath);
            if (locations == null || locations.isEmpty()) {
                System.err.println("No storage nodes available for file: " + remotePath);
                return false;
            }

            for (String location : locations) {
                // 2. 从存储位置中解析第一个 DataServer
                String[] nodeInfo = location.split(":");
//            String nodeHost = nodeInfo[0];  TODO
                String nodeHost = "localhost";
                String fileId = nodeInfo[1];

                // 与 DataServer 建立连接
                try (Connection connection = new Connection(nodeHost, Config.DATA_SERVRE_PORT)) {
                    DataOutputStream out = connection.getOut();
                    DataInputStream in = connection.getIn();

                    // 向 DataServer 发送 READ_FILE 请求
                    DataOpCode.DOWNLOAD_FILE.write(out); // 发送操作码
                    out.writeUTF(fileId);           // 发送文件 ID
                    out.flush();

                    // 将文件数据保存到本地
                    try (FileOutputStream fos = new FileOutputStream(localPath)) {
                        int chunkSize;
                        while ((chunkSize = in.readInt()) >= 0) {
                            byte[] buffer = new byte[chunkSize];
                            in.read(buffer, 0, chunkSize);
                            fos.write(buffer);
                        }
                        if (chunkSize == -2) {
                            String errorMsg = in.readUTF();
                            System.err.println("Failed to download file: " + errorMsg);
                            fos.close();
                            connection.close();
                        } else if (chunkSize == -1) {
                            System.out.println("File downloaded successfully to: " + localPath);
                            fos.close();
                            connection.close();
                            return true;
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error during file download: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
        return false;
    }



    public boolean deleteFile(String path) {
        try {
            // 获取文件存储位置
            List<String> locations = metaDataClient.getFileLocations(path);
            if (locations.isEmpty()) {
                System.err.println("No storage nodes found for file: " + path);
                return true; // 如果没有存储位置，说明文件数据可能已被清理
            }

            // 通知元数据服务器删除文件元数据
            boolean metaDeleteSuccess = metaDataClient.deleteFile(path);
            if (!metaDeleteSuccess) {
                System.err.println("Failed to delete file metadata: " + path);
                return false;
            }

            // 通知数据服务器删除文件数据
            for (String location : locations) {
                String[] nodeInfo = location.split(":");
//                String nodeHost = nodeInfo[0];  TODO
                String nodeHost = "localhost";
                String fileId = nodeInfo[1];

                try (Connection connection = new Connection(nodeHost, Config.DATA_SERVRE_PORT)) {
                    DataOutputStream out = connection.getOut();
                    DataInputStream in = connection.getIn();

                    DataOpCode.DEL_FILE.write(out); // 发送删除文件操作码
                    out.writeUTF(fileId);          // 发送文件 ID
                    out.flush();

                    int retCode = in.readInt();
                    String msg = in.readUTF();
                    connection.close();
                    if (retCode == 0) {
                        System.out.println("File deleted successfully on node " + nodeHost + ": " + msg);
                    } else {
                        System.err.println("Failed to delete file on node " + nodeHost + ": " + msg);
                    }
                }
            }

            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }




    public boolean createDirectory(String path) throws IOException {
        // 向元数据服务器发送创建目录请求
        boolean metaResult = metaDataClient.createDirectory(path);
        if (!metaResult) {
            System.err.println("Failed to create directory on metadata server: " + path);
            return false;
        }
        return true;
    }

    public boolean deleteDirectory(String path) {
        // 先读取元数据服务器上的目录信息
        List<String> locations = metaDataClient.getFileLocations(path);

        // 再删除元数据服务器上的目录信息（如果先删除meta数据，则后面就获取不到locations导致无法删除datanode上的目录了）
        boolean metaResult = metaDataClient.deleteDirectory(path);
        if (!metaResult) {
            System.err.println("Failed to delete directory on metadata server: " + path);
            return false;
        }

        for (String location : locations) {
            String[] nodeInfo = location.split(":");
//                String nodeHost = nodeInfo[0]; TODO
            String nodeHost = "localhost";
//            try (Connection connection = new Connection(nodeHost, Config.DATA_SERVRE_PORT)) {
//                DataOutputStream out = connection.getOut();
//                DataInputStream in = connection.getIn();
//
//                DataOpCode.DELETE_DIRECTORY.write(out); // 发送删除目录操作码
//                out.writeUTF(path); // 发送目录路径
//                out.flush();
//
//                int retCode = in.readInt();
//                String msg = in.readUTF();
//
//                connection.close();
//                if (retCode != 0) {
//                    System.err.println("Failed to delete directory on node " + nodeHost + ": " + msg);
//                } else {
//                    System.out.println("Directory deleted successfully on node " + nodeHost + ": " + msg);
//                }
//            } catch (IOException e) {
//                System.err.println("Failed to connect to data server: " + nodeHost);
//                e.printStackTrace();
//            }
        }
        return true;
    }

    public FileInfo getFileInfo(String path) {
        try {
            List<String> locations = metaDataClient.getFileLocations(path);
            if (locations == null || locations.isEmpty()) {
                System.err.println("No storage nodes available for file: " + path);
                return null;
            }
            return metaDataClient.getFileInfo(path); // 调用 MetaServerClient 的逻辑
        } catch (IOException e) {
            System.err.println("Failed to get file info for: " + path);
            e.printStackTrace();
            return null;
        }
    }
    public boolean copyFile(String sourcePath, String destPath) {
        try {
            // 删除旧的目标文件
            List<String> oldLocations = metaDataClient.getFileLocations(destPath);
            if (!oldLocations.isEmpty()) {
                for (String oldLocation : oldLocations){
                    String[] destNodeInfo = oldLocation.split(":");
//            String  oldNodeHost =  oldNodeInfo[0];  TODO
                    String  oldNodeHost = "localhost";
                    String  oldFileId = destNodeInfo[1];

                    try (Connection connection = new Connection(oldNodeHost, Config.DATA_SERVRE_PORT)) {
                        DataOutputStream out = connection.getOut();
                        DataInputStream in = connection.getIn();

                        DataOpCode.DEL_FILE.write(out); // 发送删除文件操作码
                        out.writeUTF(oldFileId);          // 发送文件 ID
                        out.flush();

                        int retCode = in.readInt();
                        String msg = in.readUTF();
                        if (retCode != 0) {
                            System.err.println("Serious Storage Node Error!");
                            System.exit(-1);
                        }
                    }
                }
            }

            // 调用 MetadataServer 处理元数据复制
            boolean metaResult = metaDataClient.copyFile(sourcePath, destPath);
            if (!metaResult) {
                System.err.println("Failed to copy file on metadata server.");
                return false;
            }

            // 获取目标文件存储节点
            List<String> destLocations = metaDataClient.getFileLocations(destPath);
            if (destLocations == null || destLocations.isEmpty()) {
                System.err.println("No storage nodes available for file: " + destPath);
                return false;
            }
            for (String destLocation : destLocations) {
                String[] destNodeInfo = destLocation.split(":");
//            String destNodeHost = destNodeInfo[0];  TODO
                String destNodeHost = "localhost";
                String destFileId = destNodeInfo[1];

                // 与 DataServer 建立连接
                List<String> sourceLocations = metaDataClient.getFileLocations(sourcePath);
                if (sourceLocations == null || sourceLocations.isEmpty()) {
                    System.err.println("No storage nodes available for file: " + sourcePath);
                    return false;
                }
                // 选择一个源文件存储节点
                int selectedSourceIndex = Math.abs(sourcePath.hashCode()) % sourceLocations.size();
                String sourceLocation = sourceLocations.get(selectedSourceIndex);
                String[] sourceNodeInfo = sourceLocation.split(":");
                String sourceNodeHost = sourceNodeInfo[0];
                String sourceFileId = sourceNodeInfo[1];

                try (Connection connection = new Connection(destNodeHost, Config.DATA_SERVRE_PORT)) {
                    DataOutputStream out = connection.getOut();
                    DataInputStream in = connection.getIn();

                    // 向 DataServer 发送 COPY_FILE 请求
                    DataOpCode.COPY_FILE.write(out); // 发送操作码
                    out.writeUTF(sourceFileId);      // 发送源文件 ID
                    out.writeUTF(destFileId);        // 发送目标文件 ID
                    out.flush();

                    // 检查 DataServer 的响应
                    int retCode = in.readInt();
                    String msg = in.readUTF();
                    if (retCode == 0) {
                        System.out.println("File copied successfully: " + sourcePath + " -> " + destPath);
//                            return true;
                    } else {
                        System.err.println("Serious Storage Node Error!");
                        System.exit(-1);
                    }
                }

            }
        } catch (IOException e) {
            System.err.println("Error during file copy: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean moveFile(String sourcePath, String destPath) {
        try {
            // mv f1 to f2，如果f2存在，删除f2原来的fileId文件并替换meta的fileInfo为f2的path，否则直接替换meta的fileInfo为f2的path即可
            // 获取目标文件的存储位置
            List<String> destLocations = metaDataClient.getFileLocations(destPath);
            if (!destLocations.isEmpty()) {  // 目标文件存在
                for (String destLocation : destLocations) {
                    String[] destNodeInfo = destLocation.split(":");
//                String destNodeHost = destNodeInfo[0];  TODO
                    String destNodeHost = "localhost";
                    String destFileId = destNodeInfo[1];
                    // 连接到数据服务器
                    try (Connection connection = new Connection(destNodeHost, Config.DATA_SERVRE_PORT)) {
                        DataOutputStream out = connection.getOut();
                        DataInputStream in = connection.getIn();

                        // 向数据服务器发送文件删除请求
                        DataOpCode.DEL_FILE.write(out);
                        out.writeUTF(destFileId);
                        out.flush();

                        // 接收数据服务器的响应
                        int retCode = in.readInt();
                        String msg = in.readUTF();
                        if (retCode == 0) {
                            System.out.println("File delete successfully on data server: " + sourcePath + " to " + destPath);
                        } else {
                            System.err.println("Serious Storage Node Error!");
                            System.exit(-1);
                        }
                    }
                }
            }

            // 向元数据服务器发送移动文件请求
            boolean metaResult = metaDataClient.moveFile(sourcePath, destPath);
            if (!metaResult) {
                System.err.println("Failed to move file on metadata server: " + sourcePath + " to " + destPath);
                return false;
            }
        } catch (IOException e) {
            System.err.println("Error during moveFile operation: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
        return true;
    }

    // 与存储节点断开连接
    public void disconnectFromStorageNodes() {
        for (Connection connection : storageNodeConnections) {
            try {
                connection.close();
                System.out.println("Disconnected from storage node: " + connection);
            } catch (IOException e) {
                System.err.println("Failed to disconnect from storage node: " + connection);
                e.printStackTrace();
            }
        }
        storageNodeConnections.clear();
    }

    public void connectToStorageNode(String nodeHost) throws IOException {
        Connection connection = new Connection(nodeHost, Config.DATA_SERVRE_PORT);
        storageNodeConnections.add(connection);
        System.out.println("Connected to storage node: " + nodeHost);
    }

    // 其他辅助方法的实现...
    public void changeDirectory(String path) {
        try {
            // 处理 "." 和 ".."
            if (path.equals(".")) {
                System.out.println("Current directory: " + cur_dir);
                return;
            }
            if (path.equals("..")) {
                cur_dir = cur_dir.lastIndexOf('/') > 0
                        ? cur_dir.substring(0, cur_dir.lastIndexOf('/'))
                        : "/";
                System.out.println("Current directory: " + cur_dir);
                return;
            }

            // 构建目标路径
            String targetPath = path.startsWith("/") ? path : cur_dir + "/" + path;

            // 去除多余的斜杠
            targetPath = targetPath.replaceAll("//+", "/");

            // 获取目标路径信息
            FileInfo fileInfo = metaDataClient.getFileInfo(targetPath);
            if (fileInfo == null) {
                System.err.println("Path not found: " + targetPath);
                return;
            }

            // 检查是否为目录
            if (!fileInfo.isDirectory()) {
                System.err.println("Not a directory: " + targetPath);
                return;
            }

            // 更新当前目录
            cur_dir = targetPath;
            System.out.println("Current directory: " + cur_dir);

        } catch (IOException e) {
            System.err.println("Error while changing directory: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        DistributedFileSystemClient client = new DistributedFileSystemClient();

        Scanner scanner = new Scanner(System.in);

        boolean running = true;
        while (running) {
            System.out.println("Enter command (open, read, write, close, exit):");
            String command = scanner.nextLine();
            switch (command.toLowerCase()) {
//                case "connect":
//                    System.out.println("Enter storage node host:");
//                    String nodeHost = scanner.nextLine().trim();
//                    try {
//                        client.connectToStorageNode(nodeHost);
//                    } catch (IOException e) {
//                        System.err.println("Failed to connect to storage node: " + nodeHost);
//                        e.printStackTrace();
//                    }
//                    break;
//
//                case "disconnect":
//                    client.disconnectFromStorageNodes();
//                    break;

                case "create":
                    System.out.print("Enter file name: ");
                    String filePath = scanner.nextLine().trim();
                    if (filePath.charAt(0) == '/' ) {
                        filePath = filePath.substring(1);
                    }
                    if (client.cur_dir=="/"){
                        filePath= "/"+filePath;
                    } else {
                        filePath=client.cur_dir+"/"+filePath;
                    }
                    if (client.createFile( filePath)) {
                        System.out.println("File created: " + filePath);
                    } else {
                        System.out.println("Failed to create file: " + filePath);
                    }
                    break;

                case "move":
//                    waitTODO
                    System.out.println("Enter source file path:");
                    String moveSourcePath = scanner.nextLine().trim();
                    System.out.println("Enter destination file path:");
                    String moveDestPath = scanner.nextLine().trim();
                    if (moveSourcePath.charAt(0) == '/' ) {
                        moveSourcePath = moveSourcePath.substring(1);
                    }
                    if (client.cur_dir=="/"){
                        moveSourcePath= "/"+moveSourcePath;
                    } else {
                        moveSourcePath=client.cur_dir+"/"+moveSourcePath;
                    }

                    if (moveDestPath.charAt(0) == '/' ) {
                        moveDestPath = moveDestPath.substring(1);
                    }
                    if (client.cur_dir=="/"){
                        moveDestPath= "/"+moveDestPath;
                    } else {
                        moveDestPath=client.cur_dir+"/"+moveDestPath;
                    }
                    if (client.moveFile(moveSourcePath, moveDestPath)) {
                        System.out.println("File moved successfully.");
                    } else {
                        System.err.println("File move failed.");
                    }
                    break;

                case "mkdir":
                    System.out.println("Enter directory path:");
                    String mkdirDirPath = scanner.nextLine().trim();
                    if (mkdirDirPath.charAt(0) == '/' ) {
                        mkdirDirPath = mkdirDirPath.substring(1);
                    }
                    if (client.cur_dir=="/"){
                        mkdirDirPath= "/"+mkdirDirPath;
                    } else {
                        mkdirDirPath=client.cur_dir+"/"+mkdirDirPath;
                    }

                    if (client.createDirectory(mkdirDirPath)) {
                        System.out.println("Directory created: " + mkdirDirPath);
                    } else {
                        System.err.println("Failed to create directory: " + mkdirDirPath);
                    }
                    break;

                case "rmdir":
                    System.out.println("Enter directory path:");
                    String rmdirDirPath = scanner.nextLine().trim();
                    if (rmdirDirPath.charAt(0) == '/' ) {
                        rmdirDirPath = rmdirDirPath.substring(1);
                    }
                    if (client.cur_dir=="/"){
                        rmdirDirPath= "/"+rmdirDirPath;
                    } else {
                        rmdirDirPath=client.cur_dir+"/"+rmdirDirPath;
                    }
                    if (client.deleteDirectory(rmdirDirPath)) {
                        System.out.println("Directory deleted: " + rmdirDirPath);
                    } else {
                        System.err.println("Failed to delete directory: " + rmdirDirPath);
                    }
                    break;

                case "size":
                    System.out.println("Enter file or directory path:");
                    String sizeDirPath = scanner.nextLine().trim();
                    if (sizeDirPath.charAt(0) == '/' ) {
                        sizeDirPath = sizeDirPath.substring(1);
                    }
                    if (client.cur_dir=="/"){
                        sizeDirPath= "/"+sizeDirPath;
                    } else {
                        sizeDirPath=client.cur_dir+"/"+sizeDirPath;
                    }
                    long size = client.getFileSize(sizeDirPath);
                    if (size >= 0) {
                        System.out.println("Size of " + sizeDirPath + ": " + size + " bytes");
                    } else {
                        System.err.println("Failed to get size of: " + sizeDirPath);
                    }
                    break;

                case "download":
                    System.out.println("Enter remote file path:");
                    String remotePath = scanner.nextLine().trim();
                    System.out.println("Enter local file path:");
                    String localPath = scanner.nextLine().trim();
                    if (remotePath.charAt(0) == '/' ) {
                        remotePath = remotePath.substring(1);
                    }
                    if (client.cur_dir=="/"){
                        remotePath= "/"+remotePath;
                    } else {
                        remotePath=client.cur_dir+"/"+remotePath;
                    }
                    if (client.downloadFile(remotePath, localPath)) {
                        System.out.println("Download successful.");
                    } else {
                        System.err.println("Download failed.");
                    }
                    break;

//                case "open":
//                    System.out.println("Enter file path:");
//                    String path = scanner.nextLine();
//                    DataInputStream fileStream = client.openFile(path);
//                    if (fileStream != null) {
//                        System.out.println("File opened.");
//                    } else {
//                        System.out.println("Failed to open file.");
//                    }
//                    break;

                case "copy":
                    System.out.println("Enter source file path:");
                    String copySourcePath = scanner.nextLine().trim();
                    System.out.println("Enter destination file path:");
                    String copyDestPath = scanner.nextLine().trim();
                    if (copySourcePath.charAt(0) == '/' ) {
                        copySourcePath = copySourcePath.substring(1);
                    }
                    if (client.cur_dir=="/"){
                        copySourcePath= "/"+copySourcePath;
                    } else {
                        copySourcePath=client.cur_dir+"/"+copySourcePath;
                    }

                    if (copyDestPath.charAt(0) == '/' ) {
                        copyDestPath = copyDestPath.substring(1);
                    }
                    if (client.cur_dir=="/"){
                        copyDestPath= "/"+copyDestPath;
                    } else {
                        copyDestPath=client.cur_dir+"/"+copyDestPath;
                    }

                    if (client.copyFile(copySourcePath, copyDestPath)) {
                        System.out.println("File copied successfully.");
                    } else {
                        System.err.println("File copy failed.");
                    }
                    break;

                case "cd":
                    System.out.println("Enter path:");
                    String rel_path = scanner.nextLine().trim();
                    client.changeDirectory(rel_path);
                    break;

                case "pwd":
                    System.out.println("Current directory: " + client.cur_dir);
                    break;

                case "info":
                    System.out.println("Enter file or directory path:");
                    String infoDirPath = scanner.nextLine().trim();
                    if (infoDirPath.charAt(0) == '/' ) {
                        infoDirPath = infoDirPath.substring(1);
                    }
                    if (client.cur_dir=="/"){
                        infoDirPath= "/"+infoDirPath;
                    } else {
                        infoDirPath=client.cur_dir+"/"+infoDirPath;
                    }

                    FileInfo info = client.getFileInfo(infoDirPath);
                    if (info != null) {
                        System.out.println("File/Directory Info: FileInfo{" + "fileName='" + info.getFileName() + '\'' +
                                ", fileSize='" + info.getFileSize()+  '\''+
                                ", creationTime='" + info.getCreationTime() + '\'' +
                                ", path='" + info.getPath()  + '\'' +
                                ", owner='" + info.getOwner()  + '\'' +
                                ", group='" + info.getGroup() + '\'' +
                                ", isDirectory='" + info.isDirectory() + '\''+
                                ", parent= " + "FileInfo{" + "path='" + info.getParent().getPath() + '\''
                                + ", owner='" + info.getParent().getOwner() + '\'' +
                                '}');
                    } else {
                        System.err.println("Failed to retrieve info for: " + infoDirPath);
                    }
                    break;

                case "read":
                    // 实现从文件读取数据的逻辑
                    System.out.println("Enter file path:");
                    String readPath = scanner.nextLine().trim();
                    if (readPath.charAt(0) == '/' ) {
                        readPath = readPath.substring(1);
                    }
                    if (client.cur_dir=="/"){
                        readPath= "/"+readPath;
                    } else {
                        readPath=client.cur_dir+"/"+readPath;
                    }
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

                    if (writePath.charAt(0) == '/' ) {
                        writePath = writePath.substring(1);
                    }
                    if (client.cur_dir=="/"){
                        writePath= "/"+writePath;
                    } else {
                        writePath=client.cur_dir+"/"+writePath;
                    }

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
                    String f = scanner.nextLine().trim();
                    if (f.charAt(0) == '/' ) {
                        f = f.substring(1);
                    }
                    if (client.cur_dir=="/"){
                        f= "/"+f;
                    } else {
                        f=client.cur_dir+"/"+f;
                    }
                    byte[] content = client.readFile(f,0,0);
                    System.out.println(new String(content));
                    break;

//                case "close":
//                    // 实现关闭文件的逻辑
//                    System.out.println("Enter file path:");
//                    String closePath = scanner.nextLine().trim();
//                    boolean closeSuccess = client.closeFile(closePath);
//                    System.out.println("Close result: " + (closeSuccess ? "Success" : "Failed"));
//                    break;

                case "delete":
                    System.out.println("Enter file path:");
                    String deletePath = scanner.nextLine().trim();
                    if (deletePath.charAt(0) == '/' ) {
                        deletePath = deletePath.substring(1);
                    }
                    if (client.cur_dir=="/"){
                        deletePath= "/"+deletePath;
                    } else {
                        deletePath=client.cur_dir+"/"+deletePath;
                    }
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

