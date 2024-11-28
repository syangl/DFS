package com.ucas.bigdata.implement;

import com.ucas.bigdata.client.Connection;
import com.ucas.bigdata.common.Config;
import com.ucas.bigdata.common.FileInfo;
import com.ucas.bigdata.common.MetaOpCode;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MetadataServer {
    private static Logger log = LogManager.getLogger(MetadataServer.class);
    private ServerSocket serverSocket;
    private Map<String, String> fileToStorageNode; // 文件路径到存储节点名称的映射
    private LinkedHashMap<String,StorageNode> storageNodes = new LinkedHashMap();
    private LinkedHashMap<String,Long> storageNodesUpTime = new LinkedHashMap();
    private HashMap<String,ServerThread> threads = new HashMap<>();
    private Map<String, String> fileOwners;
    private Map<String, FileInfo> fileSystem; // 文件元数据，文件路径到文件信息的映射
    private RocksDB db;
    private ExecutorService threadPool; // 使用线程池
    private boolean isRunning;



    public MetadataServer() {

        fileSystem = new HashMap();
        fileToStorageNode = new HashMap();
        // 初始化存储节点，对应三台虚拟机
        storageNodes.put("dfs101", new StorageNode("dfs101"));
        storageNodes.put("dfs102", new StorageNode("dfs102"));
        storageNodes.put("dfs103", new StorageNode("dfs103"));
        // 初始化根目录（文件系统的起点）
        fileSystem.put("/", new FileInfo(null, "/", true,0l,"root", 0l));
        try {
            serverSocket = new ServerSocket(Config.META_SERVRE_PORT);
            threadPool = Executors.newFixedThreadPool(100);
            // 初始化DB
            Options options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, Config.META_DB_PATH);

            // 加载已有元数据
            loadMetadata();

        } catch (IOException e) {
            log.info(e);} catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private void loadMetadata() {
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                String path = new String(iterator.key());
                FileInfo fileInfo = deserializeFileInfo(iterator.value());
                fileSystem.put(path, fileInfo);
            }
        } catch (Exception e) {
            log.error("Error loading metadata from RocksDB: ", e);
        }
    }

    private FileInfo deserializeFileInfo(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (FileInfo) ois.readObject();
    }

    private byte[] serializeFileInfo(FileInfo fileInfo) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(fileInfo);
        return bos.toByteArray();
    }
    private void serve() {
        System.out.println("MetaServer is running...");
        isRunning = true;
        try {
            while (isRunning) {
                // 接受客户端连接
                Socket clientSocket = null;

                clientSocket = serverSocket.accept();
                // 接收客户端连接
                System.out.println("Client connected: " + clientSocket.getInetAddress());
                // 提交给线程池处理
                threadPool.execute(new ServerThread(clientSocket));
            }
        } catch (IOException e){
            System.err.println("Error accepting client connections: " + e.getMessage());
        }
    }

    public void shutdown() {
        isRunning = false;
        threadPool.shutdown(); // 优雅关闭线程池
        try {
            serverSocket.close();
        } catch (IOException e) {
            System.err.println("Error shutting down server: " + e.getMessage());
        }
    }

    private class ServerThread implements Runnable {
        private Socket clientSocket;
        public ServerThread(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }
        @Override
        public void run() {
            try {
                DataInputStream in = new DataInputStream(clientSocket.getInputStream());
                DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
                // 处理客户端请求
                while (true){
                    System.out.println("Before read");
                    MetaOpCode op = MetaOpCode.read(in);
                    System.out.println("After read OpCode: " + op);
                    process(op, in, out);
                }
            }
            catch (IOException e) {
                System.err.println("Error processing client request: " + e.getMessage());
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    System.err.println("Error closing client connection: " + e.getMessage());
                }
            }
        }
    }

    //
    /**
     * 处理客户端元数据请求并返回响应,处理的信息包括：
     * 注册和心跳： 数据服务器需要在启动时向元数据服务器注册自己，告知自己的可用性。随后，数据服务器可以定期发送心跳消息，以通知元数据服务器它的状态。
     * 文件信息交换： 数据服务器可以定期向元数据服务器汇报文件信息，例如已存储的文件列表、文件大小等。元数据服务器可以根据这些信息维护文件目录和存储位置信息。
     * 文件副本管理： 元数据服务器可以指导数据服务器在不同的存储节点上创建文件的副本，以增加数据冗余和容错性。
     * 文件删除和迁移： 元数据服务器可以通知数据服务器删除特定文件，也可以指导文件从一个数据服务器迁移到另一个数据服务器。
     * 数据块分配： 元数据服务器可以负责指导数据服务器如何分配文件的不同数据块，以便实现数据的分布式存储和访问。
     * 一致性和同步： 当文件信息或状态发生变化时，确保元数据服务器和数据服务器之间的信息是一致的。你可能需要考虑使用分布式一致性协议（如 Paxos、Raft）来实现这一点。
     * @throws IOException
     */
    protected final void process( MetaOpCode op, DataInputStream in, DataOutputStream out) throws IOException {
        switch (op) {
            case HEART_BEAT:
                handleHeartBeat(in, out);
                break;
            case CREATE_FILE:
                createFile(in, out);
                break;
            case RENAME_FILE:
                //@todo
                break;
            case DEL_FILE:
                delete(in, "dfs", out);  // 默认用户 "dfs"
                break;
            case LIST_FILE:
                listFile(in, out);
                break;
            case GET_FILE_LOCATIONS:
                getFileLocations(in, out);
                break;
            case CLOSE_FILE:
                closeFile(in, out);
                break;
            case GET_FILE_SIZE:
                handleGetFileSize(in, out);
                break;
            case READ_FILE:
                handleDownloadFile(in, out);
                break;
            case OPEN_FILE:
                handleOpenFile(in, out);
                break;
            case DOWNLOAD_FILE:
                handleDownloadFile(in, out);
                break;
            case GET_FILE_INFO:
                handleGetFileInfo(in, out);
                break;
            case COPY_FILE:
                handleCopyFile(in, out);
                break;
            case MOVE_FILE:
                handleMoveFile(in, out);
                break;
            default:
                System.out.println("Unknown op " + op + " in data stream");
        }
    }

    private void closeFile(DataInputStream in, DataOutputStream out) throws IOException {
        String path = in.readUTF(); // 读取文件路径

        try {
            // 检查文件是否存在
            FileInfo fileInfo = fileSystem.get(path);
            if (fileInfo == null || fileInfo.isDirectory()) {
                throw new IllegalArgumentException("File not found or is a directory: " + path);
            }

            // 可添加资源释放逻辑，例如将文件标记为关闭
            // 在此示例中仅记录关闭操作
            log.info("File " + path + " closed successfully.");

            out.writeInt(0); // 成功状态码
            out.writeUTF("File closed successfully.");
        } catch (IllegalArgumentException e) {
            out.writeInt(-1); // 错误状态码
            out.writeUTF(e.getMessage());
        } catch (Exception e) {
            out.writeInt(-1);
            out.writeUTF("Internal server error: " + e.getMessage());
        }
    }

    private void createFile(DataInputStream in, DataOutputStream out) {
        try {
            String path = in.readUTF(); // 读取客户端发送路径
            String owner = in.readUTF(); // 读取客户端发送用户
            boolean isDir = in.readBoolean(); // 是否为目录
            log.info(new Date().toString()+" before createFile." );
            FileInfo fi = create(path,owner,isDir);
            if (fi == null) {
                out.writeInt(-1);
                out.writeUTF("File already exists: " + path);
                out.flush();
                return;
            }else {
                log.info(new Date().toString()+" after createFile." );
                String nodeName = getNewStorageNode(0); // 分配存储节点
                String localFileId = UUID.randomUUID().toString(); // 生成唯一文件块 ID
                fi.getLocations().add(nodeName + ":" + localFileId); // 记录存储位置
                // 持久化到 RocksDB
                db.put(path.getBytes(), serializeFileInfo(fi));
                // 返回code
                out.writeInt(0);
                out.writeUTF(isDir ? "Directory created successfully: " + path : "File created successfully: " + path);
                out.flush();
                log.info(new Date().toString()+" createFile 1." );
            }
        } catch (IOException e) {
            e.printStackTrace();
            try {
                out.writeInt(-1);
                out.writeUTF(e.getMessage());
                out.flush();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        System.out.println(new Date().toString()+" createFile done." );
    }

//    // 添加文件元数据
//    public void addFileMetadata(String filePath, long fileSize, String storageNode) {
//        FileInfo fileInfo = new FileInfo(filePath, fileSize);
//        fileSystem.put(filePath, fileInfo);
//        fileToStorageNode.put(filePath, storageNode);
//    }

    // 创建文件或目录
    public FileInfo create(String path, String owner, boolean isDirectory) {
        FileInfo fileInfo = null;
        if (!fileSystem.containsKey(path)) {

            String parentPath = getParentPath(path);
            if (!fileSystem.containsKey(parentPath) && !parentPath.equals("/")) {
                System.out.println("Parent directory " + parentPath + " does not exist.");
                create(parentPath,  owner, true);
            }

            if (fileSystem.containsKey(parentPath) || parentPath.equals("/")) {
                FileInfo parentInfo = fileSystem.get(parentPath);
                fileInfo = new FileInfo(path, owner, isDirectory, parentInfo);
                fileSystem.put(path, fileInfo);
                System.out.println((isDirectory ? "Directory" : "File") + " " + path + " created by " + owner);
            }
            return fileInfo;
        } else {
            System.out.println((isDirectory ? "Directory" : "File") + " " + path + " already exists.");
//            fileInfo = fileSystem.get(path);
            return null;
        }
    }

    // 删除文件或目录
    public void delete(DataInputStream in, String requester, DataOutputStream out) {
        String path = null;
        try {
            path = in.readUTF();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            if (!fileSystem.containsKey(path)) {
                // 文件或目录不存在
                out.writeInt(-1); // 错误响应
                out.writeUTF("File/Directory " + path + " not found.");
                log.info("File/Directory " + path + " not found.");
                return;
            }
            FileInfo fileInfo = fileSystem.get(path);
            String owner = fileInfo.getOwner();

            if (!owner.equals(requester)) {
                // 权限不足
                out.writeInt(-1); // 错误响应
                out.writeUTF("Permission denied. You are not the owner of " + path);
                log.info("Permission denied. You are not the owner of " + path);
                return;
            }
            // 如果是目录，递归删除子文件和子目录
            if (fileInfo.isDirectory()) {
                deleteDirectoryRecursive(fileInfo);
            }
            // 删除文件元数据
            fileSystem.remove(path);
            db.delete(path.getBytes()); // 删除持久化记录
            out.writeInt(0); // 成功响应
            out.writeUTF("File/Directory " + path + " deleted successfully.");
            log.info((fileInfo.isDirectory() ? "Directory" : "File") + " " + path + " deleted by " + requester);
        } catch (IOException | RocksDBException e) {
            try {
                out.writeInt(-1); // 错误响应
                out.writeUTF("Error deleting file/directory: " + e.getMessage());
                log.error("Error deleting file/directory " + path + ": ", e);
            } catch (IOException ex) {
                log.error("Error sending delete response: ", ex);
            }
        }
    }

    // 递归删除目录的所有子文件和子目录
    private void deleteDirectoryRecursive(FileInfo dirInfo) {
        if (dirInfo.getChildren() != null) {
            for (FileInfo child : new ArrayList<>(dirInfo.getChildren())) {
                if (child.isDirectory()) {
                    deleteDirectoryRecursive(child); // 递归删除子目录
                }
                fileSystem.remove(child.getPath());
                try {
                    db.delete(child.getPath().getBytes()); // 删除持久化记录
                } catch (RocksDBException e) {
                    log.error("Error deleting persistent metadata for " + child.getPath(), e);
                }
                log.info((child.isDirectory() ? "Directory" : "File") + " " + child.getPath() + " deleted.");
            }
        }
    }


    // 获取文件或目录信息
    public FileInfo getFileInfo(String path) {
        FileInfo fileInfo = null;
        if (fileSystem.containsKey(path)) {
            fileInfo = fileSystem.get(path);
            System.out.print("Path: " + path);
            System.out.print( " Owner: " + fileInfo.getOwner());
            System.out.print(" Is Directory: " + fileInfo.isDirectory());
        } else {
            System.out.print(" File/Directory " + path + " not found.");
        }
        return fileInfo;
    }

    // 获取父目录路径
    private String getParentPath(String path) {
        int lastSeparatorIndex = path.lastIndexOf('/');
        if (lastSeparatorIndex == -1) {
            return "/";
        } else {
            if(lastSeparatorIndex == 0){
                return "/";
            }
            return path.substring(0, lastSeparatorIndex);
        }
    }


    // 获取文件对应的存储节点
    public String getStorageNode(String filePath) {
        return fileToStorageNode.get(filePath);
    }

    public String getNewStorageNode(long fileSize) {
        Random random = new Random();
        int id = Math.abs(random.nextInt()) % storageNodes.size();
        String sn = (String)storageNodes.keySet().toArray()[id];
        return sn;
    }

    public void setStorageNode(List<StorageNode> storageNodes) {
        for(StorageNode sn : storageNodes)
            this.storageNodes.put(sn.getName(),sn);
    }


    private void listFile(DataInputStream in, DataOutputStream out) {
        try {
            List<String> fileList = new ArrayList<String>();
            String cur_dir = in.readUTF();
            FileInfo fileInfo = getFileInfo(cur_dir);
            if(fileInfo != null && fileInfo.getChildren() != null){
                for(FileInfo ch:fileInfo.getChildren()){
                    fileList.add(ch.getFileName());
                }
            }
            int size = fileList.size();
            out.writeInt(size);
            if(size > 0) {
                for (String name : fileList) {
                    out.writeUTF(name);
                }
                out.flush();
            }
            System.out.print("end of listfile.");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void handleHeartBeat(DataInputStream in, DataOutputStream out) {
        try {
            String nodeName = in.readUTF(); // 读取数据服务器发送的注册信息
            //TODO 更新时间戳
//            System.out.println(new Date().toString()+" received heartBeat from DataServer: " + nodeName);
            String msg = nodeName;
            Long now = System.currentTimeMillis();
            if(this.storageNodes.containsKey(nodeName)){//更新时间
                if(!storageNodesUpTime.containsKey(nodeName)){
                    storageNodesUpTime.put(nodeName,now);
                    msg += " time out is registered.";
                    System.out.println(msg);
                }
                Long lastUpTime = storageNodesUpTime.get(nodeName);
                if(now - lastUpTime > Config.TIMEOUT_OF_HEARTBEATS * Config.HEARTBEAT_SECS * 1000){
                    msg += " time out is recovered.";
                    System.out.println(msg);
                }else{
                    storageNodesUpTime.put(nodeName,now);//更新时间
                }
            }
            // 处理心跳信息并回复数据服务器
            out.writeInt(0);
            out.writeUTF(msg);
        }catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    private void handleCreateFile(DataInputStream in, DataOutputStream out) throws IOException {
        String path = in.readUTF();
        String owner = in.readUTF();
        boolean isDir = in.readBoolean(); // 区分文件和目录

        if (isDir) {
            handleCreateDirectory(path, owner, out); // 调用新增的 createDirectory 逻辑
        } else {
            // 调用现有的 createFile 方法逻辑
            try {
                FileInfo fi = create(path, owner, false);
                String nodeName = getNewStorageNode(0);
                String localFileId = UUID.randomUUID().toString();
                fi.getLocations().add(nodeName + ":" + localFileId);
                db.put(path.getBytes(), serializeFileInfo(fi));

                out.writeInt(0);
                out.writeUTF(nodeName + ":" + localFileId);
            } catch (Exception e) {
                out.writeInt(-1);
                out.writeUTF(e.getMessage());
            }
        }
    }

    /**
     * 处理目录创建请求
     *
     * @param path  路径
     * @param owner 所有者
     * @param out   输出流
     */
    private void handleCreateDirectory(String path, String owner, DataOutputStream out) throws IOException {
        try {
            FileInfo dirInfo = createDirectory(path, owner); // 调用新增的 createDirectory 方法
            db.put(path.getBytes(), serializeFileInfo(dirInfo)); // 持久化到 RocksDB

            out.writeInt(0); // 成功状态码
            out.writeUTF("Directory created successfully: " + path);
        } catch (IllegalArgumentException e) {
            out.writeInt(-1);
            out.writeUTF("Invalid request: " + e.getMessage());
        } catch (Exception e) {
            out.writeInt(-1);
            out.writeUTF("Internal server error: " + e.getMessage());
        }
    }

    /**
     * 创建目录逻辑
     *
     * @param path  路径
     * @param owner 所有者
     * @return 创建的目录信息
     */
    private FileInfo createDirectory(String path, String owner) {
        if (fileSystem.containsKey(path)) {
            throw new IllegalArgumentException("Directory already exists: " + path);
        }

        String parentPath = getParentPath(path);
        FileInfo parent = fileSystem.get(parentPath);

        if (parent == null || !parent.isDirectory()) {
            throw new IllegalArgumentException("Parent directory does not exist: " + parentPath);
        }

        FileInfo newDir = new FileInfo(path, owner, true, parent);
        fileSystem.put(path, newDir);
        return newDir;
    }

    private void handleDeleteFile(DataInputStream in, DataOutputStream out) throws IOException {
        String path = in.readUTF();
        boolean isDirectory = in.readBoolean(); // 区分文件和目录

        if (isDirectory) {
            handleDeleteDirectory(path, out); // 调用新增的目录删除逻辑
        } else {
            // 调用现有的文件删除逻辑
            try {
                FileInfo fileInfo = fileSystem.get(path);
                if (fileInfo == null) {
                    throw new IllegalArgumentException("File not found: " + path);
                }

                fileSystem.remove(path);
                db.delete(path.getBytes()); // 从 RocksDB 删除元数据
                out.writeInt(0);
                out.writeUTF("File deleted successfully: " + path);
            } catch (Exception e) {
                out.writeInt(-1);
                out.writeUTF(e.getMessage());
            }
        }
    }

    /**
     * 删除目录及其子内容
     *
     * @param path 目录路径
     * @param out  输出流
     */
    private void handleDeleteDirectory(String path, DataOutputStream out) throws IOException {
        try {
            FileInfo dirInfo = fileSystem.get(path);
            if (dirInfo == null || !dirInfo.isDirectory()) {
                throw new IllegalArgumentException("Directory not found or not a directory: " + path);
            }

            deleteDirectoryRecursive(dirInfo); // 递归删除目录内容
            fileSystem.remove(path);
            db.delete(path.getBytes()); // 删除持久化元数据

            out.writeInt(0);
            out.writeUTF("Directory deleted successfully: " + path);
        } catch (Exception e) {
            out.writeInt(-1);
            out.writeUTF(e.getMessage());
        }
    }


    private void handleGetFileSize(DataInputStream in, DataOutputStream out) throws IOException {
        String path = in.readUTF(); // 读取路径

        try {
            FileInfo fileInfo = fileSystem.get(path);
            if (fileInfo == null) {
                throw new IllegalArgumentException("File or directory not found: " + path);
            }

            long size;
            if (fileInfo.isDirectory()) {
                size = calculateDirectorySize(fileInfo); // 计算目录大小
            } else {
                size = fileInfo.getFileSize(); // 文件大小
            }

            out.writeInt(0); // 成功状态码
            out.writeLong(size); // 返回大小
        } catch (IllegalArgumentException e) {
            out.writeInt(-1); // 错误状态码
            out.writeUTF(e.getMessage());
        } catch (Exception e) {
            out.writeInt(-1);
            out.writeUTF("Internal server error: " + e.getMessage());
        }
    }

    /**
     * 计算目录大小（递归累加子文件和子目录大小）
     *
     * @param dirInfo 目录信息
     * @return 大小（字节）
     */
    private long calculateDirectorySize(FileInfo dirInfo) {
        long totalSize = 0;
        for (FileInfo child : dirInfo.getChildren()) {
            if (child.isDirectory()) {
                totalSize += calculateDirectorySize(child); // 递归计算子目录大小
            } else {
                totalSize += child.getFileSize();
            }
        }
        return totalSize;
    }

    private void handleDownloadFile(DataInputStream in, DataOutputStream out) throws IOException {
        String filePath = in.readUTF(); // 读取文件路径

        try {
            FileInfo fileInfo = fileSystem.get(filePath);
            if (fileInfo == null || fileInfo.isDirectory()) {
                throw new IllegalArgumentException("File not found or is a directory: " + filePath);
            }

            String location = fileInfo.getLocations().get(0); // 获取存储节点信息
            String[] nodeInfo = location.split(":");
            String nodeHost = nodeInfo[0];
            String fileId = nodeInfo[1];

            // 将存储节点和文件 ID 返回给客户端
            out.writeInt(0); // 成功状态码
            out.writeUTF(nodeHost + ":" + fileId); // 返回文件存储位置
        } catch (Exception e) {
            out.writeInt(-1);
            out.writeUTF("Error retrieving file location: " + e.getMessage());
        }
    }

    private void handleOpenFile(DataInputStream in, DataOutputStream out) throws IOException {
        String filePath = in.readUTF(); // 读取文件路径

        try {
            FileInfo fileInfo = fileSystem.get(filePath);
            if (fileInfo == null || fileInfo.isDirectory()) {
                throw new IllegalArgumentException("File not found or is a directory: " + filePath);
            }

            String location = fileInfo.getLocations().get(0); // 获取存储节点信息
            String[] nodeInfo = location.split(":");
            String nodeHost = nodeInfo[0];
            String fileId = nodeInfo[1];

            // 将存储节点和文件 ID 返回给客户端
            out.writeInt(0); // 成功状态码
            out.writeUTF(nodeHost + ":" + fileId); // 返回文件存储位置
        } catch (Exception e) {
            out.writeInt(-1); // 错误状态码
            out.writeUTF("Error retrieving file location: " + e.getMessage());
        }
    }


    private void handleGetFileInfo(DataInputStream in, DataOutputStream out) throws IOException {
        String path = in.readUTF(); // 读取路径

        try {
            FileInfo fileInfo = fileSystem.get(path); // 查找元数据
            if (fileInfo == null) {
                throw new IllegalArgumentException("File or directory not found: " + path);
            }

            out.writeInt(0); // 成功状态码
            out.writeUTF(fileInfo.serialize()); // 返回序列化的 FileInfo 对象
        } catch (IllegalArgumentException e) {
            out.writeInt(-1); // 错误状态码
            out.writeUTF(e.getMessage());
        } catch (Exception e) {
            out.writeInt(-1);
            out.writeUTF("Internal server error: " + e.getMessage());
        }
    }

    private void handleCopyFile(DataInputStream in, DataOutputStream out) throws IOException {
        String sourcePath = in.readUTF(); // 源文件路径
        String destPath = in.readUTF();   // 目标文件路径

        try {
            // 检查源文件是否存在
            FileInfo sourceFileInfo = fileSystem.get(sourcePath);
            if (sourceFileInfo == null || sourceFileInfo.isDirectory()) {
                throw new IllegalArgumentException("Source file not found or is a directory: " + sourcePath);
            }

            // 检查目标路径是否已存在
            if (fileSystem.containsKey(destPath)) {
                throw new IllegalArgumentException("Destination path already exists: " + destPath);
            }

            // 创建目标文件元数据
            FileInfo destFileInfo = new FileInfo(
                    destPath,
                    sourceFileInfo.getOwner(),
                    false,
                    fileSystem.get(getParentPath(destPath))
            );
            destFileInfo.setFileSize(sourceFileInfo.getFileSize());
            destFileInfo.setLocations(new ArrayList<>(sourceFileInfo.getLocations())); // 复制存储位置

            // 更新元数据
            fileSystem.put(destPath, destFileInfo);
            db.put(destPath.getBytes(), serializeFileInfo(destFileInfo)); // 持久化到 RocksDB

            out.writeInt(0); // 成功状态码
            out.writeUTF("File copied successfully.");
        } catch (Exception e) {
            out.writeInt(-1); // 错误状态码
            out.writeUTF(e.getMessage());
        }
    }

    private void handleMoveFile(DataInputStream in, DataOutputStream out) throws IOException {
        String sourcePath = in.readUTF(); // 源文件路径
        String destPath = in.readUTF();   // 目标文件路径

        try {
            // 检查源文件是否存在
            FileInfo sourceFileInfo = fileSystem.get(sourcePath);
            if (sourceFileInfo == null) {
                throw new IllegalArgumentException("Source file not found: " + sourcePath);
            }

            // 检查目标路径是否已存在
            if (fileSystem.containsKey(destPath)) {
                throw new IllegalArgumentException("Destination path already exists: " + destPath);
            }

            // 更新元数据
            FileInfo destFileInfo = new FileInfo(
                    destPath,
                    sourceFileInfo.getOwner(),
                    sourceFileInfo.isDirectory(),
                    fileSystem.get(getParentPath(destPath))
            );
            destFileInfo.setFileSize(sourceFileInfo.getFileSize());
            destFileInfo.setLocations(new ArrayList<>(sourceFileInfo.getLocations()));

            // 删除源文件元数据
            fileSystem.remove(sourcePath);
            db.delete(sourcePath.getBytes());

            // 添加目标文件元数据
            fileSystem.put(destPath, destFileInfo);
            db.put(destPath.getBytes(), serializeFileInfo(destFileInfo));

            out.writeInt(0); // 成功状态码
            out.writeUTF("File moved successfully.");
        } catch (Exception e) {
            out.writeInt(-1); // 错误状态码
            out.writeUTF(e.getMessage());
        }
    }



    // 查询文件的存储位置
    public void getFileLocations(DataInputStream in, DataOutputStream out) throws IOException {
        String path = in.readUTF();
        FileInfo fileInfo = fileSystem.get(path);
        if (fileInfo != null) {
            // 写出List<String>
            List<String> locations = fileInfo.getLocations();
            out.writeInt(locations.size());
            for (String location : locations) {
                out.writeUTF(location);
            }
        } else {
            // 写出空List
            out.writeInt(0);
            out.flush();
        }
    }


    public static void main(String[] args) throws IOException {
        MetadataServer metaServer = new MetadataServer();
        metaServer.serve();

    }


}

