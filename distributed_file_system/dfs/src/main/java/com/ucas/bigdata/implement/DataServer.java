package com.ucas.bigdata.implement;

// DataServer 代码
import com.ucas.bigdata.client.MetaServerClient;
import com.ucas.bigdata.common.Config;
import com.ucas.bigdata.common.DataOpCode;
import com.ucas.bigdata.common.MetaOpCode;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import sun.security.krb5.internal.HostAddress;
import sun.security.x509.IPAddressName;

import java.io.*;
import java.net.*;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataServer {
    private static Logger log = LogManager.getLogger(DataServer.class);
    String nodeName;
    boolean isRunning = false;
    int DATA_SERVRE_PORT = 9526;
    int END_STREAM = -1;
    String storage_path;
    MetaServerClient metaClient;
    ServerSocket serverSocket;
    HeartBeatThread heartBeat;
    private ExecutorService threadPool;

    public DataServer() {
        try {


            this.nodeName = InetAddress.getLocalHost().getHostName();
            serverSocket = new ServerSocket(DATA_SERVRE_PORT);
            threadPool = Executors.newFixedThreadPool(100);
            metaClient = new MetaServerClient();
            heartBeat = new HeartBeatThread();
            storage_path = "/homework_storage";
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void serve() throws IOException {
        System.out.println("Data Server is running...");
        isRunning = true;
        heartBeat.start();
        while (isRunning) {
            // 接受客户端连接
            Socket clientSocket = serverSocket.accept();
            System.out.println("Client connected: " + clientSocket.getInetAddress());
            threadPool.execute(new ServerThread(clientSocket));
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
                    DataOpCode op = DataOpCode.read(in);
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

    public class HeartBeatThread extends Thread {

        @Override
        public void run() {
            while(isRunning){
                metaClient.heartBeat(nodeName);
                try {
                    Thread.sleep(Config.HEARTBEAT_SECS * 1000);
                    //TODO
//                    System.out.println("heartBeat node " + nodeName + " time:"+new Date().toString());
                } catch (InterruptedException e) {
                }
            }
        }
    }

    // 处理客户端请求并返回响应
    protected final void process(DataOpCode op, DataInputStream in, DataOutputStream out) throws IOException {
        switch(op) {
            case WRITE_FILE:
                writeFile(in,out);
                break;
            case READ_FILE:
                readFile(in,out);
                break;
            case DOWNLOAD_FILE:
                handleReadFile(in,out);
                break;
            case OPEN_FILE:
                handleOpenFile(in,out);
                break;
            case DEL_FILE:
                deleteFile(in,out);
                break;
            case CREATE_FILE:
                handleCreateFile(in,out);
                break;
            case CREATE_DIRECTORY:
                handleCreateDirectory(in,out);
                break;
            case DELETE_DIRECTORY:
                handleDeleteDirectory(in,out);
                break;
            case MOVE_FILE:
                handleMoveFile(in, out);
                break;
            case GET_FILE_SIZE:
                handleGetFileSize(in, out);
                break;
            default:
                throw new IOException("Unknown op " + op + " in data stream");
        }
    }

    private void handleCreateFile(DataInputStream in, DataOutputStream out) {
        // 创建一个新的函数来处理创建文件的逻辑
        int retCode = 0;
        String msg = "";
        FileOutputStream fout = null;

        try {
            // 1. 读取文件 ID 和文件路径
            String fileId = in.readUTF();
            String path = storage_path + File.separator + fileId;

            // 2. 创建文件
            File file = new File(path);
            if (file.exists()) {
                throw new IOException("File already exists: " + path);
            }
            if (file.createNewFile()) {
                fout = new FileOutputStream(file);
                log.info("File " + fileId + " created successfully.");
                msg = "File created successfully: " + fileId;
            } else {
                throw new IOException("Failed to create file: " + path);
            }

            // 3. 写入文件数据
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            while (bytesRead > 0) {
                fout.write(buffer, 0, bytesRead);
                if (bytesRead < 1024) break;
                bytesRead = in.read(buffer);
            }
            fout.flush();
        } catch (IOException e) {
            log.error("Error creating file: ", e);
            retCode = -1;
            msg = "Error creating file: " + e.getMessage();
        } finally {
            try {
                if (fout != null) fout.close();
                // 发送响应
                out.writeInt(retCode);
                out.writeUTF(msg);
                out.flush();
            } catch (IOException e) {
                log.error("Error sending response: ", e);
            }
        }
    }


    // 删除文件数据块
    private void deleteFile(DataInputStream in, DataOutputStream out) {
        try {
            String fileId = in.readUTF();
            String filePath = storage_path + File.separator + fileId;

            File file = new File(filePath);
            if (file.exists() && file.delete()) {
                out.writeInt(0); // 成功状态码
                out.writeUTF("File deleted successfully: " + filePath);
            } else {
                out.writeInt(-1); // 错误状态码
                out.writeUTF("Error deleting file or file not found: " + filePath);
            }
        } catch (IOException e) {
            try {
                out.writeInt(-1);
                out.writeUTF("Error handling delete file request: " + e.getMessage());
            } catch (IOException ex) {
                log.error("Error sending response: ", ex);
            }
        }
    }


    private void readFile(DataInputStream in, DataOutputStream out) {
        // 创建服务器套接字，监听指定端口
        RandomAccessFile file = null;
        int retCode = -1;
        String msg = "read failed";
        try {
            String fileId = in.readUTF();
            long offset = in.readLong();
            String path = storage_path+File.separator+fileId;
            byte[] buffer = new byte[1024];
            file = new RandomAccessFile(path, "r");
            file.seek(offset); // 设置文件指针到偏移量位置

            int bytesRead = file.read(buffer);
            if(bytesRead>0){
                retCode = 0;
                out.writeInt(retCode);
                out.writeUTF("OK");
                out.flush();

                do{
                    out.writeInt(bytesRead);
                    out.write(buffer,0,bytesRead);
                    out.flush();
                    bytesRead = file.read(buffer);
                }while (bytesRead >0);
                out.writeInt(END_STREAM);
            }

        } catch (IOException e) {
            log.info(e);
            retCode = -1;
            msg = "File write error:"+e.getMessage();
            try {
                out.writeInt(retCode);
                out.writeUTF(msg);
                out.flush();
            } catch (IOException e1) {
                log.error(e1);
            }

        }finally {
            try {
                file.close();
            } catch (IOException e) {
                log.error(e);
            }
        }
    }


    private void writeFile(DataInputStream in,DataOutputStream out){
        // 创建服务器套接字，监听指定端口
        FileOutputStream fout = null;
        int retCode = 0;
        String msg = "";
        try {
            String fileId = in.readUTF();//1.读取文件ID
            String path = storage_path+File.separator+fileId;
            fout = new FileOutputStream(path);
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);//2.遍历读取
            while (bytesRead>0) {
                fout.write(buffer,0,bytesRead);
                if(bytesRead < 1024) break;
                bytesRead = in.read(buffer);
            }

            log.info("File "+ fileId+" write succuessfully!");
            retCode = 0;
            msg = "File "+ fileId+" write succuessfully!";
        } catch (IOException e) {
            log.info(e);
            retCode = -1;
            msg = "File write error:"+e.getMessage();
        }finally {
            if(fout != null)
                try {
                    out.writeInt(retCode);//3.回写返回码
                    out.writeUTF(msg);//4.回写消息
                    out.flush();
                    fout.close();
                } catch (IOException e) {
                    log.info(e);
                }
        }
    }

    private static void sendMessageToMetaServer(Socket socket, String message) throws IOException {
        PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
        writer.println(message);
    }

    private void handleCreateDirectory(DataInputStream in, DataOutputStream out) {
        int retCode = 0;
        String msg = "";

        try {
            // 1. 读取目录路径
            String dirPath = in.readUTF();
            String fullPath = storage_path + File.separator + dirPath;

            // 2. 创建目录
            File dir = new File(fullPath);
            if (dir.exists() || dir.mkdirs()) {
                retCode = 0;
                msg = "Directory " + dirPath + " created successfully!";
                log.info(msg);
            } else {
                throw new IOException("Failed to create directory: " + fullPath);
            }
        } catch (IOException e) {
            log.error("Error creating directory: ", e);
            retCode = -1;
            msg = e.getMessage();
        } finally {
            try {
                out.writeInt(retCode);
                out.writeUTF(msg);
                out.flush();
            } catch (IOException e) {
                log.error("Error sending response to client: ", e);
            }
        }
    }


    private void handleDeleteDirectory(DataInputStream in, DataOutputStream out) throws IOException {
        String dirPath = in.readUTF();
        String fullPath = storage_path + File.separator + dirPath;

        File dir = new File(fullPath);
        if (dir.exists() && dir.isDirectory()) {
            deleteDirectoryRecursive(dir); // 递归删除目录及其内容
            out.writeInt(0); // 成功响应
            out.writeUTF("Directory deleted successfully: " + dirPath);
        } else {
            out.writeInt(-1); // 错误响应
            out.writeUTF("Directory not found or not a directory: " + dirPath);
        }
    }

    private void deleteDirectoryRecursive(File dir) {
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) {
                deleteDirectoryRecursive(file);
            }
            file.delete();
        }
        dir.delete();
    }

    private void handleMoveFile(DataInputStream in, DataOutputStream out) throws IOException {
        String sourcePath = in.readUTF(); // 源文件路径
        String destPath = in.readUTF();   // 目标文件路径

        String sourceFullPath = storage_path + File.separator + sourcePath;
        String destFullPath = storage_path + File.separator + destPath;

        File sourceFile = new File(sourceFullPath);
        File destFile = new File(destFullPath);

        try {
            // 检查源文件是否存在
            if (!sourceFile.exists()) {
                throw new IOException("Source file not found: " + sourcePath);
            }

            // 检查目标文件是否已存在
            if (destFile.exists()) {
                throw new IOException("Destination file already exists: " + destPath);
            }

            // 移动文件
            if (sourceFile.renameTo(destFile)) {
                out.writeInt(0); // 成功响应
                out.writeUTF("File moved successfully.");
            } else {
                throw new IOException("Failed to move file from " + sourcePath + " to " + destPath);
            }
        } catch (IOException e) {
            out.writeInt(-1); // 错误响应
            out.writeUTF(e.getMessage());
        }
    }

    private void handleReadFile(DataInputStream in, DataOutputStream out) throws IOException {
        String fileId = in.readUTF(); // 读取文件 ID
        String filePath = storage_path + File.separator + fileId;

        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = file.read(buffer)) != -1) {
                out.writeInt(bytesRead);
                out.write(buffer, 0, bytesRead);
            }
            out.writeInt(-1); // 结束标志
        } catch (Exception e) {
            out.writeInt(-1);
            out.writeUTF("Error reading file: " + e.getMessage());
        }
    }
    private void handleGetFileSize(DataInputStream in, DataOutputStream out) throws IOException {
        String fileId = in.readUTF(); // 读取文件 ID
        String filePath = storage_path + File.separator + fileId;

        try {
            File file = new File(filePath);
            if (!file.exists()) {
                throw new IOException("File not found: " + fileId);
            }

            long size = file.length();
            out.writeInt(0); // 成功响应
            out.writeLong(size); // 返回文件大小
        } catch (IOException e) {
            out.writeInt(-1); // 错误响应
            out.writeUTF(e.getMessage());
        }
    }
    private void handleOpenFile(DataInputStream in, DataOutputStream out) throws IOException {
        String fileId = in.readUTF(); // 读取文件 ID
        String filePath = storage_path + File.separator + fileId;

        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            out.writeInt(0); // 成功状态码
            out.writeUTF("File is ready for reading.");

            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = file.read(buffer)) != -1) {
                out.writeInt(bytesRead);
                out.write(buffer, 0, bytesRead);
            }
            out.writeInt(-1); // 结束标志
        } catch (Exception e) {
            out.writeInt(-1);
            out.writeUTF("Error opening file: " + e.getMessage());
        }
    }


    public static void main(String[] args) {
        DataServer dataServer = new DataServer();
        try {
            dataServer.serve();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

