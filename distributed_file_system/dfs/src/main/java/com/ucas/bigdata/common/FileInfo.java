package com.ucas.bigdata.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FileInfo implements Serializable { // 文件元数据结构

    private String fileName;
    private long fileSize;
    private long creationTime;
    private String path;
    private String owner;
    private String group;
    private boolean isDirectory;
    private FileInfo parent;
    private List<FileInfo> children = new ArrayList();//子节点
    private List<String> locations = new ArrayList();//存储位置
    private List<Integer> status = new ArrayList();  //副本状态


    public FileInfo(String fileName,String path, boolean isDirectory,long fileSize, String owner,long creationTime) {
        this.path = path;
        this.owner = owner;
        this.isDirectory = isDirectory;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.creationTime = creationTime;
    }

    public FileInfo(String path, String owner, boolean isDirectory, FileInfo parentInfo) {
        this.path = path;
        this.owner = owner;
        this.isDirectory = isDirectory;
        this.fileName = getFileName(path);
        this.parent = parentInfo;
        this.fileSize = -1;
        this.creationTime = System.currentTimeMillis();
        this.parent.getChildren().add(this);
    }

    // 获取父目录路径
    private String getFileName(String path) {
        path = path.trim();
        int lastSeparatorIndex = path.lastIndexOf('/');
        if(lastSeparatorIndex == path.length()-1){
            path = path.substring(0,path.length()-1);
        }
        if (lastSeparatorIndex == -1) {
            return "/";
        } else {
            return path.substring(lastSeparatorIndex+1);
        }
    }

    public String serialize() {
        StringBuilder serialized = new StringBuilder();
        serialized.append(fileName).append(",")
                .append(fileSize).append(",")
                .append(path).append(",")
                .append(owner).append(",")
                .append(isDirectory);

        // 添加存储位置
        serialized.append(",").append(String.join(";", locations));
        return serialized.toString();
    }

    /**
     * 反序列化字符串为 FileInfo 对象
     *
     * @param serialized 序列化的字符串
     * @return 反序列化后的 FileInfo 对象
     */
    public static FileInfo deserialize(String serialized) {
        String[] parts = serialized.split(",", 6); // 使用 6 分隔，避免路径或其他字段被误分隔
        String fileName = parts[0];
        long fileSize = Long.parseLong(parts[1]);
        String path = parts[2];
        String owner = parts[3];
        boolean isDirectory = Boolean.parseBoolean(parts[4]);
        long creationTime = Long.parseLong(parts[5]);

        // 构造对象
        FileInfo fileInfo = new FileInfo(fileName, path, isDirectory, fileSize, owner, creationTime);
        // 添加存储位置
        if (parts.length > 5 && !parts[5].isEmpty()) {
            String[] locationArray = parts[5].split(";");
            for (String location : locationArray) {
                fileInfo.getLocations().add(location);
            }
        }

        return fileInfo;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public boolean isDirectory() {
        return isDirectory;
    }

    public void setDirectory(boolean directory) {
        isDirectory = directory;
    }

    public FileInfo getParent() {
        return parent;
    }

    public void setParent(FileInfo parent) {
        this.parent = parent;
    }

    public List<FileInfo> getChildren() {
        return children;
    }

    public void setChildren(List<FileInfo> children) {
        this.children = children;
    }

    public List<String> getLocations() {
        return locations;
    }

    public void setLocations(List<String> locations) {
        this.locations = locations;
    }

    public List<Integer> getStatus() {
        return status;
    }

    public void setStatus(List<Integer> status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "FileInfo{" +
                "fileName='" + fileName + '\'' +
                ", fileSize=" + fileSize +
                ", creationTime=" + creationTime +
                ", path='" + path + '\'' +
                ", owner='" + owner + '\'' +
                ", group='" + group + '\'' +
                ", isDirectory=" + isDirectory +
                ", parent=" + parent +
                '}';
    }
}
