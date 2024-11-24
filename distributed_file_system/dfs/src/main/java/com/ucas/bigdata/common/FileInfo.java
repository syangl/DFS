package com.ucas.bigdata.common;

import java.util.ArrayList;
import java.util.List;

public class FileInfo { // 文件元数据结构

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
