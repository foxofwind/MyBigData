package com.bi.analysis.domain;

/**
 * Created by Administrator on 2017/12/14.
 */
public class PvStatistics {
    private static final long serialVersionUID = 3518776796426921776L;
    private long id;
    private String projectName;
    private String path;
    private long createTime;
    private int pvCount;
    private String type;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public int getPvCount() {
        return pvCount;
    }

    public void setPvCount(int pvCount) {
        this.pvCount = pvCount;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }


}
