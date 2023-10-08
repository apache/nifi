package org.apache.nifi.doris.util;

public class Result {
    private String loadResult;
    private int statusCode;
    private String executeStatus;

    public String getLoadResult() {
        return loadResult;
    }

    public void setLoadResult(String loadResult) {
        this.loadResult = loadResult;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public String getExecuteStatus() {
        return executeStatus;
    }

    public void setExecuteStatus(String executeStatus) {
        this.executeStatus = executeStatus;
    }
}
