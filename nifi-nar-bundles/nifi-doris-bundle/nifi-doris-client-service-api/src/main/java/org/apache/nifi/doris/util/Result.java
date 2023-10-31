package org.apache.nifi.doris.util;

public class Result {
    private String loadResult;
    private int statusCode;

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

    @Override
    public String toString() {
        return "Result{" +
                "loadResult='" + loadResult + '\'' +
                ", statusCode=" + statusCode +
                '}';
    }
}
