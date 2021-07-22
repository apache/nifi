package org.apache.nifi.bootstrap.util;

public class DiagnosticProperties {

    private final String dirPath;
    private final int maxFileCount;
    private final int maxSizeInBytes;
    private final boolean verbose;
    private final boolean allowed;

    public DiagnosticProperties(String dirPath, int maxFileCount, int maxSizeInBytes, boolean verbose, boolean allowed) {
        this.dirPath = dirPath;
        this.maxFileCount = maxFileCount;
        this.maxSizeInBytes = maxSizeInBytes;
        this.verbose = verbose;
        this.allowed = allowed;
    }

    public String getDirPath() {
        return dirPath;
    }

    public int getMaxFileCount() {
        return maxFileCount;
    }

    public int getMaxSizeInBytes() {
        return maxSizeInBytes;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public boolean isAllowed() {
        return allowed;
    }
}
