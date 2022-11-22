package org.apache.nifi.processors.adx.source.model;

public class KustoBlobStorageParams {

    private String storageAccount;

    private String endPointSuffix;
    private String containerName;
    private String sasToken;

    public String getStorageAccount() {
        return storageAccount;
    }

    public void setStorageAccount(String storageAccount) {
        this.storageAccount = storageAccount;
    }

    public String getContainerName() {
        return containerName;
    }

    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    public String getSasToken() {
        return sasToken;
    }

    public void setSasToken(String sasToken) {
        this.sasToken = sasToken;
    }

    public String getEndPointSuffix() {
        return endPointSuffix;
    }

    public void setEndPointSuffix(String endPointSuffix) {
        this.endPointSuffix = endPointSuffix;
    }
}
