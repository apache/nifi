package org.apache.nifi.processors.adx.enums;

public enum IngestionStatusEnum {
    ST_SUCCESS("IngestionStatus:SUCCESS","Wait until ingestions is reported as succeded."),
    ST_FIREANDFORGET("ST_FIREANDFORGET","Do not wait on ADX for status.");

    private String ingestionStatus;
    private String description;


    IngestionStatusEnum(String ingestionStatus, String description) {
        this.ingestionStatus = ingestionStatus;
        this.description = description;
    }

    public String getIngestionStatus() {
        return ingestionStatus;
    }

    public String getDescription() {
        return description;
    }
}
