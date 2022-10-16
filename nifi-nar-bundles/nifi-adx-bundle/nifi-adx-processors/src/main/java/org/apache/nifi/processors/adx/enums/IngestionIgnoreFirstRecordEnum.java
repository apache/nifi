package org.apache.nifi.processors.adx.enums;

public enum IngestionIgnoreFirstRecordEnum {

    YES("Yes","Ignore first record while ingesting data"),
    NO("No","Do not ignore the first record while ingesting data");

    private String ingestFirstRecord;
    private String description;


    IngestionIgnoreFirstRecordEnum(String displayName, String description) {
        this.ingestFirstRecord = displayName;
        this.description = description;
    }

    public String getIngestFirstRecord() {
        return ingestFirstRecord;
    }

    public void setIngestFirstRecord(String ingestFirstRecord) {
        this.ingestFirstRecord = ingestFirstRecord;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
