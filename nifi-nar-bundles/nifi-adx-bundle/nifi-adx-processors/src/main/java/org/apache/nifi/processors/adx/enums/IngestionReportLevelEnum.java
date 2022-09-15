package org.apache.nifi.processors.adx.enums;

public enum IngestionReportLevelEnum {
    IRL_NONE("IngestionReportLevel:None","No reports are generated at all."),
    IRL_FAIL("IngestionReportLevel:Failure","Status get's reported on failure only."),
    IRL_FAS("IngestionReportLevel:FailureAndSuccess","Status get's reported on failure and success.");

    private String ingestionReportLevel;

    private String description;


    IngestionReportLevelEnum(String displayName, String description) {
        this.ingestionReportLevel = displayName;
        this.description = description;
    }

    public String getIngestionReportLevel() {
        return ingestionReportLevel;
    }

    public String getDescription() {
        return description;
    }
}
