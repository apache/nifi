package org.apache.nifi.processors.adx.enums;

public enum IngestionReportMethodEnum {
    IRM_QUEUE("IngestionReportMethod:Queue","Reports are generated for queue-events."),
    IRM_TABLE("IngestionReportMethod:Table","Reports are generated for table-events."),
    IRM_TABLEANDQUEUE("IngestionReportMethod:TableAndQueue","Reports are generated for table- and queue-events.");

    private String ingestionReportMethod;
    private String description;


    IngestionReportMethodEnum(String displayName, String description) {
        this.ingestionReportMethod = displayName;
        this.description = description;
    }

    public String getIngestionReportMethod() {
        return ingestionReportMethod;
    }

    public String getDescription() {
        return description;
    }
}
