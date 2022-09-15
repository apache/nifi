package org.apache.nifi.processors.adx.enums;

public enum AzureAdxIngestProcessorParamsEnum {
    DB_NAME("Database name","The name of the database to store the data in."),
    TABLE_NAME("Table Name","The name of the table in the database."),
    MAPPING_NAME("Mapping name","The name of the mapping responsible for storing the data in the appropriate columns."),
    FLUSH_IMMEDIATE("Flush immediate","Flush the content sent immediately to the ingest endpoint."),
    DATA_FORMAT("Data format","The format of the data that is sent to ADX."),
    IM_KIND("IngestionMappingKind","The type of the ingestion mapping related to the table in ADX."),
    IR_LEVEL("IngestionReportLevel","ADX can report events on several levels: None, Failure and Failure&Success."),
    IR_METHOD("IngestionReportMethod","ADX can report events on several methods: Table, Queue and Table&Queue."),
    ADX_SERVICE("AzureADXConnectionService","Service that provides the ADX-Connections."),
    WAIT_FOR_STATUS("Wait for status","Define the status to be waited on.");

    private String paramDisplayName;
    private String paramDescription;


    AzureAdxIngestProcessorParamsEnum(String paramDisplayName, String paramDescription) {
        this.paramDisplayName = paramDisplayName;
        this.paramDescription = paramDescription;
    }

    public String getParamDisplayName() {
        return paramDisplayName;
    }

    public String getParamDescription() {
        return paramDescription;
    }
}
