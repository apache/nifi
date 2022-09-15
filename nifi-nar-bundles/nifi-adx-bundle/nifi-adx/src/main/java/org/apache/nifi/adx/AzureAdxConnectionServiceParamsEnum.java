package org.apache.nifi.adx;

public enum AzureAdxConnectionServiceParamsEnum {
    INGEST_URL("Ingest URL","URL of the ingestion endpoint of the azure data explorer cluster."),
    APP_ID("Application ID","Azure application ID for accessing the ADX-Cluster"),
    APP_KEY("Application KEY","Azure application Key for accessing the ADX-Cluster"),
    APP_TENANT("Application Tenant","Azure application tenant for accessing the ADX-Cluster"),
    IS_STREAMING_ENABLED("Is Streaming enabled","This property determines whether we want to stream data to ADX."),
    CLUSTER_URL("Cluster URL","Endpoint of ADX cluster. This is required only when streaming data to ADX cluster is enabled.");

    private String paramDisplayName;
    private String description;


    AzureAdxConnectionServiceParamsEnum(String paramDisplayName, String description) {
        this.paramDisplayName = paramDisplayName;
        this.description = description;
    }

    public String getParamDisplayName() {
        return paramDisplayName;
    }

    public String getDescription() {
        return description;
    }
}
