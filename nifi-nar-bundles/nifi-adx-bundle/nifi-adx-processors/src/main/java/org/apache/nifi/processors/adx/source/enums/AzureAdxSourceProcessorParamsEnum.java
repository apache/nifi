package org.apache.nifi.processors.adx.source.enums;

public enum AzureAdxSourceProcessorParamsEnum {

    DB_NAME("Database name","The name of the database to store the data in."),

    ADX_QUERY("ADX query", "The query which needs to be executed in Azure Data Explorer");

    private String paramDisplayName;

    private String paramDescription;

    AzureAdxSourceProcessorParamsEnum(String paramDisplayName, String paramDescription) {
        this.paramDisplayName = paramDisplayName;
        this.paramDescription = paramDescription;
    }

    public String getParamDisplayName() {
        return paramDisplayName;
    }

    public void setParamDisplayName(String paramDisplayName) {
        this.paramDisplayName = paramDisplayName;
    }

    public String getParamDescription() {
        return paramDescription;
    }

    public void setParamDescription(String paramDescription) {
        this.paramDescription = paramDescription;
    }
}
