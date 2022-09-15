/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
