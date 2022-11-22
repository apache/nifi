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
package org.apache.nifi.processors.adx.sink.enums;

public enum AzureAdxIngestProcessorParamsEnum {
    DB_NAME("Database name","The name of the database to store the data in."),
    TABLE_NAME("Table Name","The name of the table in the database."),
    MAPPING_NAME("Mapping name","The name of the mapping responsible for storing the data in the appropriate columns."),
    FLUSH_IMMEDIATE("Flush immediate","Flush the content sent immediately to the ingest endpoint."),
    DATA_FORMAT("Data format","The format of the data that is sent to ADX."),
    IM_KIND("Ingestion Mapping Kind","The type of the ingestion mapping related to the table in ADX."),
    IR_LEVEL("Ingestion Report Level","ADX can report events on several levels: None, Failure and Failure&Success."),
    IR_METHOD("Ingestion Report Method","ADX can report events on several methods: Table, Queue and Table&Queue."),
    ADX_SERVICE("Azure ADXConnection Service","Service that provides the ADX-Connections."),
    WAIT_FOR_STATUS("Wait for status","Define the status to be waited on."),
    IS_TRANSACTIONAL("Ingestion Transactional Mode","Defines whether we want transactionality in the ingested data."),

    IS_IGNORE_FIRST_RECORD("Ingestion Ignore First Record","Defines whether ignore first record while ingestion."),

    MAX_BATCHING_TIME_SPAN("Maximum batching time span", "Maximum batching time span while queued ingestion"),

    MAX_BATCHING_NO_OF_ITEMS("Maximum number of Items","Maximum number of items during batching while queued ingestion"),

    MAX_BATCHING_RAW_DATA_SIZE_IN_MB("Maximum raw data size in MB","Maximum size of raw data in MBs while queued ingestion");

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
