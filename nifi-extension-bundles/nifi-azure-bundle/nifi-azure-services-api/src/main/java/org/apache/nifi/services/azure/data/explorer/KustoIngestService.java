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
package org.apache.nifi.services.azure.data.explorer;

import org.apache.nifi.controller.ControllerService;

/**
 * Kusto Ingest Service interface for Azure Data Explorer
 */
public interface KustoIngestService extends ControllerService {
    /**
     * Ingest data stream and return result status
     *
     * @param kustoIngestionRequest Kusto Ingestion Request with input stream
     * @return Ingestion Result with status
     */
    KustoIngestionResult ingestData(KustoIngestionRequest kustoIngestionRequest);

    /**
     * Is Streaming Policy Enabled for specified database
     *
     * @param databaseName Database Name to be checked
     * @return Streaming Policy Enabled status
     */
    boolean isStreamingPolicyEnabled(String databaseName);

    /**
     * Is Database Table Readable for specified database and table name
     *
     * @param databaseName Database Name to be checked
     * @param table Table Name to be checked
     * @return Table Readable status
     */
    boolean isTableReadable(String databaseName, String table);
}
