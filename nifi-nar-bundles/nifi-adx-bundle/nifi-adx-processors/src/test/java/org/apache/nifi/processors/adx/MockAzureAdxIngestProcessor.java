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
package org.apache.nifi.processors.adx;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.processor.ProcessContext;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class MockAzureAdxIngestProcessor extends AzureAdxIngestProcessor{

    @Override
    protected boolean isStreamingPolicyEnabled(
            String entityType, String entityName, Client engineClient, String database) throws DataClientException, DataServiceException {
        return true;
    }

    @Override
    protected boolean isIngestorRole(String databaseName,String tableName,Client executionClient) {
        return true;
    }


    @Override
    protected String addModifyBatchingPolicy(IngestionProperties ingestionProperties, ProcessContext context){
        return null;
    }

    @Override
    protected String showOriginalTableRetentionPolicy(IngestionProperties ingestionProperties) {
        return "sampleRetentionPolicy";
    }

    @Override
    protected void dropTempTableIfExists(IngestionProperties ingestionPropertiesCreateTempTable) {
    }

    @Override
    protected void createTempTable(IngestionProperties ingestionPropertiesCreateTempTable, IngestionProperties ingestionProperties, String columnsAsSchema) {
    }

    @Override
    protected void alterTempTableRetentionPolicy(IngestionProperties ingestionPropertiesCreateTempTable,ProcessContext context) {
    }

    @Override
    protected void alterTempTableAutoDeletePolicy(IngestionProperties ingestionPropertiesCreateTempTable, String expiryDate) {
    }

    @Override
    protected void applyTempTableBatchingPolicy(IngestionProperties ingestionProperties, String ingestionBatchingString) {
    }

    protected boolean isNifiClusteredSetup(NodeTypeProvider nodeTypeProvider){
        return nodeTypeProvider.isClustered();
    }

    protected boolean shouldUseMaterializedViewFlag(String databaseName, String tableName) {
        return false;
    }

    protected String executeMoveExtentsAsyncOperation(String databaseName, String moveExtentsQuery) {
        return "1234";
    }

    protected String pollAndFindExtentMergeAsyncOperation(final String databaseName, final String showOperationsQuery, final String stateCol) {
        return "Completed";
    }

}
