package org.apache.nifi.processors.adx;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.processor.ProcessContext;

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

}
