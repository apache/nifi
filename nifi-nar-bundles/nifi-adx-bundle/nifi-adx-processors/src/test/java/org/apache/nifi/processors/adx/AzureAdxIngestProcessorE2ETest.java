package org.apache.nifi.processors.adx;

import org.apache.nifi.adx.AzureAdxConnectionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

public class AzureAdxIngestProcessorE2ETest {

    private AzureAdxIngestProcessor azureAdxIngestProcessor;

    private AzureAdxConnectionService azureAdxConnectionService;

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        azureAdxIngestProcessor = new AzureAdxIngestProcessor();
    }

    @Test
    public void testAzureAdxIngestProcessorQueuedIngestionSingleNodeNonTransactional() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(azureAdxIngestProcessor);

        testRunner.setProperty(AzureAdxIngestProcessor.TABLE_NAME,System.getProperty("tableName"));
        testRunner.setProperty(AzureAdxIngestProcessor.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(AzureAdxIngestProcessor.MAPPING_NAME,System.getProperty("mappingName"));
        testRunner.setProperty(AzureAdxIngestProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_LEVEL,"IRL_FAS");
        testRunner.setProperty(AzureAdxIngestProcessor.WAIT_FOR_STATUS,"ST_SUCCESS");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_METHOD,"IRM_TABLE");
        testRunner.setProperty(AzureAdxIngestProcessor.ADX_SERVICE,"adx-connection-service");

        testRunner.setValidateExpressionUsage(false);

        azureAdxConnectionService = new AzureAdxConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxConnectionService, AzureAdxConnectionService.INGEST_URL,System.getProperty("ingestUrl"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(azureAdxConnectionService);
        testRunner.assertValid(azureAdxConnectionService);
        InputStream inputStream = this.getClass().getResourceAsStream("/file0.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(AzureAdxIngestProcessor.RL_SUCCEEDED);

    }

    @Test
    public void testAzureAdxIngestProcessorQueuedIngestionSingleNodeTransactional() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(azureAdxIngestProcessor);

        testRunner.setProperty(AzureAdxIngestProcessor.TABLE_NAME,System.getProperty("tableName"));
        testRunner.setProperty(AzureAdxIngestProcessor.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(AzureAdxIngestProcessor.MAPPING_NAME,System.getProperty("mappingName"));
        testRunner.setProperty(AzureAdxIngestProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_LEVEL,"IRL_FAS");
        testRunner.setProperty(AzureAdxIngestProcessor.WAIT_FOR_STATUS,"ST_SUCCESS");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_METHOD,"IRM_TABLE");
        testRunner.setProperty(AzureAdxIngestProcessor.ADX_SERVICE,"adx-connection-service");
        testRunner.setProperty(AzureAdxIngestProcessor.IS_TRANSACTIONAL,AzureAdxIngestProcessor.TRANSACTIONAL_YES.getValue());

        testRunner.setValidateExpressionUsage(false);

        azureAdxConnectionService = new AzureAdxConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxConnectionService, AzureAdxConnectionService.INGEST_URL,System.getProperty("ingestUrl"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(azureAdxConnectionService);
        testRunner.assertValid(azureAdxConnectionService);
        InputStream inputStream = this.getClass().getResourceAsStream("/file0.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(AzureAdxIngestProcessor.RL_SUCCEEDED);

    }

    @Test
    public void testAzureAdxIngestProcessorQueuedIngestionSingleNodeTransactionalFailure() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(azureAdxIngestProcessor);

        testRunner.setProperty(AzureAdxIngestProcessor.TABLE_NAME,System.getProperty("tableName"));
        testRunner.setProperty(AzureAdxIngestProcessor.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(AzureAdxIngestProcessor.MAPPING_NAME,System.getProperty("mappingName"));
        testRunner.setProperty(AzureAdxIngestProcessor.DATA_FORMAT,"CSV");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_LEVEL,"IRL_FAS");
        testRunner.setProperty(AzureAdxIngestProcessor.WAIT_FOR_STATUS,"ST_SUCCESS");
        testRunner.setProperty(AzureAdxIngestProcessor.IR_METHOD,"IRM_TABLE");
        testRunner.setProperty(AzureAdxIngestProcessor.ADX_SERVICE,"adx-connection-service");
        testRunner.setProperty(AzureAdxIngestProcessor.IS_TRANSACTIONAL,AzureAdxIngestProcessor.TRANSACTIONAL_YES.getValue());

        testRunner.setValidateExpressionUsage(false);

        azureAdxConnectionService = new AzureAdxConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxConnectionService, AzureAdxConnectionService.INGEST_URL,System.getProperty("ingestUrl"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(azureAdxConnectionService);
        testRunner.assertValid(azureAdxConnectionService);
        InputStream inputStream = this.getClass().getResourceAsStream("/file1.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(AzureAdxIngestProcessor.RL_FAILED);

    }


}
