package org.apache.nifi.processors.adx;

import org.apache.nifi.adx.AzureAdxSinkConnectionService;
import org.apache.nifi.adx.AzureAdxSourceConnectionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

public class AzureAdxSourceProcessorE2ETest {

    private AzureAdxSourceProcessor azureAdxSourceProcessor;

    private AzureAdxSourceConnectionService azureAdxSourceConnectionService;

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        azureAdxSourceProcessor = new AzureAdxSourceProcessor();
    }

    @Test
    public void testAzureAdxSourceProcessorSuccessE2E() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(azureAdxSourceProcessor);

        testRunner.setProperty(AzureAdxSourceProcessor.ADX_QUERY,System.getProperty("adxQuery"));
        testRunner.setProperty(AzureAdxSourceProcessor.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_SOURCE_SERVICE,"adx-connection-service");

        testRunner.setValidateExpressionUsage(false);

        azureAdxSourceConnectionService = new AzureAdxSourceConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxSourceConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(azureAdxSourceConnectionService);
        testRunner.assertValid(azureAdxSourceConnectionService);
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(org.apache.nifi.processors.adx.AzureAdxSinkProcessor.RL_SUCCEEDED);

    }

    @Test
    public void testAzureAdxIngestProcessorFailureE2E() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(azureAdxSourceProcessor);

        testRunner.setProperty(AzureAdxSourceProcessor.ADX_QUERY,System.getProperty("adxQueryLimit"));
        testRunner.setProperty(AzureAdxSourceProcessor.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_SOURCE_SERVICE,"adx-connection-service");

        testRunner.setValidateExpressionUsage(false);

        azureAdxSourceConnectionService = new AzureAdxSourceConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxSourceConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(azureAdxSourceConnectionService);
        testRunner.assertValid(azureAdxSourceConnectionService);
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(AzureAdxSinkProcessor.RL_FAILED);

    }

    @Test
    public void testAzureAdxIngestProcessorQueuedIngestionSingleNodeTransactionalFailureE2E() throws InitializationException, IOException {

        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner = TestRunners.newTestRunner(azureAdxSourceProcessor);

        testRunner.setProperty(AzureAdxSourceProcessor.ADX_QUERY,System.getProperty("adxQuery"));
        testRunner.setProperty(AzureAdxSourceProcessor.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_SOURCE_SERVICE,"adx-connection-service");

        testRunner.setValidateExpressionUsage(false);

        azureAdxSourceConnectionService = new AzureAdxSourceConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxSourceConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSinkConnectionService.CLUSTER_URL, System.getProperty("clusterUrl"));

        testRunner.enableControllerService(azureAdxSourceConnectionService);
        testRunner.assertValid(azureAdxSourceConnectionService);
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(AzureAdxSinkProcessor.RL_FAILED);

    }

}
