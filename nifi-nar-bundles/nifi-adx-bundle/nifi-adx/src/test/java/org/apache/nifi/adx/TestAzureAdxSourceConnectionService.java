package org.apache.nifi.adx;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.ManagedStreamingIngestClient;
import com.microsoft.azure.kusto.ingest.QueuedIngestClient;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAzureAdxSourceConnectionService {

    private TestRunner runner;

    private AzureAdxSourceConnectionService service;

    private static final String MOCK_APP_ID = "mockAppId";

    private static final String MOCK_APP_KEY = "mockAppKey";

    private static final String MOCK_APP_TENANT = "mockAppTenant";

    private static final String MOCK_CLUSTER_URL = "https://mockClusterUrl.com/";

    @BeforeEach
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(TestAzureAdxIngestProcessor.class);

        service = new AzureAdxSourceConnectionService();
        runner.addControllerService("test-good", service);

    }

    @AfterEach
    public void after() {
        runner.clearProperties();
    }

    @Test
    public void testAdxConnectionControllerWithoutStreaming() {
        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();

        runner.assertValid(service);
    }

    private void configureAppId() {
        runner.setProperty(service, AzureAdxSinkConnectionService.APP_ID, MOCK_APP_ID);
    }

    private void configureAppKey() {
        runner.setProperty(service, AzureAdxSinkConnectionService.APP_KEY, MOCK_APP_KEY);
    }

    private void configureAppTenant() {
        runner.setProperty(service, AzureAdxSinkConnectionService.APP_TENANT, MOCK_APP_TENANT);
    }

    private void configureClusterURL() {
        runner.setProperty(service, AzureAdxSinkConnectionService.CLUSTER_URL, MOCK_CLUSTER_URL);
    }

    @Test
    public void testCreateExecutionClientSuccess(){

        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();

        runner.assertValid(service);
        runner.setValidateExpressionUsage(false);

        runner.enableControllerService(service);

        Client executionClient = service.getKustoExecutionClient();
        Assertions.assertNotNull(executionClient);

    }

    @Test
    public void testPropertyDescriptor(){

        configureAppId();
        configureAppKey();
        configureAppTenant();
        configureClusterURL();

        List<PropertyDescriptor> pd = service.getSupportedPropertyDescriptors();
        assertTrue(pd.contains(AzureAdxSinkConnectionService.APP_ID));
        assertTrue(pd.contains(AzureAdxSinkConnectionService.APP_KEY));
        assertTrue(pd.contains(AzureAdxSinkConnectionService.APP_TENANT));
        assertTrue(pd.contains(AzureAdxSinkConnectionService.CLUSTER_URL));
    }


    @Test
    public void testInvalidConnectionMissingProperty(){

        configureAppId();
        configureAppKey();
        configureAppTenant();

        runner.assertNotValid(service);
        runner.setValidateExpressionUsage(false);

        assertThrows(IllegalStateException.class,()->{
            runner.enableControllerService(service);
        });

    }
}
