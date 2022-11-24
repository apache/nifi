package org.apache.nifi.processors.adx;

import org.apache.nifi.adx.AzureAdxConnectionService;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.when;

public class AzureAdxSourceProcessorTest {

    private AzureAdxSourceProcessor azureAdxSourceProcessor;

    private MockProcessSession processSession;

    private SharedSessionState sharedState;


    private static final String MOCK_DB_NAME= "mockDBName";

    private static final String MOCK_QUERY= "mockQuery";


    private AzureAdxConnectionService azureAdxConnectionService;

    private TestRunner testRunner;


    @BeforeEach
    public void init() {

        azureAdxSourceProcessor = new AzureAdxSourceProcessor();
        final ProcessorInitializationContext initContext = Mockito.mock(ProcessorInitializationContext.class);
        final String componentId = "componentId";
        when(initContext.getIdentifier()).thenReturn(componentId);
        MockComponentLog componentLog = new MockComponentLog(componentId, azureAdxSourceProcessor);
        when(initContext.getLogger()).thenReturn(componentLog);
        azureAdxSourceProcessor.initialize(initContext);

        final ProcessSessionFactory processSessionFactory = Mockito.mock(ProcessSessionFactory.class);

        sharedState = new SharedSessionState(azureAdxSourceProcessor, new AtomicLong(0));
        processSession = new MockProcessSession(sharedState, azureAdxSourceProcessor);



    }

    public void testProcessorSuccess() throws InitializationException, IOException {
        testRunner = TestRunners.newTestRunner(azureAdxSourceProcessor);

        testRunner.setProperty(AzureAdxSourceProcessor.DB_NAME,MOCK_DB_NAME);
        testRunner.setProperty(AzureAdxSourceProcessor.ADX_QUERY,MOCK_QUERY);

        testRunner.setValidateExpressionUsage(false);

        azureAdxConnectionService = new MockAzureAdxConnectionService();

        testRunner.addControllerService("adx-connection-service", azureAdxConnectionService, new HashMap<>());

        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.INGEST_URL,"http://ingest-sample.com/");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_ID,"sample");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_KEY,"sample");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.APP_TENANT,"sample");
        testRunner.setProperty(azureAdxConnectionService,AzureAdxConnectionService.CLUSTER_URL, "http://sample.com/");

        testRunner.enableControllerService(azureAdxConnectionService);
        testRunner.assertValid(azureAdxConnectionService);
        InputStream inputStream = this.getClass().getResourceAsStream("/fileQueuedSuccess.csv");
        testRunner.enqueue(inputStream);
        assert inputStream != null;
        inputStream.close();
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(AzureAdxIngestProcessor.RL_SUCCEEDED);
        testRunner.assertAllFlowFilesTransferred(AzureAdxIngestProcessor.RL_SUCCEEDED);
    }
}
