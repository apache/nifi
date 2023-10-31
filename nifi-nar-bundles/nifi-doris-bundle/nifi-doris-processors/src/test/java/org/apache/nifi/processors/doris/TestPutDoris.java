package org.apache.nifi.processors.doris;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestPutDoris {

    @Test
    public void testMainProcesses() throws InitializationException {
        TestRunner runner = getTestRunner();
        runner.setProperty(PutDoris.BATCH_SIZE, "2");
        getDorisClientServiceTest(runner);
        String data = "{\"a\": \"111s\", \"b\": 1, \"c\": \"1s\", \"d\": null, \"database\": \"cdc1\", \"table_name\": \"test1\",\"op\":\"insert\"}\n" +
                "{\"a\": \"55\", \"b\": 55, \"c\": \"ddddsss\", \"d\": null, \"database\": \"cdc1\", \"table_name\": \"test1\",\"op\":\"insert\"}\n" +
                "{\"a\": \"a01\", \"b\": 1103, \"c\": \"c01\", \"d\": \"d01\", \"database\": \"cdc1\", \"table_name\": \"test2\",\"op\":\"insert\"}\n" +
                "{\"a\": \"a1\", \"b\": 1002, \"c\": \"a1\", \"d\": \"a1\", \"database\": \"cdc1\", \"table_name\": \"test1\",\"op\":\"insert\"}\n" +
                "{\"a\": \"a2\", \"b\": 1003, \"c\": \"a1\", \"d\": \"a1\", \"database\": \"cdc1\", \"table_name\": \"test1\",\"op\":\"insert\"}\n" +
                "{\"a\": \"aa\", \"b\": 101, \"c\": \"cc\", \"d\": \"dd\", \"database\": \"cdc1\", \"table_name\": \"test1\",\"op\":\"update\"}\n" +
                "{\"a\": \"ddd\", \"b\": 35, \"c\": \"w\", \"d\": \"d\", \"database\": \"cdc1\", \"table_name\": \"test1\",\"op\":\"insert\"}\n" +
                "{\"a\": \"ssd\", \"b\": 23, \"c\": \"sd\", \"d\": \"x\", \"database\": \"cdc1\", \"table_name\": \"test1\",\"op\":\"delete\"}\n" +
                "{\"a\": \"ssds\", \"b\": 231, \"c\": \"dd\", \"d\": \"ssss\", \"database\": \"cdc1\", \"table_name\": \"test1\",\"op\":\"insert\"}";
        MockFlowFile mockFlowFile = new MockFlowFile(1);
        MockFlowFile mockFlowFile2 = new MockFlowFile(2);
        mockFlowFile2.setData(data.getBytes());
        mockFlowFile.setData(data.getBytes());

        Map<String, String> attributes = new HashMap<>();
        mockFlowFile.putAttributes(attributes);

        runner.enqueue(mockFlowFile,mockFlowFile2);
        runner.run();
        runner.assertValid();
    }



    private TestRunner getTestRunner() {
        TestRunner testRunner = TestRunners.newTestRunner(PutDoris.class);
        return testRunner;
    }

    private void getDorisClientServiceTest(TestRunner testRunner) throws InitializationException {
        MockDorisClientServiceTest mockDorisClientServiceTest = new MockDorisClientServiceTest();
        testRunner.addControllerService("dorisClient", mockDorisClientServiceTest);
        testRunner.enableControllerService(mockDorisClientServiceTest);
        testRunner.setProperty(PutDoris.DORIS_CLIENT_SERVICE, "dorisClient");
    }
}
