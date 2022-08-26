package org.apache.nifi.processors.adx;


import org.apache.nifi.util.TestRunner;

public class MockAdxTestBase {

    protected static final String MOCK_DB_NAME = "MOCK_DB_NAME";

    protected TestRunner testRunner;

    protected void setBasicMockProperties(){
        if(testRunner != null){
            testRunner.setProperty(AzureAdxIngestProcessor.DB_NAME,MOCK_DB_NAME);
            //AzureAdxConnectionService service = new MockControllerService();
        }
    }

}
