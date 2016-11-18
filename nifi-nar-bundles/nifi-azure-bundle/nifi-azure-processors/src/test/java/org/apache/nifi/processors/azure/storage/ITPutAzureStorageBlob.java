package org.apache.nifi.processors.azure.storage;

import java.io.IOException;
import java.util.List;

import org.apache.nifi.processors.azure.AzureConstants;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class ITPutAzureStorageBlob extends AbstractAzureIT {

    @Test
    public void testPuttingBlob() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutAzureBlobStorage());

        runner.setValidateExpressionUsage(true);

        runner.setProperty(AzureConstants.ACCOUNT_NAME, getAccountName());
        runner.setProperty(AzureConstants.ACCOUNT_KEY, getAccountKey());
        runner.setProperty(AzureConstants.CONTAINER, TEST_CONTAINER_NAME);
        runner.setProperty(FetchAzureBlobStorage.BLOB, "testingUpload");
        runner.setProperty(FetchAzureBlobStorage.BLOB_TYPE, AzureConstants.BLOCK.getValue());

        runner.enqueue("0123456789".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(PutAzureBlobStorage.REL_SUCCESS, 1);
        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(PutAzureBlobStorage.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFilesForRelationship) {
            flowFile.assertContentEquals("0123456789".getBytes());
            flowFile.assertAttributeEquals("azure.length", "10");
        }
    }
}
