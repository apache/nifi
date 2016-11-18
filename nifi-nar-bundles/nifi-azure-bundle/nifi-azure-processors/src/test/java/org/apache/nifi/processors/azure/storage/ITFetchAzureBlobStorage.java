package org.apache.nifi.processors.azure.storage;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.processors.azure.AzureConstants;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import com.microsoft.azure.storage.StorageException;

public class ITFetchAzureBlobStorage extends AbstractAzureIT {

    @Test
    public void testFetchingBlob() throws InvalidKeyException, URISyntaxException, StorageException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(new FetchAzureBlobStorage());

        runner.setValidateExpressionUsage(true);

        runner.setProperty(AzureConstants.ACCOUNT_NAME, getAccountName());
        runner.setProperty(AzureConstants.ACCOUNT_KEY, getAccountKey());
        runner.setProperty(AzureConstants.CONTAINER, TEST_CONTAINER_NAME);
        runner.setProperty(FetchAzureBlobStorage.BLOB, "${azure.blobname}");
        //runner.setProperty(FetchAzureBlobStorage.BLOB_TYPE, "${azure.blobtype}");
        runner.setProperty(FetchAzureBlobStorage.BLOB_TYPE, AzureConstants.BLOCK.getValue());
        
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("azure.primaryUri", "http://" + getAccountName() + ".blob.core.windows.net/" + TEST_CONTAINER_NAME + "/" + TEST_BLOB_NAME);
        attributes.put("azure.blobname", TEST_BLOB_NAME);
        attributes.put("azure.blobtype", AzureConstants.BLOCK.getValue());
        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(FetchAzureBlobStorage.REL_SUCCESS, 1);
        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(FetchAzureBlobStorage.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFilesForRelationship) {
            flowFile.assertContentEquals("0123456789".getBytes());
            flowFile.assertAttributeEquals("azure.length", "10");
        }
    }
}
