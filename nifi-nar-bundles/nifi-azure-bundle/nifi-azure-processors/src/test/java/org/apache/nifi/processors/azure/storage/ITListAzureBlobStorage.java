package org.apache.nifi.processors.azure.storage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import org.apache.nifi.processors.azure.AzureConstants;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public class ITListAzureBlobStorage extends AbstractAzureIT {

    @BeforeClass
    public static void setupSomeFiles() throws InvalidKeyException, URISyntaxException, StorageException, IOException {
        CloudBlobContainer container = getContainer();
        container.createIfNotExists();

        CloudBlob blob = container.getBlockBlobReference(TEST_BLOB_NAME);
        byte[] buf = "0123456789".getBytes();
        InputStream in = new ByteArrayInputStream(buf);
        blob.upload(in, 10);
    }

    @AfterClass
    public static void tearDown() throws InvalidKeyException, URISyntaxException, StorageException {
        CloudBlobContainer container = getContainer();
        container.deleteIfExists();
    }

    @Test
    public void testListsAzureBlobStorageContent() {
        final TestRunner runner = TestRunners.newTestRunner(new ListAzureBlobStorage());

        runner.setProperty(AzureConstants.ACCOUNT_NAME, getAccountName());
        runner.setProperty(AzureConstants.ACCOUNT_KEY, getAccountKey());
        runner.setProperty(AzureConstants.CONTAINER, TEST_CONTAINER_NAME);

        // requires multiple runs to deal with List processor checking
        runner.run(3);

        runner.assertTransferCount(ListAzureBlobStorage.REL_SUCCESS, 1);
        runner.assertAllFlowFilesTransferred(ListAzureBlobStorage.REL_SUCCESS, 1);

        for (MockFlowFile entry : runner.getFlowFilesForRelationship(ListAzureBlobStorage.REL_SUCCESS)) {
            entry.assertAttributeEquals("azure.length", "10");
            entry.assertAttributeEquals("mime.type", "application/octet-stream");
        }
    }
}
