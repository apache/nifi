package org.apache.nifi.processors.azure.storage;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import org.apache.nifi.processors.azure.AzureConstants;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.microsoft.azure.storage.StorageException;

public class ITPutAzureStorageTable extends AbstractAzureIT {

    private static final String TEST_PARTITON_KEY = "testPartition";
    private static final String TEST_ROW_KEY = "testKey";

    @BeforeClass
    public static void setupTable() throws InvalidKeyException, StorageException, URISyntaxException {
        getTable().createIfNotExists();
    }

    @AfterClass
    public static void tearDownTable() throws InvalidKeyException, StorageException, URISyntaxException {
        getTable().deleteIfExists();
    }

    @Test
    public void testPutAzureTableJson() {
        final TestRunner runner = TestRunners.newTestRunner(new PutAzureTableJson());

        runner.setValidateExpressionUsage(true);

        runner.setProperty(AzureConstants.ACCOUNT_NAME, getAccountName());
        runner.setProperty(AzureConstants.ACCOUNT_KEY, getAccountKey());
        runner.setProperty(AzureConstants.CONTAINER, TEST_CONTAINER_NAME);
        runner.setProperty(PutAzureTableJson.TABLE, TEST_TABLE_NAME);
        runner.setProperty(PutAzureTableJson.PARTITION, TEST_PARTITON_KEY);
        runner.setProperty(PutAzureTableJson.ROW, TEST_ROW_KEY);

        InputStream data = getClass().getResourceAsStream("/table.json");
        runner.enqueue(data);

        runner.run();
        
        runner.assertAllFlowFilesTransferred(PutAzureTableJson.REL_SUCCESS, 1);
    }
}
