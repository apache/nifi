package org.apache.nifi.processors.azure.storage;

import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Properties;

import org.apache.nifi.util.file.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;

public abstract class AbstractAzureIT {
    protected final static String CREDENTIALS_FILE = System.getProperty("user.home") + "/azure-credentials.properties";
    public static final String TEST_CONTAINER_NAME = "nifitest";

    private static final Properties config;
    protected static final String TEST_BLOB_NAME = "testing";
    protected static final String TEST_TABLE_NAME = "testing";

    static {
        final FileInputStream fis;
        config = new Properties();
        try {
            fis = new FileInputStream(CREDENTIALS_FILE);
            try {
                config.load(fis);
            } catch (IOException e) {
                fail("Could not open credentials file " + CREDENTIALS_FILE + ": " + e.getLocalizedMessage());
            } finally {
                FileUtils.closeQuietly(fis);
            }
        } catch (FileNotFoundException e) {
            fail("Could not open credentials file " + CREDENTIALS_FILE + ": " + e.getLocalizedMessage());
        }

    }

    @BeforeClass
    public static void oneTimeSetup() throws StorageException, InvalidKeyException, URISyntaxException {
        CloudBlobContainer container = getContainer();
        container.createIfNotExists();
    }

    @AfterClass
    public static void tearDown() throws InvalidKeyException, URISyntaxException, StorageException {
        CloudBlobContainer container = getContainer();
        for (ListBlobItem blob : container.listBlobs()) {
            if (blob instanceof CloudBlob) {
                ((CloudBlob) blob).delete(DeleteSnapshotsOption.INCLUDE_SNAPSHOTS, null, null, null);
            }
        }
    }

    public static String getAccountName() {
        return config.getProperty("accountName");
    }

    public static String getAccountKey() {
        return config.getProperty("accountKey");
    }

    protected static CloudBlobContainer getContainer() throws InvalidKeyException, URISyntaxException, StorageException {
        String storageConnectionString = String.format("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s", getAccountName(), getAccountKey());
        CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
        return blobClient.getContainerReference(TEST_CONTAINER_NAME);
    }

    protected static CloudTable getTable() throws InvalidKeyException, URISyntaxException, StorageException {
        String storageConnectionString = String.format("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s", getAccountName(), getAccountKey());
        CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
        CloudTableClient tableClient = storageAccount.createCloudTableClient();
        return tableClient.getTableReference(TEST_TABLE_NAME);
    }

}