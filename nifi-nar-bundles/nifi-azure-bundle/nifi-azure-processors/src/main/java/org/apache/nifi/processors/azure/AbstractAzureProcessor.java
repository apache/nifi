package org.apache.nifi.processors.azure;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;

import com.microsoft.azure.storage.CloudStorageAccount;

public abstract class AbstractAzureProcessor extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All FlowFiles that are received are routed to success").build();
    protected static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("Any failed fetches will be transferred to the failure relation.").build();
    public static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    protected CloudStorageAccount createStorageConnection(ProcessContext context) {
        final String accountName = context.getProperty(AzureConstants.ACCOUNT_NAME).evaluateAttributeExpressions().getValue();
        final String accountKey = context.getProperty(AzureConstants.ACCOUNT_KEY).evaluateAttributeExpressions().getValue();
        return createStorageConnectionFromNameAndKey(accountName, accountKey);
    }

    protected CloudStorageAccount createStorageConnection(ProcessContext context, FlowFile flowFile) {
        final String accountName = context.getProperty(AzureConstants.ACCOUNT_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String accountKey = context.getProperty(AzureConstants.ACCOUNT_KEY).evaluateAttributeExpressions(flowFile).getValue();
        return createStorageConnectionFromNameAndKey(accountName, accountKey);
    }

    private CloudStorageAccount createStorageConnectionFromNameAndKey(String accountName, String accountKey) {
        final String storageConnectionString = String.format("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s", accountName, accountKey);
        try {
            return createStorageAccountFromConnectionString(storageConnectionString);
        } catch (InvalidKeyException | IllegalArgumentException | URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Validates the connection string and returns the storage account. The connection string must be in the Azure connection string format.
     *
     * @param storageConnectionString
     *            Connection string for the storage service or the emulator
     * @return The newly created CloudStorageAccount object
     *
     * @throws URISyntaxException
     * @throws IllegalArgumentException
     * @throws InvalidKeyException
     */
    protected static CloudStorageAccount createStorageAccountFromConnectionString(String storageConnectionString) throws IllegalArgumentException, URISyntaxException, InvalidKeyException {
        CloudStorageAccount storageAccount;
        try {
            storageAccount = CloudStorageAccount.parse(storageConnectionString);
        } catch (IllegalArgumentException | URISyntaxException e) {
            throw e;
        } catch (InvalidKeyException e) {
            throw e;
        }
        return storageAccount;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

}
