package org.apache.nifi.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.couchbase.GetCouchbaseKey;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;

import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COUCHBASE_CLUSTER_SERVICE;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractCouchbaseNifiTest {
    protected static final String SERVICE_ID = "couchbaseClusterService";
    protected final String bucketName = "bucket-1";
    protected final String scopeName = "scope-1";
    protected final String collectionName = "collection-1";
    protected TestRunner testRunner;

    @BeforeEach
    public void init() throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.couchbase.GetCouchbaseKey", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.couchbase.TestGetCouchbaseKey", "debug");

        testRunner = TestRunners.newTestRunner(processor());
    }

    protected abstract Class<? extends Processor> processor();

    protected Collection setupMockCouchbase(String bucket, String scope, String collection) throws InitializationException {
        Bucket bucketMock = mock(Bucket.class);
        Scope scopeMock = mock(Scope.class);
        Collection collectionMock = mock(Collection.class);
        CouchbaseClusterControllerService service = mock(CouchbaseClusterControllerService.class);
        when(bucketMock.name()).thenReturn(bucket);
        when(service.getIdentifier()).thenReturn(SERVICE_ID);
        when(service.openBucket(bucket)).thenReturn(bucketMock);
        when(service.openCollection(bucket, scope, collection)).thenReturn(collectionMock);
        when(bucketMock.scope(anyString())).thenReturn(scopeMock);
        when(scopeMock.name()).thenReturn(scope);
        when(scopeMock.collection(anyString())).thenReturn(collectionMock);
        when(collectionMock.name()).thenReturn(collection);
        when(collectionMock.bucketName()).thenReturn(bucket);
        when(collectionMock.scopeName()).thenReturn(scope);
        testRunner.addControllerService(SERVICE_ID, service);
        testRunner.enableControllerService(service);
        testRunner.setProperty(COUCHBASE_CLUSTER_SERVICE, SERVICE_ID);
        return collectionMock;
    }
}
