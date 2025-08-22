/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.couchbase.integration;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.GetResult;
import org.apache.nifi.processors.couchbase.PutCouchbase;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.BUCKET_NAME;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.COUCHBASE_CONNECTION_SERVICE;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.DOCUMENT_ID;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_FAILURE;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_RETRY;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.BUCKET_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.CAS_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.COLLECTION_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.DEFAULT_COLLECTION;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.DEFAULT_SCOPE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.SCOPE_ATTRIBUTE;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PutCouchbaseIT extends AbstractCouchbaseIT {

    @Test
    public void testPutDocument() throws InitializationException {
        runner = TestRunners.newTestRunner(PutCouchbase.class);
        initConnectionService();
        runner.setProperty(DOCUMENT_ID, TEST_DOCUMENT_ID);
        runner.setProperty(BUCKET_NAME, TEST_BUCKET_NAME);
        runner.setProperty(COUCHBASE_CONNECTION_SERVICE, SERVICE_ID);
        runner.enqueue(TEST_DATA.getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_FAILURE, 0);
        runner.assertTransferCount(REL_RETRY, 0);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        outFile.assertAttributeEquals(BUCKET_ATTRIBUTE, TEST_BUCKET_NAME);
        outFile.assertAttributeEquals(SCOPE_ATTRIBUTE, DEFAULT_SCOPE);
        outFile.assertAttributeEquals(COLLECTION_ATTRIBUTE, DEFAULT_COLLECTION);
        outFile.assertAttributeExists(CAS_ATTRIBUTE);

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord receiveEvent = provenanceEvents.getFirst();
        assertEquals(ProvenanceEventType.SEND, receiveEvent.getEventType());
        assertEquals(
                StringUtils.join(Arrays.asList(container.getConnectionString(), TEST_BUCKET_NAME, DEFAULT_SCOPE, DEFAULT_COLLECTION, TEST_DOCUMENT_ID), "/"),
                receiveEvent.getTransitUri());

        assertEquals(TEST_DATA.replaceAll("\\s", ""), getTestDocument());
    }

    private String getTestDocument() {
        try (Cluster cluster = Cluster.connect(
                container.getConnectionString(),
                container.getUsername(),
                container.getPassword()
        )) {
            final Bucket bucket = cluster.bucket(TEST_BUCKET_NAME);
            final Collection collection = bucket.defaultCollection();

            final GetResult result = collection.get(TEST_DOCUMENT_ID);
            return result.contentAsObject().toString();
        }
    }
}
