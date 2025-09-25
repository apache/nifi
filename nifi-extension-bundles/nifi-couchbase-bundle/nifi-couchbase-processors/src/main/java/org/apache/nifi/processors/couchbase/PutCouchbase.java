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
package org.apache.nifi.processors.couchbase;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.services.couchbase.CouchbaseClient;
import org.apache.nifi.services.couchbase.CouchbaseConnectionService;
import org.apache.nifi.services.couchbase.exception.CouchbaseException;
import org.apache.nifi.services.couchbase.utils.CouchbaseContext;
import org.apache.nifi.services.couchbase.utils.CouchbaseUpsertResult;

import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.BUCKET_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.BUCKET_ATTRIBUTE_DESC;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.CAS_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.CAS_ATTRIBUTE_DESC;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.COLLECTION_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.COLLECTION_ATTRIBUTE_DESC;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.DOCUMENT_ID_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.DOCUMENT_ID_ATTRIBUTE_DESC;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.SCOPE_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.SCOPE_ATTRIBUTE_DESC;

@Tags({"nosql", "couchbase", "database", "put"})
@CapabilityDescription("Put a document to Couchbase Server.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@ReadsAttributes({
        @ReadsAttribute(attribute = "uuid", description = "Used as a document id if 'Document Id' is not specified")
})
@WritesAttributes({
        @WritesAttribute(attribute = BUCKET_ATTRIBUTE, description = BUCKET_ATTRIBUTE_DESC),
        @WritesAttribute(attribute = SCOPE_ATTRIBUTE, description = SCOPE_ATTRIBUTE_DESC),
        @WritesAttribute(attribute = COLLECTION_ATTRIBUTE, description = COLLECTION_ATTRIBUTE_DESC),
        @WritesAttribute(attribute = DOCUMENT_ID_ATTRIBUTE, description = DOCUMENT_ID_ATTRIBUTE_DESC),
        @WritesAttribute(attribute = CAS_ATTRIBUTE, description = CAS_ATTRIBUTE_DESC)
})
public class PutCouchbase extends AbstractCouchbaseProcessor {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();
        final CouchbaseConnectionService connectionService = context.getProperty(COUCHBASE_CONNECTION_SERVICE).asControllerService(CouchbaseConnectionService.class);
        final String documentId = context.getProperty(DOCUMENT_ID).evaluateAttributeExpressions(flowFile).getValue();

        final CouchbaseContext couchbaseContext = getCouchbaseContext(context, flowFile);
        final CouchbaseClient couchbaseClient = connectionService.getClient(couchbaseContext);

        try {
            final CouchbaseUpsertResult result = couchbaseClient.upsertDocument(documentId, readFlowFileContent(session, flowFile));

            flowFile = session.putAllAttributes(flowFile, getFlowfileAttributes(couchbaseContext, documentId, String.valueOf(result.cas())));

            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().send(flowFile, createTransitUri(connectionService.getServiceLocation(), couchbaseContext, documentId), transferMillis);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (CouchbaseException e) {
            handleCouchbaseException(couchbaseClient, context, session, getLogger(), flowFile, e, "Failed to upsert document into Couchbase");
        }
    }
}
