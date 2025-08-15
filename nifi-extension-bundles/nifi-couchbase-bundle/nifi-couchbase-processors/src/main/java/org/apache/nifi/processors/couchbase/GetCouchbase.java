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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.services.couchbase.CouchbaseClient;
import org.apache.nifi.services.couchbase.exception.CouchbaseException;
import org.apache.nifi.services.couchbase.utils.CouchbaseContext;
import org.apache.nifi.services.couchbase.utils.CouchbaseGetResult;
import org.apache.nifi.services.couchbase.utils.DocumentType;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Map.entry;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.BUCKET_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.CAS_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.COLLECTION_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.DOCUMENT_ID_ATTRIBUTE;
import static org.apache.nifi.processors.couchbase.utils.CouchbaseAttributes.SCOPE_ATTRIBUTE;

@Tags({"nosql", "couchbase", "database", "get"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Get a document from Couchbase Server. The ID of the document to fetch may be supplied by setting " +
        "the <Document Id> property or reading it from the FlowFile content.")
@WritesAttributes({
        @WritesAttribute(attribute = BUCKET_ATTRIBUTE, description = "Bucket where the document was stored."),
        @WritesAttribute(attribute = SCOPE_ATTRIBUTE, description = "Scope where the document was stored."),
        @WritesAttribute(attribute = COLLECTION_ATTRIBUTE, description = "Collection where the document was stored."),
        @WritesAttribute(attribute = DOCUMENT_ID_ATTRIBUTE, description = "Id of the document."),
        @WritesAttribute(attribute = CAS_ATTRIBUTE, description = "CAS of the document.")
})
public class GetCouchbase extends AbstractCouchbaseProcessor {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();
        final String documentId = context.getProperty(DOCUMENT_ID).isSet()
                ? context.getProperty(DOCUMENT_ID).evaluateAttributeExpressions(flowFile).getValue()
                : new String(readFlowFileContent(session, flowFile), StandardCharsets.UTF_8);

        if (isEmpty(documentId)) {
            throw new ProcessException("Document ID is missing. Please provide a valid Document ID through processor property or FlowFile content.");
        }

        final String bucketName = context.getProperty(BUCKET_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String scopeName = context.getProperty(SCOPE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String collectionName = context.getProperty(COLLECTION_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final DocumentType documentType = DocumentType.valueOf(context.getProperty(DOCUMENT_TYPE).getValue());

        final CouchbaseContext couchbaseContext = new CouchbaseContext(bucketName, scopeName, collectionName, documentType);
        final CouchbaseClient couchbaseClient = connectionService.getClient(couchbaseContext);

        try {
            final CouchbaseGetResult result = couchbaseClient.getDocument(documentId);
            flowFile = session.write(flowFile, out -> out.write(result.resultContent()));

            final Map<String, String> attributes = Map.ofEntries(
                    entry(BUCKET_ATTRIBUTE, bucketName),
                    entry(SCOPE_ATTRIBUTE, scopeName),
                    entry(COLLECTION_ATTRIBUTE, collectionName),
                    entry(DOCUMENT_ID_ATTRIBUTE, documentId),
                    entry(CAS_ATTRIBUTE, String.valueOf(result.cas())),
                    entry(CoreAttributes.MIME_TYPE.key(), documentType.getMimeType())
            );
            flowFile = session.putAllAttributes(flowFile, attributes);

            final long fetchMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().fetch(flowFile, createTransitUrl(couchbaseContext, documentId), fetchMillis);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (CouchbaseException e) {
            handleCouchbaseException(couchbaseClient, context, session, getLogger(), flowFile, e, "Failed to get document from Couchbase");
        }
    }
}
