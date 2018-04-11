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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.couchbase.CouchbaseAttributes;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.error.DocumentDoesNotExistException;

@Tags({"nosql", "couchbase", "database", "get"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Get a document from Couchbase Server via Key/Value access. The ID of the document to fetch may be supplied by setting the <Document Id> property. "
    + "NOTE: if the Document Id property is not set, the contents of the FlowFile will be read to determine the Document Id, which means that the contents of the entire "
    + "FlowFile will be buffered in memory.")
@WritesAttributes({
    @WritesAttribute(attribute = "couchbase.cluster", description = "Cluster where the document was retrieved from."),
    @WritesAttribute(attribute = "couchbase.bucket", description = "Bucket where the document was retrieved from."),
    @WritesAttribute(attribute = "couchbase.doc.id", description = "Id of the document."),
    @WritesAttribute(attribute = "couchbase.doc.cas", description = "CAS of the document."),
    @WritesAttribute(attribute = "couchbase.doc.expiry", description = "Expiration of the document."),
    @WritesAttribute(attribute = "couchbase.exception", description = "If Couchbase related error occurs the CouchbaseException class name will be captured here.")
})
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class GetCouchbaseKey extends AbstractCouchbaseProcessor {

    @Override
    protected void addSupportedProperties(final List<PropertyDescriptor> descriptors) {
        descriptors.add(DOCUMENT_TYPE);
        descriptors.add(DOC_ID);
    }

    @Override
    protected void addSupportedRelationships(final Set<Relationship> relationships) {
        relationships.add(REL_SUCCESS);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_RETRY);
        relationships.add(REL_FAILURE);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile inFile = session.get();
        if (inFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();
        final ComponentLog logger = getLogger();
        String docId = null;
        if (context.getProperty(DOC_ID).isSet()) {
            docId = context.getProperty(DOC_ID).evaluateAttributeExpressions(inFile).getValue();
        } else {
            final byte[] content = new byte[(int) inFile.getSize()];
            session.read(inFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, content, true);
                }
            });
            docId = new String(content, StandardCharsets.UTF_8);
        }

        if (StringUtils.isEmpty(docId)) {
            throw new ProcessException("Please check 'Document Id' setting. Couldn't get document id from " + inFile);
        }

        try {
            final Document<?> doc;
            final byte[] content;
            final Bucket bucket = openBucket(context);
            final DocumentType documentType = DocumentType.valueOf(context.getProperty(DOCUMENT_TYPE).getValue());

            switch (documentType) {
                case Json: {
                    RawJsonDocument document = bucket.get(docId, RawJsonDocument.class);
                    if (document == null) {
                        doc = null;
                        content = null;
                    } else {
                        content = document.content().getBytes(StandardCharsets.UTF_8);
                        doc = document;
                    }
                    break;
                }
                case Binary: {
                    BinaryDocument document = bucket.get(docId, BinaryDocument.class);
                    if (document == null) {
                        doc = null;
                        content = null;
                    } else {
                        content = document.content().array();
                        doc = document;
                    }
                    break;
                }
                default: {
                    doc = null;
                    content = null;
                }
            }

            if (doc == null) {
                logger.error("Document {} was not found in {}; routing {} to failure", new Object[] {docId, getTransitUrl(context, docId), inFile});
                inFile = session.putAttribute(inFile, CouchbaseAttributes.Exception.key(), DocumentDoesNotExistException.class.getName());
                session.transfer(inFile, REL_FAILURE);
                return;
            }

            FlowFile outFile = session.create(inFile);
            outFile = session.write(outFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(content);
                }
            });

            final Map<String, String> updatedAttrs = new HashMap<>();
            updatedAttrs.put(CouchbaseAttributes.Cluster.key(), context.getProperty(COUCHBASE_CLUSTER_SERVICE).getValue());
            updatedAttrs.put(CouchbaseAttributes.Bucket.key(), context.getProperty(BUCKET_NAME).getValue());
            updatedAttrs.put(CouchbaseAttributes.DocId.key(), docId);
            updatedAttrs.put(CouchbaseAttributes.Cas.key(), String.valueOf(doc.cas()));
            updatedAttrs.put(CouchbaseAttributes.Expiry.key(), String.valueOf(doc.expiry()));
            outFile = session.putAllAttributes(outFile, updatedAttrs);

            final long fetchMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().fetch(outFile, getTransitUrl(context, docId), fetchMillis);
            session.transfer(outFile, REL_SUCCESS);
            session.transfer(inFile, REL_ORIGINAL);
        } catch (final CouchbaseException e) {
            String errMsg = String.format("Getting document %s from Couchbase Server using %s failed due to %s", docId, inFile, e);
            handleCouchbaseException(context, session, logger, inFile, e, errMsg);
        }
    }


}
