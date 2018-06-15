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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.couchbase.CouchbaseUtils;
import org.apache.nifi.couchbase.DocumentType;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.error.DocumentDoesNotExistException;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COUCHBASE_CLUSTER_SERVICE;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.DOCUMENT_TYPE;

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

    public static final PropertyDescriptor PUT_VALUE_TO_ATTRIBUTE = new PropertyDescriptor.Builder()
        .name("put-to-attribute")
        .displayName("Put Value to Attribute")
        .description("If set, the retrieved value will be put into an attribute of the FlowFile instead of a the content of the FlowFile." +
                " The attribute key to put to is determined by evaluating value of this property.")
        .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    private volatile boolean putToAttribute = false;

    @Override
    protected void addSupportedProperties(final List<PropertyDescriptor> descriptors) {
        descriptors.add(DOCUMENT_TYPE);
        descriptors.add(DOC_ID);
        descriptors.add(PUT_VALUE_TO_ATTRIBUTE);
    }

    @Override
    protected void addSupportedRelationships(final Set<Relationship> relationships) {
        relationships.add(new Relationship.Builder().name(REL_ORIGINAL.getName())
                .description("The original input FlowFile is routed to this relationship" +
                        " when the value is retrieved from Couchbase Server and routed to 'success'.").build());
        relationships.add(new Relationship.Builder().name(REL_SUCCESS.getName())
                .description("Values retrieved from Couchbase Server are written as outgoing FlowFiles content" +
                        " or put into an attribute of the incoming FlowFile and routed to this relationship.").build());
        relationships.add(new Relationship.Builder().name(REL_RETRY.getName())
                .description("All FlowFiles failed to fetch from Couchbase Server but can be retried are routed to this relationship.").build());
        relationships.add(new Relationship.Builder().name(REL_FAILURE.getName())
                .description("All FlowFiles failed to fetch from Couchbase Server and not retry-able are routed to this relationship.").build());
    }

    @Override
    protected Set<Relationship> filterRelationships(Set<Relationship> rels) {
        // If destination is attribute, then success == original.
        return rels.stream().filter(rel -> !REL_ORIGINAL.equals(rel) || !putToAttribute).collect(Collectors.toSet());
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if (PUT_VALUE_TO_ATTRIBUTE.equals(descriptor)) {
            putToAttribute = !isEmpty(newValue);
        }
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

        if (isEmpty(docId)) {
            throw new ProcessException("Please check 'Document Id' setting. Couldn't get document id from " + inFile);
        }

        String putTargetAttr = null;
        if (context.getProperty(PUT_VALUE_TO_ATTRIBUTE).isSet()) {
            putTargetAttr = context.getProperty(PUT_VALUE_TO_ATTRIBUTE).evaluateAttributeExpressions(inFile).getValue();
            if (isEmpty(putTargetAttr)) {
                inFile = session.putAttribute(inFile, CouchbaseAttributes.Exception.key(), "InvalidPutTargetAttributeName");
                session.transfer(inFile, REL_FAILURE);
                return;
            }
        }

        try {
            final Bucket bucket = openBucket(context);
            final DocumentType documentType = DocumentType.valueOf(context.getProperty(DOCUMENT_TYPE).getValue());
            Document<?> doc = null;
            // A function to write a document into outgoing FlowFile content.
            OutputStreamCallback outputStreamCallback = null;
            final Map<String, String> updatedAttrs = new HashMap<>();

            switch (documentType) {
                case Json: {
                    RawJsonDocument document = bucket.get(docId, RawJsonDocument.class);
                    if (document != null) {
                        outputStreamCallback = out -> {
                            final byte[] content = document.content().getBytes(StandardCharsets.UTF_8);
                            out.write(content);
                            updatedAttrs.put(CoreAttributes.MIME_TYPE.key(), "application/json");
                        };
                        doc = document;
                    }
                    break;
                }
                case Binary: {
                    BinaryDocument document = bucket.get(docId, BinaryDocument.class);
                    if (document != null) {
                        outputStreamCallback = out -> {
                            // Write to OutputStream without copying any to heap.
                            final ByteBuf byteBuf = document.content();
                            byteBuf.getBytes(byteBuf.readerIndex(), out, byteBuf.readableBytes());
                            byteBuf.release();
                        };
                        doc = document;
                    }
                    break;
                }
            }

            if (doc == null) {
                logger.warn("Document {} was not found in {}; routing {} to failure", new Object[] {docId, getTransitUrl(bucket, docId), inFile});
                inFile = session.putAttribute(inFile, CouchbaseAttributes.Exception.key(), DocumentDoesNotExistException.class.getName());
                session.transfer(inFile, REL_FAILURE);
                return;
            }

            FlowFile outFile;
            if (putToAttribute) {
                outFile = inFile;
                updatedAttrs.put(putTargetAttr, CouchbaseUtils.getStringContent(doc.content()));
            } else {
                outFile = session.create(inFile);
                outFile = session.write(outFile, outputStreamCallback);
                session.transfer(inFile, REL_ORIGINAL);
            }

            updatedAttrs.put(CouchbaseAttributes.Cluster.key(), context.getProperty(COUCHBASE_CLUSTER_SERVICE).getValue());
            updatedAttrs.put(CouchbaseAttributes.Bucket.key(), bucket.name());
            updatedAttrs.put(CouchbaseAttributes.DocId.key(), docId);
            updatedAttrs.put(CouchbaseAttributes.Cas.key(), String.valueOf(doc.cas()));
            updatedAttrs.put(CouchbaseAttributes.Expiry.key(), String.valueOf(doc.expiry()));
            outFile = session.putAllAttributes(outFile, updatedAttrs);

            final long fetchMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().fetch(outFile, getTransitUrl(bucket, docId), fetchMillis);
            session.transfer(outFile, REL_SUCCESS);

        } catch (final CouchbaseException e) {
            String errMsg = String.format("Getting document %s from Couchbase Server using %s failed due to %s", docId, inFile, e);
            handleCouchbaseException(context, session, logger, inFile, e, errMsg);
        }
    }


}
