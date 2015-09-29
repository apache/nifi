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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.couchbase.CouchbaseAttributes;
import org.apache.nifi.couchbase.CouchbaseClusterControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.RawJsonDocument;

@Tags({ "nosql", "couchbase", "database", "put" })
@CapabilityDescription("Put a document to Couchbase Server via Key/Value access.")
@SeeAlso({CouchbaseClusterControllerService.class})
@ReadsAttributes({
    @ReadsAttribute(attribute = "uuid", description = "Used as a document id if 'Document Id' is not specified"),
    @ReadsAttribute(attribute = "*", description = "Any attribute can be used as part of a document id by 'Document Id' expression.")
    })
@WritesAttributes({
    @WritesAttribute(attribute="couchbase.cluster", description="Cluster where the document was stored."),
    @WritesAttribute(attribute="couchbase.bucket", description="Bucket where the document was stored."),
    @WritesAttribute(attribute="couchbase.doc.id", description="Id of the document."),
    @WritesAttribute(attribute="couchbase.doc.cas", description="CAS of the document."),
    @WritesAttribute(attribute="couchbase.doc.expiry", description="Expiration of the document."),
    @WritesAttribute(attribute="couchbase.exception", description="If Couchbase related error occurs the CouchbaseException class name will be captured here.")
    })
public class PutCouchbaseKey extends AbstractCouchbaseProcessor {


    public static final PropertyDescriptor PERSIST_TO = new PropertyDescriptor
            .Builder().name("Persist To")
            .description("Durability constraint about disk persistence.")
            .required(true)
            .allowableValues(PersistTo.values())
            .defaultValue(PersistTo.NONE.toString())
            .build();

    public static final PropertyDescriptor REPLICATE_TO = new PropertyDescriptor
            .Builder().name("Replicate To")
            .description("Durability constraint about replication.")
            .required(true)
            .allowableValues(ReplicateTo.values())
            .defaultValue(ReplicateTo.NONE.toString())
            .build();

    @Override
    protected void addSupportedProperties(List<PropertyDescriptor> descriptors) {
        descriptors.add(DOCUMENT_TYPE);
        descriptors.add(DOC_ID);
        descriptors.add(PERSIST_TO);
        descriptors.add(REPLICATE_TO);
    }

    @Override
    protected void addSupportedRelationships(Set<Relationship> relationships) {
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ProcessorLog logger = getLogger();
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        String docId = null;
        final byte[] content = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, content, true);
            }
        });

        try {
            docId = String.valueOf(flowFile.getAttribute(CoreAttributes.UUID.key()));
            if(!StringUtils.isEmpty(context.getProperty(DOC_ID).getValue())){
                docId = context.getProperty(DOC_ID).evaluateAttributeExpressions(flowFile).getValue();
            }
        } catch (Throwable t) {
            throw new ProcessException("Please check 'Document Id' setting. Couldn't get document id from " + flowFile);
        }

        try {
            Document<?> doc = null;
            DocumentType documentType = DocumentType.valueOf(context.getProperty(DOCUMENT_TYPE).getValue());
            switch (documentType){
                case Json : {
                    doc = RawJsonDocument.create(docId, new String(content, StandardCharsets.UTF_8));
                    break;
                }
                case Binary : {
                    ByteBuf buf = Unpooled.copiedBuffer(content);
                    doc = BinaryDocument.create(docId, buf);
                    break;
                }
            }

            PersistTo persistTo = PersistTo.valueOf(context.getProperty(PERSIST_TO).getValue());
            ReplicateTo replicateTo = ReplicateTo.valueOf(context.getProperty(REPLICATE_TO).getValue());
            doc = openBucket(context).upsert(doc, persistTo, replicateTo);
            Map<String, String> updatedAttrs = new HashMap<>();
            updatedAttrs.put(CouchbaseAttributes.Cluster.key(), context.getProperty(COUCHBASE_CLUSTER_SERVICE).getValue());
            updatedAttrs.put(CouchbaseAttributes.Bucket.key(), context.getProperty(BUCKET_NAME).getValue());
            updatedAttrs.put(CouchbaseAttributes.DocId.key(), docId);
            updatedAttrs.put(CouchbaseAttributes.Cas.key(), String.valueOf(doc.cas()));
            updatedAttrs.put(CouchbaseAttributes.Expiry.key(), String.valueOf(doc.expiry()));
            flowFile = session.putAllAttributes(flowFile, updatedAttrs);
            session.getProvenanceReporter().send(flowFile, getTransitUrl(context));
            session.transfer(flowFile, REL_SUCCESS);

        } catch (CouchbaseException e) {
            String errMsg = String.format("Writing docuement %s to Couchbase Server using %s failed due to %s", docId, flowFile, e);
            handleCouchbaseException(session, logger, flowFile, e, errMsg);
        }
    }

}
