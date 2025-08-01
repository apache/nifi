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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.UpsertOptions;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.couchbase.DocumentType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;

import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COUCHBASE_CLUSTER_SERVICE;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.DOCUMENT_TYPE;

@Tags({"nosql", "couchbase", "database", "put"})
@CapabilityDescription("Put a document to Couchbase Server via Key/Value access.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@ReadsAttributes({
    @ReadsAttribute(attribute = "uuid", description = "Used as a document id if 'Document Id' is not specified"),
})
@WritesAttributes({
    @WritesAttribute(attribute = "couchbase.cluster", description = "Cluster where the document was stored."),
    @WritesAttribute(attribute = "couchbase.bucket", description = "Bucket where the document was stored."),
    @WritesAttribute(attribute = "couchbase.scope", description = "Scope where the document was stored."),
    @WritesAttribute(attribute = "couchbase.collection", description = "Collection where the document was stored."),
    @WritesAttribute(attribute = "couchbase.doc.id", description = "Id of the document."),
    @WritesAttribute(attribute = "couchbase.doc.cas", description = "CAS of the document."),
    @WritesAttribute(attribute = "couchbase.exception", description = "If Couchbase related error occurs the CouchbaseException class name will be captured here.")
})
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutCouchbaseKey extends AbstractCouchbaseProcessor {


    public static final PropertyDescriptor PERSIST_TO = new PropertyDescriptor.Builder()
        .name("persist-to")
        .displayName("Persist To")
        .description("Durability constraint about disk persistence.")
        .required(true)
        .allowableValues(PersistTo.values())
        .defaultValue(PersistTo.NONE.toString())
        .build();

    public static final PropertyDescriptor REPLICATE_TO = new PropertyDescriptor.Builder()
        .name("replicate-to")
        .displayName("Replicate To")
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
        relationships.add(new Relationship.Builder().name(REL_SUCCESS.getName())
                .description("All FlowFiles that are written to Couchbase Server are routed to this relationship.").build());
        relationships.add(new Relationship.Builder().name(REL_RETRY.getName())
                .description("All FlowFiles failed to be written to Couchbase Server but can be retried are routed to this relationship.").build());
        relationships.add(new Relationship.Builder().name(REL_FAILURE.getName())
                .description("All FlowFiles failed to be written to Couchbase Server and not retry-able are routed to this relationship.").build());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final byte[] content = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, content, true);
            }
        });

        String docId = flowFile.getAttribute(CoreAttributes.UUID.key());
        if (context.getProperty(DOC_ID).isSet()) {
            docId = context.getProperty(DOC_ID).evaluateAttributeExpressions(flowFile).getValue();
        }

        try {
            final DocumentType documentType = DocumentType.valueOf(context.getProperty(DOCUMENT_TYPE).getValue());
            final PersistTo persistTo = PersistTo.valueOf(context.getProperty(PERSIST_TO).getValue());
            final ReplicateTo replicateTo = ReplicateTo.valueOf(context.getProperty(REPLICATE_TO).getValue());
            final Collection collection = openCollection(context);
            UpsertOptions uo = UpsertOptions.upsertOptions()
                    .durability(persistTo, replicateTo)
                    .transcoder(documentType == DocumentType.Json ? RawJsonTranscoder.INSTANCE : RawBinaryTranscoder.INSTANCE);

            MutationResult mr = collection.upsert(docId, content, uo);

            final Map<String, String> updatedAttrs = new HashMap<>();
            updatedAttrs.put(CouchbaseAttributes.Cluster.key(), context.getProperty(COUCHBASE_CLUSTER_SERVICE).getValue());
            updatedAttrs.put(CouchbaseAttributes.Bucket.key(), collection.bucketName());
            updatedAttrs.put(CouchbaseAttributes.Scope.key(), collection.scopeName());
            updatedAttrs.put(CouchbaseAttributes.Collection.key(), collection.name());
            updatedAttrs.put(CouchbaseAttributes.DocId.key(), docId);
            if (mr != null) {
                updatedAttrs.put(CouchbaseAttributes.Cas.key(), String.valueOf(mr.cas()));
            }


            flowFile = session.putAllAttributes(flowFile, updatedAttrs);
            session.getProvenanceReporter().send(flowFile, getTransitUrl(collection, docId));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final CouchbaseException e) {
            String errMsg = String.format("Writing document %s to Couchbase Server using %s failed due to %s", docId, flowFile, e);
            handleCouchbaseException(context, session, logger, flowFile, e, errMsg);
        }
    }

}
