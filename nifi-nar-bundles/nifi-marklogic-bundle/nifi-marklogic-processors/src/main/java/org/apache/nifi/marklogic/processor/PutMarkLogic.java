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
package org.apache.nifi.marklogic.processor;

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.datamovement.impl.WriteEventImpl;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * The TriggerWhenEmpty annotation is used so that this processor has a chance to flush the WriteBatcher when no
 * flowfiles are ready to be received.
 */
@Tags({"MarkLogic", "Put", "Write", "Insert"})
@CapabilityDescription("Write batches of FlowFiles as documents to a MarkLogic server using the " +
    "MarkLogic Data Movement SDK (DMSDK)")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@DynamicProperty(name = "Server transform parameter name", value = "Value of the server transform parameter",
    description = "Adds server transform parameters to be passed to the server transform specified. "
    + "Server transform parameter name should start with the string 'trans:'.")
@TriggerWhenEmpty
public class PutMarkLogic extends AbstractMarkLogicProcessor {

    class FlowFileInfo {
        FlowFile flowFile;
        ProcessSession session;
        FlowFileInfo(FlowFile flowFile, ProcessSession session) {
            this.flowFile = flowFile;
            this.session = session;
        }
    }
    private Map<String, FlowFileInfo> uriFlowFileMap = new HashMap<>();
    public static final PropertyDescriptor COLLECTIONS = new PropertyDescriptor.Builder()
        .name("Collections")
        .displayName("Collections")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .description("Comma-delimited sequence of collections to add to each document")
        .required(false)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor FORMAT = new PropertyDescriptor.Builder()
        .name("Format")
        .displayName("Format")
        .description("Format for each document; if not specified, MarkLogic will determine the format" +
            " based on the URI")
        .allowableValues(Format.JSON.name(), Format.XML.name(), Format.TEXT.name(), Format.BINARY.name(), Format.UNKNOWN.name())
        .required(false)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor JOB_ID = new PropertyDescriptor.Builder()
        .name("Job ID")
        .displayName("Job ID")
        .description("ID for the WriteBatcher job")
        .required(false)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor JOB_NAME = new PropertyDescriptor.Builder()
        .name("Job Name")
        .displayName("Job Name")
        .description("Name for the WriteBatcher job")
        .required(false)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor MIMETYPE = new PropertyDescriptor.Builder()
        .name("MIME type")
        .displayName("MIME type")
        .description("MIME type for each document; if not specified, MarkLogic will determine the " +
            "MIME type based on the URI")
        .addValidator(Validator.VALID)
        .required(false)
        .build();

    public static final PropertyDescriptor PERMISSIONS = new PropertyDescriptor.Builder()
        .name("Permissions")
        .displayName("Permissions")
        .defaultValue("rest-reader,read,rest-writer,update")
        .description("Comma-delimited sequence of permissions - role1, capability1, role2, " +
            "capability2 - to add to each document")
        .addValidator(Validator.VALID)
        .required(false)
        .build();

    public static final PropertyDescriptor TEMPORAL_COLLECTION = new PropertyDescriptor.Builder()
        .name("Temporal collection")
        .displayName("Temporal collection")
        .description("The temporal collection to use for a temporal document insert")
        .addValidator(Validator.VALID)
        .required(false)
        .build();

    public static final PropertyDescriptor TRANSFORM = new PropertyDescriptor.Builder()
        .name("Server transform")
        .displayName("Server transform")
        .description("The name of REST server transform to apply to every document as it's" +
            " written")
        .addValidator(Validator.VALID)
        .required(false)
        .build();

    public static final PropertyDescriptor URI_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
        .name("URI attribute name")
        .displayName("URI attribute name")
        .defaultValue("uuid")
        .required(true)
        .description("The name of the FlowFile attribute whose value will be used as the URI")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    public static final PropertyDescriptor URI_PREFIX = new PropertyDescriptor.Builder()
        .name("URI prefix")
        .displayName("URI prefix")
        .description("The prefix to prepend to each URI")
        .required(false)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor URI_SUFFIX = new PropertyDescriptor.Builder()
        .name("URI suffix")
        .displayName("URI suffix")
        .description("The suffix to append to each URI")
        .required(false)
        .addValidator(Validator.VALID)
        .build();

    protected static final Relationship SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles that are successfully written to MarkLogic are routed to the " +
            "success relationship for future processing.")
        .build();

    protected static final Relationship FAILURE = new Relationship.Builder()
        .name("failure")
        .description("All FlowFiles that failed to be written to MarkLogic are routed to the " +
            "failure relationship for future processing.")
        .build();

    private volatile DataMovementManager dataMovementManager;
    private volatile WriteBatcher writeBatcher;
    // If no FlowFile exists when this processor is triggered, this variable determines whether or not a call is made to
    // flush the WriteBatcher
    private volatile boolean shouldFlushIfEmpty = true;

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .addValidator(Validator.VALID)
            .dynamic(true)
            .required(false)
            .build();
    }

    @Override
    public void init(ProcessorInitializationContext context) {
        super.init(context);

        List<PropertyDescriptor> list = new ArrayList<>();
        list.addAll(properties);
        list.add(COLLECTIONS);
        list.add(FORMAT);
        list.add(JOB_ID);
        list.add(JOB_NAME);
        list.add(MIMETYPE);
        list.add(PERMISSIONS);
        list.add(TRANSFORM);
        list.add(TEMPORAL_COLLECTION);
        list.add(URI_ATTRIBUTE_NAME);
        list.add(URI_PREFIX);
        list.add(URI_SUFFIX);
        properties = Collections.unmodifiableList(list);
        Set<Relationship> set = new HashSet<>();
        set.add(SUCCESS);
        set.add(FAILURE);
        relationships = Collections.unmodifiableSet(set);
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        dataMovementManager = getDatabaseClient(context).newDataMovementManager();
        writeBatcher = dataMovementManager.newWriteBatcher()
            .withJobId(context.getProperty(JOB_ID).getValue())
            .withJobName(context.getProperty(JOB_NAME).getValue())
            .withBatchSize(context.getProperty(BATCH_SIZE).asInteger())
            .withTemporalCollection(context.getProperty(TEMPORAL_COLLECTION).getValue());

        final String transform = context.getProperty(TRANSFORM).getValue();
        if (transform != null) {
            ServerTransform serverTransform = new ServerTransform(transform);
            final String transformPrefix = "trans:";
            for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
                if (!descriptor.isDynamic() && !descriptor.getName().startsWith(transformPrefix)) continue;
                serverTransform.addParameter(descriptor.getName().substring(transformPrefix.length()), context.getProperty(descriptor).getValue());
            }
            writeBatcher.withTransform(serverTransform);
        }
        Integer threadCount = context.getProperty(THREAD_COUNT).asInteger();
        if(threadCount != null) {
            writeBatcher.withThreadCount(threadCount);
        }
        this.writeBatcher.onBatchSuccess(writeBatch -> {
            for(WriteEvent writeEvent : writeBatch.getItems()) {
                routeDocumentToRelationship(writeEvent, SUCCESS);
            }
        }).onBatchFailure((writeBatch, throwable) -> {
            for(WriteEvent writeEvent : writeBatch.getItems()) {
                routeDocumentToRelationship(writeEvent, FAILURE);
            }
        });
        dataMovementManager.startJob(writeBatcher);
    }

    private void routeDocumentToRelationship(WriteEvent writeEvent, Relationship relationship) {
        DocumentMetadataHandle metadata = (DocumentMetadataHandle) writeEvent.getMetadata();
        String flowFileUUID = metadata.getMetadataValues().get("flowFileUUID");
        FlowFileInfo flowFile = uriFlowFileMap.get(flowFileUUID);
        if(flowFile != null) {
            flowFile.session.getProvenanceReporter().send(flowFile.flowFile, writeEvent.getTargetUri());
            flowFile.session.transfer(flowFile.flowFile, relationship);
            flowFile.session.commit();
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Routing " + writeEvent.getTargetUri() + " to " + relationship.getName());
            }
        }
        uriFlowFileMap.remove(flowFileUUID);
    }

    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        try {
            onTrigger(context, session);
        } catch (final Throwable t) {
            getLogger().error("{} failed to process due to {}; rolling back session", new Object[]{this, t});
            session.rollback(true);
            throw new ProcessException(t);
        }
    }
    /**
     * When a FlowFile is received, hand it off to the WriteBatcher so it can be written to MarkLogic.
     * <p>
     * If a FlowFile is not set (possible because of the TriggerWhenEmpty annotation), then yield is called on the
     * ProcessContext so that Nifi doesn't invoke this method repeatedly when nothing is available. Then, a check is
     * made to determine if flushAsync should be called on the WriteBatcher. This ensures that any batch of documents
     * that is smaller than the WriteBatcher's batch size will be flushed immediately and not have to wait for more
     * FlowFiles to arrive to fill out the batch.
     *
     */
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            context.yield();
            if (shouldFlushIfEmpty) {
                flushWriteBatcherAsync(this.writeBatcher);
            }
            shouldFlushIfEmpty = false;
        } else {
            shouldFlushIfEmpty = true;

            WriteEvent writeEvent = buildWriteEvent(context, session, flowFile);
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Writing URI: " + writeEvent.getTargetUri());
            }
            addWriteEvent(this.writeBatcher, writeEvent);
        }
    }

    /*
     * Protected so that it can be overridden for unit testing purposes.
     */
    protected void flushWriteBatcherAsync(WriteBatcher writeBatcher) {
        writeBatcher.flushAsync();
    }

    /*
     * Protected so that it can be overridden for unit testing purposes.
     */
    protected void addWriteEvent(WriteBatcher writeBatcher, WriteEvent writeEvent) {
        writeBatcher.add(writeEvent);
    }

    protected WriteEvent buildWriteEvent(ProcessContext context, ProcessSession session, FlowFile flowFile) {
        String uri = flowFile.getAttribute(context.getProperty(URI_ATTRIBUTE_NAME).getValue());
        final String prefix = context.getProperty(URI_PREFIX).getValue();
        if (prefix != null) {
            uri = prefix + uri;
        }
        final String suffix = context.getProperty(URI_SUFFIX).getValue();
        if (suffix != null) {
            uri += suffix;
        }

        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        final PropertyValue collectionProperty = context.getProperty(COLLECTIONS);
        final String collections = collectionProperty != null ? collectionProperty.isExpressionLanguagePresent()
            ? collectionProperty.evaluateAttributeExpressions(flowFile).getValue() : collectionProperty.getValue() : null;
        if (collections != null) {
            metadata.withCollections(collections.split(","));
        }
        final String permissions = context.getProperty(PERMISSIONS).getValue();
        if (permissions != null) {
            String[] tokens = permissions.split(",");
            for (int i = 0; i < tokens.length; i += 2) {
                String role = tokens[i];
                String capability = tokens[i + 1];
                metadata.withPermission(role, DocumentMetadataHandle.Capability.getValueOf(capability));
            }
        }
        // Add the flow file UUID for Provenance purposes and for sending them
        // to the appropriate relationship
        String flowFileUUID = flowFile.getAttribute(CoreAttributes.UUID.key());
        metadata.withMetadataValue("flowFileUUID", flowFileUUID);
        final byte[] content = new byte[(int) flowFile.getSize()];
        session.read(flowFile, inputStream -> StreamUtils.fillBuffer(inputStream, content));

        BytesHandle handle = new BytesHandle(content);

        final String format = context.getProperty(FORMAT).getValue();
        if (format != null) {
            handle.withFormat(Format.valueOf(format));
        } else {
            addFormat(uri, handle);
        }

        final String mimetype = context.getProperty(MIMETYPE).getValue();
        if (mimetype != null) {
            handle.withMimetype(mimetype);
        }

        uriFlowFileMap.put(flowFileUUID, new FlowFileInfo(flowFile, session));
        return new WriteEventImpl()
            .withTargetUri(uri)
            .withMetadata(metadata)
            .withContent(handle);
    }

    protected void addFormat(String uri, BytesHandle handle) {
        int extensionStartIndex = uri.lastIndexOf(".");
        if(extensionStartIndex > 0) {
            String extension = uri.substring(extensionStartIndex + 1).toLowerCase();
            switch ( extension ) {
                case "xml" :
                    handle.withFormat(Format.XML);
                    break;
                case "json" :
                    handle.withFormat(Format.JSON);
                    break;
                case "txt" :
                    handle.withFormat(Format.TEXT);
                    break;
                default:
                    handle.withFormat(Format.UNKNOWN);
                    break;
            }
        }
    }

    @OnStopped
    public void completeWriteBatcherJob() {
        if (writeBatcher != null) {
            getLogger().info("Calling flushAndWait on WriteBatcher");
            writeBatcher.flushAndWait();

            getLogger().info("Stopping WriteBatcher job");
            dataMovementManager.stopJob(writeBatcher);
        }
        writeBatcher = null;
        dataMovementManager = null;
    }

}
