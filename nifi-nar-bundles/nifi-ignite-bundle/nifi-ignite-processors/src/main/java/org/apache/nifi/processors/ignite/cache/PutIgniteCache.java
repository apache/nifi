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
package org.apache.nifi.processors.ignite.cache;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.ignite.ClientType;
import org.apache.nifi.stream.io.StreamUtils;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Put Ignite cache processors which pushes the FlowFile content into Ignite Cache using DataStreamer interface.
 */
@EventDriven
@SupportsBatching
@Tags({"Ignite", "insert", "update", "stream", "write", "put", "cache", "key"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Stream the contents of a FlowFile to Ignite Cache using DataStreamer in the case of thick Ignite client " +
        "or puts the contents of a FlowFile to Ignite Cache using thin Ignite client. " +
        "The processor uses the value of FlowFile attribute (Ignite cache entry key) as the " +
        "cache key and the byte array of the FlowFile as the value of the cache entry value. Both the string key and a " +
        "non-empty byte array value are required otherwise the FlowFile is transferred to the failure relation. " +
        "Note - The Ignite Kernel periodically outputs node performance statistics to the logs. This message " +
        "can be turned off by setting the log level for logger 'org.apache.ignite' to WARN in the logback.xml configuration file.")
@WritesAttributes({
        @WritesAttribute(attribute = PutIgniteCache.IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, description = "The total number of FlowFiles in the batch."),
        @WritesAttribute(attribute = PutIgniteCache.IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, description = "The item number of FlowFiles in the batch."),
        @WritesAttribute(attribute = PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, description = "The successful FlowFiles item number."),
        @WritesAttribute(attribute = PutIgniteCache.IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, description = "The number of successful FlowFiles."),
        @WritesAttribute(attribute = PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, description = "The failed FlowFiles item number."),
        @WritesAttribute(attribute = PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, description = "The total number of failed FlowFiles in the batch."),
        @WritesAttribute(attribute = PutIgniteCache.IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, description = "The failed reason attribute key.")
})
@SeeAlso({GetIgniteCache.class})
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutIgniteCache extends AbstractIgniteCacheProcessor {

    /**
     * FlowFile attribute keys and messages.
     */
    static final String IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE = "The FlowFile key attribute was missing.";
    static final String IGNITE_BATCH_FLOW_FILE_FAILED_ZERO_SIZE_MESSAGE = "The FlowFile size was zero.";
    static final String IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT = "ignite.cache.batch.flow.file.total.count";
    static final String IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER = "ignite.cache.batch.flow.file.item.number";
    static final String IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT = "ignite.cache.batch.flow.file.successful.count";
    static final String IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER = "ignite.cache.batch.flow.file.successful.number";
    static final String IGNITE_BATCH_FLOW_FILE_FAILED_COUNT = "ignite.cache.batch.flow.file.failed.count";
    static final String IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER = "ignite.cache.batch.flow.file.failed.number";
    static final String IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY = "ignite.cache.batch.flow.file.failed.reason";

    /**
     * The batch size of FlowFiles to be processed on invocation of onTrigger.
     */
    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .displayName("Batch Size for Entries")
            .name("batch-size-for-entries")
            .description("Batch size for entries (1-500).")
            .defaultValue("250")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(1, 500, true))
            .sensitive(false)
            .build();

    /**
     * DataStreamer per node buffer size.
     */
    static final PropertyDescriptor DATA_STREAMER_PER_NODE_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .displayName("DataStreamer per Node Buffer Size")
            .name("data-streamer-per-node-buffer-size")
            .description("DataStreamer per node buffer size (1-500). Applies only to thick Ignite client.")
            .defaultValue("250")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 500, true))
            .sensitive(false)
            .build();

    /**
     * DataStreamer per node parallelism.
     */
    static final PropertyDescriptor DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS = new PropertyDescriptor.Builder()
            .displayName("DataStreamer per Node Parallel Operations")
            .name("data-streamer-per-node-parallel-operations")
            .description("DataStreamer per node parallelism. Applies only to thick Ignite client.")
            .defaultValue("5")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 10, true))
            .sensitive(false)
            .build();

    /**
     * Override values property.
     */
    static final PropertyDescriptor ALLOW_OVERWRITE = new PropertyDescriptor.Builder()
            .displayName("Allow Overwrite")
            .name("allow-override")
            .description("Whether to overwrite values already in the cache.")
            .defaultValue("false")
            .required(true)
            .allowableValues(new AllowableValue("true"), new AllowableValue("false"))
            .sensitive(false)
            .build();

    /**
     * DataStreamer auto flush frequency.
     */
    private static final PropertyDescriptor DATA_STREAMER_AUTO_FLUSH_FREQUENCY = new PropertyDescriptor.Builder()
            .displayName("DataStreamer Auto Flush Frequency in Millis")
            .name("data-streamer-auto-flush-frequency-in-millis")
            .description("DataStreamer flush interval in milliseconds. Applies only to thick Ignite client.")
            .defaultValue("10")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 100, true))
            .sensitive(false)
            .build();

    /**
     * Property descriptors.
     */
    private static final List<PropertyDescriptor> descriptors =
            Arrays.asList(IGNITE_CLIENT_TYPE, IGNITE_CONFIGURATION_FILE, CACHE_NAME, BATCH_SIZE, IGNITE_CACHE_ENTRY_KEY,
                    DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS, DATA_STREAMER_PER_NODE_BUFFER_SIZE,
                    DATA_STREAMER_AUTO_FLUSH_FREQUENCY, ALLOW_OVERWRITE);

    /**
     * DataStreamer instance.
     */
    private transient IgniteDataStreamer<String, byte[]> igniteDataStreamer;

    /**
     * Get the supported property descriptors.
     *
     * @return The supported property descriptors.
     */
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    /**
     * Get the DataStreamer instance.
     *
     * @return The DataStreamer instance.
     */
    private IgniteDataStreamer<String, byte[]> getIgniteDataStreamer() {
        return igniteDataStreamer;
    }

    /**
     * Initialise the PutIgniteCache processor.
     *
     * @param context Process context.
     * @throws ProcessException If there is a problem while scheduling the processor.
     */
    @OnScheduled
    public final void initializePutIgniteCacheProcessor(final ProcessContext context) throws ProcessException {
        initializeIgniteCache(context);
        final ClientType clientType = ClientType.valueOf(context.getProperty(IGNITE_CLIENT_TYPE).getValue());

        if (clientType.equals(ClientType.THICK)) {
            if (igniteDataStreamer != null) {
                return;
            }
            getLogger().info("Creating thick Ignite client DataStreamer.");
            try {
                final int perNodeParallelOperations = context.getProperty(DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS) != null
                        ? context.getProperty(DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS).asInteger()
                        : Integer.valueOf(DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS.getDefaultValue());
                final int perNodeBufferSize = context.getProperty(DATA_STREAMER_PER_NODE_BUFFER_SIZE) != null
                        ? context.getProperty(DATA_STREAMER_PER_NODE_BUFFER_SIZE).asInteger()
                        : Integer.valueOf(DATA_STREAMER_PER_NODE_BUFFER_SIZE.getDefaultValue());
                final int autoFlushFrequency = context.getProperty(DATA_STREAMER_AUTO_FLUSH_FREQUENCY) != null
                        ? context.getProperty(DATA_STREAMER_AUTO_FLUSH_FREQUENCY).asInteger()
                        : Integer.valueOf(DATA_STREAMER_AUTO_FLUSH_FREQUENCY.getDefaultValue());
                final boolean allowOverwrite = context.getProperty(ALLOW_OVERWRITE).asBoolean();

                igniteDataStreamer = getThickIgniteClient().dataStreamer(getThickIgniteClientCache().getName());
                igniteDataStreamer.perNodeBufferSize(perNodeBufferSize);
                igniteDataStreamer.perNodeParallelOperations(perNodeParallelOperations);
                igniteDataStreamer.autoFlushFrequency(autoFlushFrequency);
                igniteDataStreamer.allowOverwrite(allowOverwrite);

            } catch (final Exception exception) {
                getLogger().error("Failed to schedule PutIgniteCache due to {}.", new Object[]{exception}, exception);
                throw new ProcessException(exception);
            }
        }
    }

    /**
     * Handle FlowFiles.
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final List<FlowFile> flowFiles = session.get(batchSize);

        if (flowFiles.isEmpty()) {
            return;
        }

        final List<Map.Entry<String, byte[]>> cacheItems = new ArrayList<>();
        List<FlowFile> successfulFlowFiles = new ArrayList<>();
        List<FlowFile> failedFlowFiles = new ArrayList<>();
        try {
            for (final FlowFile file : flowFiles) {
                FlowFile flowFile = null;
                try {
                    flowFile = file;

                    final String key = context.getProperty(IGNITE_CACHE_ENTRY_KEY).evaluateAttributeExpressions(flowFile).getValue();

                    if (isFailedFlowFile(flowFile, key)) {
                        failedFlowFiles.add(flowFile);
                        continue;
                    }

                    final byte[] byteArray = new byte[(int) flowFile.getSize()];
                    session.read(flowFile, inputStream -> StreamUtils.fillBuffer(inputStream, byteArray, true));
                    cacheItems.add(new SimpleEntry<>(key, byteArray));
                    successfulFlowFiles.add(flowFile);
                } catch (final Exception exception) {
                    getLogger().error("Failed to insert {} into Ignite cache due to {}.", new Object[]{flowFile, exception}, exception);
                    session.transfer(flowFile, REL_FAILURE);
                    context.yield();
                }
            }
        } finally {
            final ClientType clientType = ClientType.valueOf(context.getProperty(IGNITE_CLIENT_TYPE).getValue());
            if (!cacheItems.isEmpty()) {
                if (clientType.equals(ClientType.THICK)) {
                    final IgniteFuture<?> futures = igniteDataStreamer.addData(cacheItems);
                    final Object result = futures.get();
                    getLogger().debug("Result {} of addData.", new Object[]{result});
                } else if (clientType.equals(ClientType.THIN)) {
                    final boolean allowOverwrite = context.getProperty(ALLOW_OVERWRITE).asBoolean();
                    cacheItems.forEach(entry -> {
                        if (allowOverwrite) {
                            getThinIgniteClientCache().put(entry.getKey(), entry.getValue());
                        } else {
                            getThinIgniteClientCache().putIfAbsent(entry.getKey(), entry.getValue());
                        }
                    });
                }
            }

            if (!successfulFlowFiles.isEmpty()) {
                successfulFlowFiles = updateSuccessfulFlowFileAttributes(flowFiles, successfulFlowFiles, session);
                session.transfer(successfulFlowFiles, REL_SUCCESS);
                final String name = clientType.equals(ClientType.THICK) ? getThickIgniteClientCache().getName() : getThinIgniteClientCache().getName();
                for (final FlowFile flowFile : successfulFlowFiles) {
                    final String key = context.getProperty(IGNITE_CACHE_ENTRY_KEY).evaluateAttributeExpressions(flowFile).getValue();
                    session.getProvenanceReporter().send(flowFile, "ignite://cache/" + name + "/" + key);
                }
            }

            if (!failedFlowFiles.isEmpty()) {
                failedFlowFiles = updateFailedFlowFileAttributes(flowFiles, failedFlowFiles, session, context);
                session.transfer(failedFlowFiles, REL_FAILURE);
            }
        }
    }

    /**
     * Check if FlowFile is corrupted (either FlowFile is empty or does not have a key attribute).
     *
     * @param flowFile The FlowFile to check.
     * @param key      The cache key.
     * @return <code>true</code> If FlowFile is incomplete
     */
    private boolean isFailedFlowFile(final FlowFile flowFile, final String key) {
        if (StringUtils.isEmpty(key)) {
            return true;
        }
        return flowFile.getSize() == 0;
    }

    /**
     * Add successful FlowFile attributes.
     *
     * @param flowFiles           All FlowFiles.
     * @param successfulFlowFiles The list of successful FlowFiles.
     * @param session             The process session.
     * @return The successful FlowFiles with updated attributes.
     */
    private List<FlowFile> updateSuccessfulFlowFileAttributes(final List<FlowFile> flowFiles,
                                                              final List<FlowFile> successfulFlowFiles,
                                                              final ProcessSession session) {

        final int flowFileCount = flowFiles.size();
        final int flowFilesSuccessful = successfulFlowFiles.size();
        final List<FlowFile> updatedSuccessfulFlowFiles = new ArrayList<>();

        FlowFile flowFile;
        final Map<String, String> attributes = new HashMap<>(4);
        for (int i = 0; i < flowFilesSuccessful; i++) {
            flowFile = successfulFlowFiles.get(i);
            attributes.put(IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_ITEM_NUMBER, Integer.toString(i));
            attributes.put(IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, Integer.toString(flowFileCount));
            attributes.put(IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, Integer.toString(flowFiles.indexOf(flowFile)));
            attributes.put(IGNITE_BATCH_FLOW_FILE_SUCCESSFUL_COUNT, Integer.toString(flowFilesSuccessful));
            flowFile = session.putAllAttributes(flowFile, attributes);
            updatedSuccessfulFlowFiles.add(flowFile);
        }

        return updatedSuccessfulFlowFiles;
    }

    /**
     * Add failed FlowFile attributes.
     *
     * @param flowFiles       All FlowFiles.
     * @param failedFlowFiles The list of failed FlowFiles.
     * @param session         The process session.
     * @param context         The process context.
     * @return The failed FlowFiles with updated attributes.
     */
    private List<FlowFile> updateFailedFlowFileAttributes(final List<FlowFile> flowFiles,
                                                          final List<FlowFile> failedFlowFiles,
                                                          final ProcessSession session, ProcessContext context) {

        final int flowFileCount = flowFiles.size();
        final int flowFileFailed = failedFlowFiles.size();
        final List<FlowFile> updatedFailedFlowFiles = new ArrayList<>();

        FlowFile flowFile;
        String key;
        final Map<String, String> attributes = new HashMap<>(5);
        for (int i = 0; i < flowFileFailed; i++) {
            flowFile = failedFlowFiles.get(i);
            attributes.put(IGNITE_BATCH_FLOW_FILE_FAILED_ITEM_NUMBER, Integer.toString(i));
            attributes.put(IGNITE_BATCH_FLOW_FILE_TOTAL_COUNT, Integer.toString(flowFileCount));
            attributes.put(IGNITE_BATCH_FLOW_FILE_ITEM_NUMBER, Integer.toString(flowFiles.indexOf(flowFile)));
            attributes.put(IGNITE_BATCH_FLOW_FILE_FAILED_COUNT, Integer.toString(flowFileFailed));

            key = context.getProperty(IGNITE_CACHE_ENTRY_KEY).evaluateAttributeExpressions(flowFile).getValue();

            if (StringUtils.isEmpty(key)) {
                attributes.put(IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, IGNITE_BATCH_FLOW_FILE_FAILED_MISSING_KEY_MESSAGE);
            } else if (flowFile.getSize() == 0) {
                attributes.put(IGNITE_BATCH_FLOW_FILE_FAILED_REASON_ATTRIBUTE_KEY, IGNITE_BATCH_FLOW_FILE_FAILED_ZERO_SIZE_MESSAGE);
            } else {
                throw new ProcessException("Unknown reason for failing file: " + flowFile);
            }

            flowFile = session.putAllAttributes(flowFile, attributes);
            updatedFailedFlowFiles.add(flowFile);
        }

        return updatedFailedFlowFiles;
    }

    /**
     * Close the DataStreamer instance.
     */
    @OnStopped
    public void closeThickIgniteClientDataStreamer() {
        if (igniteDataStreamer != null) {
            getLogger().info("Closing Ignite DataStreamer.");
            igniteDataStreamer.flush();
            igniteDataStreamer = null;
        }
    }
}
