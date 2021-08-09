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

package org.apache.nifi.kafka.connect;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class StatelessNiFiSourceTask extends SourceTask {
    public static final String STATE_MAP_KEY = "task.index";
    private static final Logger logger = LoggerFactory.getLogger(StatelessNiFiSourceTask.class);

    private StatelessDataflow dataflow;
    private String outputPortName;
    private String topicName;
    private String topicNameAttribute;
    private TriggerResult triggerResult;
    private String keyAttributeName;
    private Pattern headerAttributeNamePattern;
    private long timeoutMillis;
    private String dataflowName;
    private long failureYieldExpiration = 0L;

    private final Map<String, String> clusterStatePartitionMap = Collections.singletonMap(STATE_MAP_KEY, "CLUSTER");
    private Map<String, String> localStatePartitionMap = new HashMap<>();

    private final AtomicLong unacknowledgedRecords = new AtomicLong(0L);

    @Override
    public String version() {
        return StatelessKafkaConnectorUtil.getVersion();
    }

    @Override
    public void start(final Map<String, String> properties) {
        logger.info("Starting Source Task with properties {}", StatelessKafkaConnectorUtil.getLoggableProperties(properties));

        final String timeout = properties.getOrDefault(StatelessKafkaConnectorUtil.DATAFLOW_TIMEOUT, StatelessKafkaConnectorUtil.DEFAULT_DATAFLOW_TIMEOUT);
        timeoutMillis = (long) FormatUtils.getPreciseTimeDuration(timeout, TimeUnit.MILLISECONDS);

        topicName = properties.get(StatelessNiFiSourceConnector.TOPIC_NAME);
        topicNameAttribute = properties.get(StatelessNiFiSourceConnector.TOPIC_NAME_ATTRIBUTE);
        keyAttributeName = properties.get(StatelessNiFiSourceConnector.KEY_ATTRIBUTE);

        if (topicName == null && topicNameAttribute == null) {
            throw new ConfigException("Either the topic.name or topic.name.attribute configuration must be specified");
        }

        final String headerRegex = properties.get(StatelessNiFiSourceConnector.HEADER_REGEX);
        headerAttributeNamePattern = headerRegex == null ? null : Pattern.compile(headerRegex);

        dataflow = StatelessKafkaConnectorUtil.createDataflow(properties);
        dataflow.initialize();

        // Determine the name of the Output Port to retrieve data from
        dataflowName = properties.get(StatelessKafkaConnectorUtil.DATAFLOW_NAME);
        outputPortName = properties.get(StatelessNiFiSourceConnector.OUTPUT_PORT_NAME);
        if (outputPortName == null) {
            final Set<String> outputPorts = dataflow.getOutputPortNames();
            if (outputPorts.isEmpty()) {
                throw new ConfigException("The dataflow specified for <" + dataflowName + "> does not have an Output Port at the root level. Dataflows used for a Kafka Connect Source Task "
                    + "must have at least one Output Port at the root level.");
            }

            if (outputPorts.size() > 1) {
                throw new ConfigException("The dataflow specified for <" + dataflowName + "> has multiple Output Ports at the root level (" + outputPorts.toString()
                    + "). The " + StatelessNiFiSourceConnector.OUTPUT_PORT_NAME + " property must be set to indicate which of these Ports Kafka records should be retrieved from.");
            }

            outputPortName = outputPorts.iterator().next();
        }

        final String taskIndex = properties.get(STATE_MAP_KEY);
        localStatePartitionMap.put(STATE_MAP_KEY, taskIndex);

        final Map<String, String> localStateMap = (Map<String, String>) (Map) context.offsetStorageReader().offset(localStatePartitionMap);
        final Map<String, String> clusterStateMap = (Map<String, String>) (Map) context.offsetStorageReader().offset(clusterStatePartitionMap);

        dataflow.setComponentStates(localStateMap, Scope.LOCAL);
        dataflow.setComponentStates(clusterStateMap, Scope.CLUSTER);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        final long yieldExpiration = Math.max(failureYieldExpiration, dataflow.getSourceYieldExpiration());
        final long now = System.currentTimeMillis();
        final long yieldMillis = yieldExpiration - now;
        if (yieldMillis > 0) {
            // If source component has yielded, we don't want to trigger it again until the yield expiration expires, in order to avoid
            // overloading the source system.
            logger.debug("Source of NiFi flow has opted to yield for {} milliseconds. Will pause dataflow until that time period has elapsed.", yieldMillis);
            Thread.sleep(yieldMillis);
            return null;
        }

        if (unacknowledgedRecords.get() > 0) {
            // If we have records that haven't yet been acknowledged, we want to return null instead of running.
            // We need to wait for the last results to complete before triggering the dataflow again.
            return null;
        }

        logger.debug("Triggering dataflow");
        final long start = System.nanoTime();

        final DataflowTrigger trigger = dataflow.trigger();
        final Optional<TriggerResult> resultOptional = trigger.getResult(timeoutMillis, TimeUnit.MILLISECONDS);
        if (!resultOptional.isPresent()) {
            logger.warn("Dataflow timed out after waiting {} milliseconds. Will cancel the execution.", timeoutMillis);
            trigger.cancel();
            return null;
        }

        triggerResult = resultOptional.get();

        if (!triggerResult.isSuccessful()) {
            logger.error("Dataflow {} failed to execute properly", dataflowName, triggerResult.getFailureCause().orElse(null));
            trigger.cancel();
            failureYieldExpiration = System.currentTimeMillis() + 1000L; // delay next execution for 1 second to avoid constnatly failing and utilization huge amounts of resources
            return null;
        }

        // Verify that data was only transferred to the expected Output Port
        verifyFlowFilesTransferredToProperPort(triggerResult, outputPortName, trigger);

        final long nanos = System.nanoTime() - start;

        final List<FlowFile> outputFlowFiles = triggerResult.getOutputFlowFiles(outputPortName);
        final List<SourceRecord> sourceRecords = new ArrayList<>(outputFlowFiles.size());

        Map<String, ?> componentState = dataflow.getComponentStates(Scope.CLUSTER);
        final Map<String, ?> partitionMap;
        if (componentState == null || componentState.isEmpty()) {
            componentState = dataflow.getComponentStates(Scope.LOCAL);
            partitionMap = localStatePartitionMap;
        } else {
            partitionMap = clusterStatePartitionMap;
        }

        for (final FlowFile flowFile : outputFlowFiles) {
            final byte[] contents = triggerResult.readContent(flowFile);
            final SourceRecord sourceRecord = createSourceRecord(flowFile, contents, componentState, partitionMap);
            sourceRecords.add(sourceRecord);
        }

        logger.debug("Returning {} records from poll() method (took {} nanos to run dataflow)", sourceRecords.size(), nanos);

        // If there is at least one record, we don't want to acknowledge the trigger result until Kafka has committed the Record.
        // This is handled by incrementing the unacknkowledgedRecords count. Then, Kafka Connect will call this.commitRecords().
        // The commitRecords() call will then decrement the number of unacknowledgedRecords, and when all unacknowledged Records have been
        // acknowledged, it will acknowledge the trigger result.
        //
        // However, if there are no records, this.commitRecords() will never be called. As a result, we need toe nsure that we acknowledge the trigger result here.
        if (sourceRecords.size() > 0) {
            unacknowledgedRecords.addAndGet(sourceRecords.size());
        } else {
            triggerResult.acknowledge();
        }

        return sourceRecords;
    }

    private void verifyFlowFilesTransferredToProperPort(final TriggerResult triggerResult, final String expectedPortName, final DataflowTrigger trigger) {
        final Map<String, List<FlowFile>> flowFileOutputMap = triggerResult.getOutputFlowFiles();

        for (final Map.Entry<String, List<FlowFile>> entry : flowFileOutputMap.entrySet()) {
            final String portName = entry.getKey();
            final List<FlowFile> flowFiles = entry.getValue();

            if (!flowFiles.isEmpty() && !expectedPortName.equals(portName)) {
                logger.error("Dataflow transferred FlowFiles to Port {} but was expecting data to be transferred to {}. Rolling back session.", portName, expectedPortName);
                trigger.cancel();
                throw new RetriableException("Data was transferred to unexpected port. Expected: " + expectedPortName + ". Actual: " + portName);
            }
        }
    }


    private SourceRecord createSourceRecord(final FlowFile flowFile, final byte[] contents, final Map<String, ?> componentState, final Map<String, ?> partitionMap) {
        final Schema valueSchema = (contents == null || contents.length == 0) ? null : Schema.BYTES_SCHEMA;

        // Kafka Connect currently gives us no way to determine the number of partitions that a given topic has.
        // Therefore, we have no way to partition based on an attribute or anything like that, unless we left it up to
        // the dataflow developer to know how many partitions exist a priori and explicitly set an attribute in the range of 0..max,
        // but that is not a great solution. Kafka does support using a Simple Message Transform to change the partition of a given
        // record, so that may be the best solution.
        final Integer topicPartition = null;

        final String topic;
        if (topicNameAttribute == null) {
            topic = topicName;
        } else {
            final String attributeValue = flowFile.getAttribute(topicNameAttribute);
            topic = attributeValue == null ? topicName : attributeValue;
        }

        final ConnectHeaders headers = new ConnectHeaders();
        if (headerAttributeNamePattern != null) {
            for (final Map.Entry<String, String> entry : flowFile.getAttributes().entrySet()) {
                if (headerAttributeNamePattern.matcher(entry.getKey()).matches()) {
                    final String headerName = entry.getKey();
                    final String headerValue = entry.getValue();
                    headers.add(headerName, headerValue, Schema.STRING_SCHEMA);
                }
            }
        }

        final Object key = keyAttributeName == null ? null : flowFile.getAttribute(keyAttributeName);
        final Schema keySchema = key == null ? null : Schema.STRING_SCHEMA;
        final Long timestamp = System.currentTimeMillis();

        return new SourceRecord(partitionMap, componentState, topic, topicPartition, keySchema, key, valueSchema, contents, timestamp, headers);
    }

    @Override
    public void commitRecord(final SourceRecord record, final RecordMetadata metadata) throws InterruptedException {
        super.commitRecord(record, metadata);

        final long unacked = unacknowledgedRecords.decrementAndGet();
        logger.debug("SourceRecord {} committed; number of unacknowledged FlowFiles is now {}", record, unacked);

        if (unacked < 1) {
            logger.debug("Acknowledging trigger result");
            triggerResult.acknowledge();
        }
    }

    @Override
    public void stop() {
        logger.info("Shutting down Source Task for " + dataflowName);
        if (dataflow != null) {
            dataflow.shutdown();
        }
    }

    // Available for testing
    protected StatelessDataflow getDataflow() {
        return dataflow;
    }
}
