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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class StatelessNiFiSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(StatelessNiFiSinkTask.class);

    private StatelessDataflow dataflow;
    private String inputPortName;
    private Set<String> failurePortNames;
    private long timeoutMillis;
    private Pattern headerNameRegex;
    private String headerNamePrefix;
    private QueueSize queueSize;
    private String dataflowName;

    private long backoffMillis = 0L;

    @Override
    public String version() {
        return StatelessKafkaConnectorUtil.getVersion();
    }

    @Override
    public void start(final Map<String, String> properties) {
        logger.info("Starting Sink Task with properties {}", StatelessKafkaConnectorUtil.getLoggableProperties(properties));

        final String timeout = properties.getOrDefault(StatelessKafkaConnectorUtil.DATAFLOW_TIMEOUT, StatelessKafkaConnectorUtil.DEFAULT_DATAFLOW_TIMEOUT);
        timeoutMillis = (long) FormatUtils.getPreciseTimeDuration(timeout, TimeUnit.MILLISECONDS);

        dataflowName = properties.get(StatelessKafkaConnectorUtil.DATAFLOW_NAME);

        final String regex = properties.get(StatelessNiFiSinkConnector.HEADERS_AS_ATTRIBUTES_REGEX);
        headerNameRegex = regex == null ? null : Pattern.compile(regex);
        headerNamePrefix = properties.getOrDefault(StatelessNiFiSinkConnector.HEADER_ATTRIBUTE_NAME_PREFIX, "");

        dataflow = StatelessKafkaConnectorUtil.createDataflow(properties);
        dataflow.initialize();

        // Determine input port name. If input port is explicitly set, use the value given. Otherwise, if only one port exists, use that. Otherwise, throw ConfigException.
        final String dataflowName = properties.get(StatelessKafkaConnectorUtil.DATAFLOW_NAME);
        inputPortName = properties.get(StatelessNiFiSinkConnector.INPUT_PORT_NAME);
        if (inputPortName == null) {
            final Set<String> inputPorts = dataflow.getInputPortNames();
            if (inputPorts.isEmpty()) {
                throw new ConfigException("The dataflow specified for <" + dataflowName + "> does not have an Input Port at the root level. Dataflows used for a Kafka Connect Sink Task "
                    + "must have at least one Input Port at the root level.");
            }

            if (inputPorts.size() > 1) {
                throw new ConfigException("The dataflow specified for <" + dataflowName + "> has multiple Input Ports at the root level (" + inputPorts.toString()
                    + "). The " + StatelessNiFiSinkConnector.INPUT_PORT_NAME + " property must be set to indicate which of these Ports Kafka records should be sent to.");
            }

            inputPortName = inputPorts.iterator().next();
        }

        // Validate the input port
        if (!dataflow.getInputPortNames().contains(inputPortName)) {
            throw new ConfigException("The dataflow specified for <" + dataflowName + "> does not have Input Port with name <" + inputPortName + "> at the root level. Existing Input Port names are "
                + dataflow.getInputPortNames());
        }

        // Determine the failure Ports, if any are given.
        final String failurePortList = properties.get(StatelessNiFiSinkConnector.FAILURE_PORTS);
        if (failurePortList == null || failurePortList.trim().isEmpty()) {
            failurePortNames = Collections.emptySet();
        } else {
            failurePortNames = new HashSet<>();

            final String[] names = failurePortList.split(",");
            for (final String name : names) {
                final String trimmed = name.trim();
                failurePortNames.add(trimmed);
            }
        }

        // Validate the failure ports
        final Set<String> outputPortNames = dataflow.getOutputPortNames();
        for (final String failurePortName : failurePortNames) {
            if (!outputPortNames.contains(failurePortName)) {
                throw new ConfigException("Dataflow was configured with a Failure Port of " + failurePortName
                    + " but there is no Port with that name in the dataflow. Valid Port names are " + outputPortNames);
            }
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        logger.debug("Enqueuing {} Kafka messages", records.size());

        for (final SinkRecord record : records) {
            final Map<String, String> attributes = createAttributes(record);
            final byte[] contents = getContents(record.value());

            queueSize = dataflow.enqueue(contents, attributes, inputPortName);
        }
    }

    private void backoff() {
        // If no backoff period has been set, set it to 1 second. Otherwise, double the amount of time to backoff, up to 10 seconds.
        if (backoffMillis == 0L) {
            backoffMillis = 1000L;
        }

        backoffMillis = Math.min(backoffMillis * 2, 10_000L);
        context.timeout(backoffMillis);
    }

    private void resetBackoff() {
        backoffMillis = 0L;
    }

    private synchronized void triggerDataflow() {
        final long start = System.nanoTime();
        while (dataflow.isFlowFileQueued()) {
            final DataflowTrigger trigger = dataflow.trigger();

            try {
                final Optional<TriggerResult> resultOptional = trigger.getResult(timeoutMillis, TimeUnit.MILLISECONDS);
                if (resultOptional.isPresent()) {
                    final TriggerResult result = resultOptional.get();

                    if (result.isSuccessful()) {
                        // Verify that data was only transferred to the expected Input Port
                        verifyOutputPortContents(trigger, result);

                        // Acknowledge the data so that the session can be committed
                        result.acknowledge();
                        resetBackoff();
                    } else {
                        retry(trigger, "Dataflow " + dataflowName + " failed to execute properly", result.getFailureCause().orElse(null));
                    }
                } else {
                    retry(trigger, "Timed out waiting for dataflow " + dataflowName + " to complete", null);
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                dataflow.purge();
                throw new RuntimeException("Interrupted while waiting for dataflow to complete", e);
            }
        }

        context.requestCommit();

        final long nanos = System.nanoTime() - start;

        if (queueSize != null) {
            logger.debug("Ran dataflow with {} messages ({}) in {} nanos", queueSize.getObjectCount(), FormatUtils.formatDataSize(queueSize.getByteCount()), nanos);
        }
    }

    private void retry(final DataflowTrigger trigger, final String explanation, final Throwable cause) {
        logger.error(explanation, cause);
        trigger.cancel();

        // We don't want to keep running as fast as possible, as doing so may overwhelm a destination system that is already struggling.
        // This is analogous to ProcessContext.yield() in NiFi parlance.
        backoff();

        // We will throw a RetriableException, which will redeliver all messages. So we need to purge anything currently in the dataflow.
        dataflow.purge();

        // Because a background thread may have triggered the dataflow, we need to note that the last trigger was unsuccessful so the subsequent
        // call to either put() or flush() will throw a RetriableException. This will result in the data being redelivered/retried.
        throw new RetriableException(explanation, cause);
    }

    private void verifyOutputPortContents(final DataflowTrigger trigger, final TriggerResult result) {
        for (final String failurePort : failurePortNames) {
            final List<FlowFile> flowFiles = result.getOutputFlowFiles(failurePort);
            if (flowFiles != null && !flowFiles.isEmpty()) {
                logger.error("Dataflow transferred FlowFiles to Port {}, which is configured as a Failure Port. Rolling back session.", failurePort);
                trigger.cancel();
                throw new RetriableException("Data was transferred to Failure Port " + failurePort);
            }
        }
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        super.flush(currentOffsets);

        triggerDataflow();
    }

    private byte[] getContents(final Object value) {
        if (value == null) {
            return new byte[0];
        }
        if (value instanceof String) {
            return ((String) value).getBytes(StandardCharsets.UTF_8);
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }

        throw new IllegalArgumentException("Unsupported message type: the Message value was " + value + " but was expected to be a byte array or a String");
    }

    private Map<String, String> createAttributes(final SinkRecord record) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("kafka.topic", record.topic());
        attributes.put("kafka.offset", String.valueOf(record.kafkaOffset()));
        attributes.put("kafka.partition", String.valueOf(record.kafkaPartition()));
        attributes.put("kafka.timestamp", String.valueOf(record.timestamp()));

        final Object key = record.key();
        if (key instanceof String) {
            attributes.put("kafka.key", (String) key);
        }

        if (headerNameRegex != null) {
            for (final Header header : record.headers()) {
                if (headerNameRegex.matcher(header.key()).matches()) {
                    final String attributeName = headerNamePrefix + header.key();
                    final String attributeValue = String.valueOf(header.value());
                    attributes.put(attributeName, attributeValue);
                }
            }
        }

        return attributes;
    }

    @Override
    public void stop() {
        logger.info("Shutting down Sink Task");
        if (dataflow != null) {
            dataflow.shutdown();
        }
    }
}
