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
package org.apache.nifi.kafka.processors.consumer.convert;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.kafka.processors.ConsumeKafka;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Continue with Merged Schema strategy: groups by topic, partition, and grouping attributes,
 * merging per-record write schemas and writing once per group.
 */
public class MergeSchemaGrouping implements RecordGroupingStrategy {

    private final RecordSetWriterFactory writerFactory;
    private final ComponentLog logger;
    private final String brokerUri;
    private final boolean commitOffsets;
    private final Map<MergeGroupKey, MergeGroup> mergeGroups = new HashMap<>();

    public MergeSchemaGrouping(
            final RecordSetWriterFactory writerFactory,
            final ComponentLog logger,
            final String brokerUri,
            final boolean commitOffsets) {
        this.writerFactory = writerFactory;
        this.logger = logger;
        this.brokerUri = brokerUri;
        this.commitOffsets = commitOffsets;
    }

    @Override
    public void addRecord(
            final ProcessSession session,
            final ByteRecord consumerRecord,
            final Record recordToWrite,
            final RecordSchema writeSchema,
            final Map<String, String> attributes,
            final Map<String, String> groupingAttributes) {
        final MergeGroupKey key = new MergeGroupKey(groupingAttributes, consumerRecord.getTopic(), consumerRecord.getPartition());
        final MergeGroup group = mergeGroups.computeIfAbsent(key, ignored -> new MergeGroup(attributes));
        group.add(recordToWrite, writeSchema, consumerRecord);
    }

    @Override
    public void finishAllGroups(final ProcessSession session) {
        for (final Map.Entry<MergeGroupKey, MergeGroup> entry : mergeGroups.entrySet()) {
            final MergeGroupKey key = entry.getKey();
            final MergeGroup group = entry.getValue();

            FlowFile flowFile = session.create();

            final RecordSchema schemaToWrite;
            try {
                schemaToWrite = writerFactory.getSchema(group.attributes, group.mergedWriteSchema);
            } catch (final SchemaNotFoundException | IOException e) {
                throw new ProcessException("Failed to determine write schema for Kafka records", e);
            }

            final Map<String, String> flowFileAttributes = new HashMap<>();
            final int[] recordCountHolder = new int[1];
            flowFile = session.write(flowFile, out -> {
                try (final RecordSetWriter writer = writerFactory.createWriter(logger, schemaToWrite, out, group.attributes)) {
                    writer.beginRecordSet();
                    for (final Record record : group.records) {
                        writer.write(record);
                    }
                    final WriteResult writeResult = writer.finishRecordSet();
                    recordCountHolder[0] = writeResult.getRecordCount();

                    flowFileAttributes.putAll(writeResult.getAttributes());
                    flowFileAttributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                } catch (final SchemaNotFoundException e) {
                    throw new ProcessException("Failed to write Kafka records to FlowFile", e);
                }
            });

            final int recordCount = recordCountHolder[0];

            flowFileAttributes.put("record.count", String.valueOf(recordCount));
            flowFileAttributes.put(KafkaFlowFileAttribute.KAFKA_COUNT, String.valueOf(recordCount));

            flowFileAttributes.put(KafkaFlowFileAttribute.KAFKA_TOPIC, key.topic());
            flowFileAttributes.put(KafkaFlowFileAttribute.KAFKA_PARTITION, String.valueOf(key.partition()));
            flowFileAttributes.put(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, Long.toString(group.maxOffset));
            flowFileAttributes.put(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(group.minOffset));
            flowFileAttributes.put(KafkaFlowFileAttribute.KAFKA_TIMESTAMP, Long.toString(group.minTimestamp));
            flowFileAttributes.putAll(key.groupingAttributes());
            flowFileAttributes.put(KafkaFlowFileAttribute.KAFKA_CONSUMER_OFFSETS_COMMITTED, String.valueOf(commitOffsets));

            flowFile = session.putAllAttributes(flowFile, flowFileAttributes);

            session.getProvenanceReporter().receive(flowFile, brokerUri + "/" + key.topic());
            session.adjustCounter("Records Received from " + key.topic(), recordCount, false);
            session.transfer(flowFile, ConsumeKafka.SUCCESS);
        }
        mergeGroups.clear();
    }

    private record MergeGroupKey(Map<String, String> groupingAttributes, String topic, int partition) {
    }

    private static final class MergeGroup {
        final Map<String, String> attributes;
        final List<Record> records = new ArrayList<>();
        RecordSchema mergedWriteSchema;
        long maxOffset = Long.MIN_VALUE;
        long minOffset = Long.MAX_VALUE;
        long minTimestamp = Long.MAX_VALUE;

        MergeGroup(final Map<String, String> attributes) {
            this.attributes = attributes;
        }

        void add(final Record recordToWrite, final RecordSchema writeSchema, final ByteRecord consumerRecord) {
            records.add(recordToWrite);
            mergedWriteSchema = DataTypeUtils.merge(mergedWriteSchema, writeSchema);
            maxOffset = Math.max(maxOffset, consumerRecord.getOffset());
            minOffset = Math.min(minOffset, consumerRecord.getOffset());
            minTimestamp = Math.min(minTimestamp, consumerRecord.getTimestamp());
        }
    }
}
