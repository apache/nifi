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

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.kafka.processors.ConsumeKafka;
import org.apache.nifi.kafka.processors.common.KafkaUtils;
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;

import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaReferenceReader;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.*;

import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.avro.Schema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class AvroFileStreamKafkaMessageConverter implements KafkaMessageConverter {
    private final Charset headerEncoding;
    private final Pattern headerNamePattern;
    private final KeyEncoding keyEncoding;

    private final SchemaRegistry schemaRegistry;
    private final SchemaReferenceReader schemaReferenceReader;
    private final CodecFactory avroCodec;
    private final boolean commitOffsets;
    private final OffsetTracker offsetTracker;
    private final String brokerUri;

    public AvroFileStreamKafkaMessageConverter(
            final Charset headerEncoding,
            final Pattern headerNamePattern,
            final KeyEncoding keyEncoding,

            final SchemaRegistry schemaRegistry,
            final SchemaReferenceReader schemaReferenceReader,
            final CodecFactory avroCodec,
            final boolean commitOffsets,
            final OffsetTracker offsetTracker,
            final String brokerUri) {
        this.headerEncoding = headerEncoding;
        this.headerNamePattern = headerNamePattern;
        this.keyEncoding = keyEncoding;

        this.schemaRegistry = schemaRegistry;
        this.schemaReferenceReader = schemaReferenceReader;
        this.avroCodec = avroCodec;
        this.commitOffsets = commitOffsets;
        this.offsetTracker = offsetTracker;
        this.brokerUri = brokerUri;
    }

    @Override
    public void toFlowFiles(final ProcessSession session, final Iterator<ByteRecord> consumerRecords) {
        try {
            final Map<RecordGroupCriteria, RecordGroup> recordGroups = new HashMap<>();

            String topic = null;
            int partition = 0;
            final Map<String, String> no_variables = Map.of();

            while (consumerRecords.hasNext()) {
                final ByteRecord consumerRecord = consumerRecords.next();
                if (topic == null) {
                    partition = consumerRecord.getPartition();
                    topic = consumerRecord.getTopic();
                }

                final byte[] value = consumerRecord.getValue();
                final Map<String, String> attributes = KafkaUtils.toAttributes(
                        consumerRecord, keyEncoding, headerNamePattern, headerEncoding, commitOffsets);

                try (final InputStream in = new ByteArrayInputStream(value)) {


                        final SchemaIdentifier id = this.schemaReferenceReader.getSchemaIdentifier(no_variables, in);
                        final Map<String, String> groupAttributes = Map.of(
                                KafkaFlowFileAttribute.KAFKA_TOPIC, consumerRecord.getTopic(),
                                KafkaFlowFileAttribute.KAFKA_PARTITION, Long.toString(consumerRecord.getPartition())
                        );

                        RecordGroupCriteria ids = new RecordGroupCriteria(id, groupAttributes);
                        RecordGroup recordGroup = recordGroups.get(ids);

                        if (recordGroup == null) {
                            FlowFile flowFile = session.create();

                            flowFile = session.putAllAttributes(flowFile, groupAttributes);

                            final OutputStream rawOut = session.write(flowFile);
                            RecordSchema recordSchema = schemaRegistry.retrieveSchema(id);
                            Schema schema = AvroTypeUtil.extractAvroSchema(recordSchema);

                            final GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
                            final DataFileWriter<GenericRecord> dataFileWriter;
                            dataFileWriter = new DataFileWriter<>(datumWriter);
                            dataFileWriter.setCodec(this.avroCodec);
                            try {
                                dataFileWriter.create(schema, rawOut);
                            } catch (IOException e) {
                                throw new RuntimeException("IO Exception while preparing Avro output FlowFile for Kafka records",e);
                            }
                            recordGroup = new RecordGroup(flowFile, dataFileWriter, topic, partition, new AtomicInteger(0));
                            recordGroups.put(ids, recordGroup);

                        }

                        byte[] remainingBytes = new byte[in.available()];
                        if (in.read(remainingBytes) > 0) {
                            recordGroup.writer.appendEncoded(java.nio.ByteBuffer.wrap(remainingBytes));
                            recordGroup.recordCount.incrementAndGet();
                        }




                } catch (SchemaNotFoundException e) {
                    // Failed to parse the record. Transfer to a 'parse.failure' relationship
                    FlowFile flowFile = session.create();
                    flowFile = session.putAllAttributes(flowFile, attributes);
                    flowFile = session.write(flowFile, out -> out.write(value));
                    session.transfer(flowFile, ConsumeKafka.PARSE_FAILURE);
                    session.adjustCounter("Records Received from " + consumerRecord.getTopic(), 1, false);

                    // Track the offsets for the Kafka Record
                    offsetTracker.update(consumerRecord);

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }

            // Finish writing the records
            for (final Map.Entry<RecordGroupCriteria, RecordGroup> entry : recordGroups.entrySet()) {
                final RecordGroup recordGroup = entry.getValue();
                final RecordGroupCriteria ids = entry.getKey();
                final Map<String, String> attributes;
                final int recordCount = recordGroup.recordCount.get();
                try (final DataFileWriter<GenericRecord> writer = recordGroup.writer()) {
                    writer.flush();
                    attributes = new HashMap<>(ids.headers());
                    attributes.put("record.count", String.valueOf(recordCount));
                    attributes.put("avro.codec", avroCodec.toString());
                    attributes.put(CoreAttributes.MIME_TYPE.key(), "avro/binary");
                    attributes.put(KafkaFlowFileAttribute.KAFKA_CONSUMER_OFFSETS_COMMITTED, String.valueOf(commitOffsets));

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                FlowFile flowFile = recordGroup.flowFile();
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.getProvenanceReporter().receive(flowFile,brokerUri + "/" + topic);
                session.adjustCounter("Records received from " + topic, recordCount, false);
                session.transfer(flowFile, ConsumeKafka.SUCCESS);
            }

        } catch (Exception e) {
            throw new RuntimeException("Error while interpreting Kafka content as Confluent Avro",e);
        }
    }

        private record RecordGroupCriteria(SchemaIdentifier schema, Map<String, String> headers) {
        }

        private record RecordGroup(FlowFile flowFile, DataFileWriter<GenericRecord> writer, String topic, int partition, AtomicInteger recordCount) {

        }
    }