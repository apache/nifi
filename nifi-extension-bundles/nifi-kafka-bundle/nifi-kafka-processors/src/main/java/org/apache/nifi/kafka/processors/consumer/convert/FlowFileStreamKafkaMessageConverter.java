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
import org.apache.nifi.kafka.processors.ConsumeKafka;
import org.apache.nifi.kafka.processors.common.KafkaUtils;
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.provenance.ProvenanceReporter;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

public class FlowFileStreamKafkaMessageConverter implements KafkaMessageConverter {
    private final Charset headerEncoding;
    private final Pattern headerNamePattern;
    private final KeyEncoding keyEncoding;
    private final boolean commitOffsets;
    private final OffsetTracker offsetTracker;
    private final Runnable onSuccess;

    public FlowFileStreamKafkaMessageConverter(
            final Charset headerEncoding,
            final Pattern headerNamePattern,
            final KeyEncoding keyEncoding,
            final boolean commitOffsets,
            final OffsetTracker offsetTracker,
            final Runnable onSuccess) {
        this.headerEncoding = headerEncoding;
        this.headerNamePattern = headerNamePattern;
        this.keyEncoding = keyEncoding;
        this.commitOffsets = commitOffsets;
        this.offsetTracker = offsetTracker;
        this.onSuccess = onSuccess;
    }

    @Override
    public void toFlowFiles(final ProcessSession session, final Iterator<ByteRecord> consumerRecords) {
        while (consumerRecords.hasNext()) {
            final ByteRecord consumerRecord = consumerRecords.next();

            final byte[] value = consumerRecord.getValue();
            FlowFile flowFile = session.create();

            if (consumerRecord.getValue().length > 0) {
                flowFile = session.write(flowFile, outputStream -> outputStream.write(value));
            } else {
                session.putAttribute(flowFile, KafkaFlowFileAttribute.KAFKA_TOMBSTONE, Boolean.TRUE.toString());
            }

            final Map<String, String> attributes = KafkaUtils.toAttributes(
                    consumerRecord, keyEncoding, headerNamePattern, headerEncoding, commitOffsets);
            flowFile = session.putAllAttributes(flowFile, attributes);

            final ProvenanceReporter provenanceReporter = session.getProvenanceReporter();
            final String transitUri = String.format(TRANSIT_URI_FORMAT, consumerRecord.getTopic(), consumerRecord.getPartition());
            provenanceReporter.receive(flowFile, transitUri);

            session.adjustCounter("Records Received from " + consumerRecord.getTopic(), consumerRecord.getBundledCount(), false);

            session.transfer(flowFile, ConsumeKafka.SUCCESS);
            offsetTracker.update(consumerRecord);
        }

        onSuccess.run();
    }
}
