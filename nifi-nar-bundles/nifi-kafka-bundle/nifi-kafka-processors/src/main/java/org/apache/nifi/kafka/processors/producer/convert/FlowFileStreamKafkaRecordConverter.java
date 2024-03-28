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
package org.apache.nifi.kafka.processors.producer.convert;

import org.apache.nifi.kafka.processors.producer.common.ProducerUtils;
import org.apache.nifi.kafka.processors.producer.header.HeadersFactory;
import org.apache.nifi.kafka.service.api.record.KafkaRecord;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * {@link KafkaRecordConverter} implementation for transforming NiFi FlowFile to {@link KafkaRecord} for publish to
 * Kafka.
 */
public class FlowFileStreamKafkaRecordConverter implements KafkaRecordConverter {
    final int maxMessageSize;
    final HeadersFactory headersFactory;

    public FlowFileStreamKafkaRecordConverter(final int maxMessageSize, final HeadersFactory headersFactory) {
        this.maxMessageSize = maxMessageSize;
        this.headersFactory = headersFactory;
    }

    @Override
    public Iterator<KafkaRecord> convert(
            final Map<String, String> attributes, final InputStream in, final long inputLength) throws IOException {
        ProducerUtils.checkMessageSize(maxMessageSize, inputLength);
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        StreamUtils.copy(in, bos);
        final KafkaRecord kafkaRecord = new KafkaRecord(
                null, null, null, null, bos.toByteArray(), headersFactory.getHeaders(attributes));
        return Collections.singletonList(kafkaRecord).iterator();
    }
}
