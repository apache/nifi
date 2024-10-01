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
package org.apache.nifi.kafka.processors.common;

import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class KafkaUtils {

    public static Collection<String> toTopicList(final String topics) {
        final List<String> topicList = new ArrayList<>();
        for (final String topic : topics.split(",", 100)) {
            final String trimmedName = topic.trim();
            if (!trimmedName.isEmpty()) {
                topicList.add(trimmedName);
            }
        }
        return topicList;
    }

    public static List<RecordHeader> toHeadersFiltered(final ByteRecord consumerRecord,
                                                       final Pattern headerNamePattern) {
        if (headerNamePattern != null) {
            return consumerRecord.getHeaders().stream()
                    .filter(h -> headerNamePattern.matcher(h.key()).matches())
                    .collect(Collectors.toList());
        } else {
            return consumerRecord.getHeaders();
        }
    }

    public static String toKeyString(final byte[] key, final KeyEncoding keyEncoding) {
        final String keyAttributeValue;
        if (key == null) {
            keyAttributeValue = null;
        } else if (KeyEncoding.HEX.equals(keyEncoding)) {
            keyAttributeValue = HexFormat.of().formatHex(key);
        } else if (KeyEncoding.UTF8.equals(keyEncoding)) {
            keyAttributeValue = new String(key, StandardCharsets.UTF_8);
        } else {
            keyAttributeValue = null;
        }
        return keyAttributeValue;
    }

    public static Map<String, String> toAttributes(final ByteRecord consumerRecord, final KeyEncoding keyEncoding,
                                                   final Pattern headerNamePattern, final Charset headerEncoding,
                                                   final boolean commitOffsets) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(KafkaFlowFileAttribute.KAFKA_TOPIC, consumerRecord.getTopic());
        attributes.put(KafkaFlowFileAttribute.KAFKA_PARTITION, Long.toString(consumerRecord.getPartition()));
        attributes.put(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(consumerRecord.getOffset()));
        attributes.put(KafkaFlowFileAttribute.KAFKA_CONSUMER_OFFSETS_COMMITTED, String.valueOf(commitOffsets));
        consumerRecord.getHeaders().stream()
                .filter(h -> h.key().equals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET)).findFirst()
                .ifPresent(h -> attributes.put(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, new String(h.value(), headerEncoding)));
        consumerRecord.getHeaders().stream()
                .filter(h -> h.key().equals(KafkaFlowFileAttribute.KAFKA_COUNT)).findFirst()
                .ifPresent(h -> attributes.put(KafkaFlowFileAttribute.KAFKA_COUNT, new String(h.value(), headerEncoding)));
        attributes.put(KafkaFlowFileAttribute.KAFKA_TIMESTAMP, Long.toString(consumerRecord.getTimestamp()));
        Optional.ofNullable(toKeyString(consumerRecord.getKey().orElse(null), keyEncoding))
                .ifPresent(keyAttribute -> attributes.put(KafkaFlowFileAttribute.KAFKA_KEY, keyAttribute));

        final List<RecordHeader> headers = consumerRecord.getHeaders();
        attributes.put(KafkaFlowFileAttribute.KAFKA_HEADER_COUNT, Integer.toString(headers.size()));

        if (headerNamePattern != null) {
            for (final RecordHeader header : headers) {
                final String name = header.key();
                if (headerNamePattern.matcher(name).matches()) {
                    final String value = new String(header.value(), headerEncoding);
                    attributes.put(name, value);
                }
            }
        }

        return attributes;
    }
}
