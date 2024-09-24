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
package org.apache.nifi.kafka.processors.consumer.bundle;

import org.apache.nifi.kafka.processors.common.KafkaUtils;
import org.apache.nifi.kafka.service.api.common.TopicPartitionSummary;
import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class ByteRecordBundler {
    private final byte[] demarcator;
    private final boolean separateByKey;
    private final KeyEncoding keyEncoding;
    private final Pattern headerNamePattern;
    private final Charset headerEncoding;
    private final boolean commitOffsets;

    private final Map<BundleKey, BundleValue> bundles;

    public ByteRecordBundler(final byte[] demarcator, final boolean separateByKey, final KeyEncoding keyEncoding,
                             final Pattern headerNamePattern, final Charset headerEncoding, final boolean commitOffsets) {
        this.demarcator = demarcator;
        this.separateByKey = separateByKey;
        this.keyEncoding = keyEncoding;
        this.headerNamePattern = headerNamePattern;
        this.headerEncoding = headerEncoding;
        this.commitOffsets = commitOffsets;
        this.bundles = new HashMap<>();
    }

    public Iterator<ByteRecord> bundle(final Iterator<ByteRecord> consumerRecords) {
        while (consumerRecords.hasNext()) {
            update(bundles, consumerRecords.next());
        }

        return bundles.entrySet().stream()
                .map(e -> toByteRecord(e.getKey(), e.getValue())).iterator();
    }

    private ByteRecord toByteRecord(final BundleKey key, final BundleValue value) {
        final TopicPartitionSummary topicPartition = key.getTopicPartition();
        key.headers.add(new RecordHeader(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, Long.toString(value.getLastOffset()).getBytes(StandardCharsets.UTF_8)));
        key.headers.add(new RecordHeader(KafkaFlowFileAttribute.KAFKA_COUNT, Long.toString(value.getCount()).getBytes(StandardCharsets.UTF_8)));
        return new ByteRecord(topicPartition.getTopic(), topicPartition.getPartition(),
                value.getFirstOffset(), key.getTimestamp(), key.getHeaders(), key.getMessageKey(), value.getData(), value.getCount());
    }

    private void update(final Map<BundleKey, BundleValue> bundles, final ByteRecord byteRecord) {
        final TopicPartitionSummary topicPartition = new TopicPartitionSummary(byteRecord.getTopic(), byteRecord.getPartition());
        final List<RecordHeader> headers = byteRecord.getHeaders();
        final List<RecordHeader> headersFiltered = KafkaUtils.toHeadersFiltered(byteRecord, headerNamePattern);
        final byte[] messageKey = (separateByKey ? byteRecord.getKey().orElse(null) : null);
        final Map<String, String> attributes = KafkaUtils.toAttributes(byteRecord, keyEncoding, headerNamePattern, headerEncoding, commitOffsets);
        final BundleKey bundleKey = new BundleKey(topicPartition, byteRecord.getTimestamp(), headers, headersFiltered, attributes, messageKey);
        if (bundles.containsKey(bundleKey)) {
            update(bundles, byteRecord, bundleKey);
        } else {
            create(bundles, byteRecord, bundleKey);
        }
    }

    private void update(final Map<BundleKey, BundleValue> bundles, final ByteRecord byteRecord, final BundleKey bundleKey) {
        final BundleValue bundleValue = bundles.get(bundleKey);
        bundleValue.update(demarcator, byteRecord.getValue(), byteRecord.getOffset());
    }

    private void create(final Map<BundleKey, BundleValue> bundles, final ByteRecord byteRecord, final BundleKey bundleKey) {
        final BundleValue bundleValue = new BundleValue(byteRecord.getOffset());
        bundleValue.update(demarcator, byteRecord.getValue(), byteRecord.getOffset());
        bundles.put(bundleKey, bundleValue);
    }
}
