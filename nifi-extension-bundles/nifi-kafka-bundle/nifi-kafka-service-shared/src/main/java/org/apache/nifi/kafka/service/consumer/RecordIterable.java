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

package org.apache.nifi.kafka.service.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.kafka.service.api.record.ByteRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RecordIterable implements Iterable<ByteRecord> {
    private final Iterator<ByteRecord> records;

    public RecordIterable(final Iterable<ConsumerRecord<byte[], byte[]>> consumerRecords) {
        this.records = new RecordIterator(consumerRecords);
    }

    @Override
    public Iterator<ByteRecord> iterator() {
        return records;
    }

    private static class RecordIterator implements Iterator<ByteRecord> {
        private final Iterator<ConsumerRecord<byte[], byte[]>> consumerRecords;

        private RecordIterator(final Iterable<ConsumerRecord<byte[], byte[]>> records) {
            this.consumerRecords = records.iterator();
        }

        @Override
        public boolean hasNext() {
            return consumerRecords.hasNext();
        }

        @Override
        public ByteRecord next() {
            final ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecords.next();
            final List<RecordHeader> recordHeaders = new ArrayList<>();
            consumerRecord.headers().forEach(header -> {
                final RecordHeader recordHeader = new RecordHeader(header.key(), header.value());
                recordHeaders.add(recordHeader);
            });

            // Support Kafka tombstones
            byte[] value = consumerRecord.value();
            if (value == null) {
                value = new byte[0];
            }

            return new ByteRecord(
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.timestamp(),
                recordHeaders,
                consumerRecord.key(),
                value,
                1
            );
        }
    }
}