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
package org.apache.nifi.processors.aws.kinesis;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Deaggregates KPL (Kinesis Producer Library) aggregated records into individual user records.
 *
 * <p>KPL aggregation packs multiple user records into a single Kinesis record using a protobuf
 * envelope with a 4-byte magic header and a 16-byte MD5 trailer. Non-aggregated records
 * pass through unchanged as a single {@link DeaggregatedRecord} with {@code subSequenceNumber=0}.
 *
 * <p>If a record has the magic header but fails MD5 verification or protobuf parsing, it falls
 * back to passthrough to avoid data loss.
 *
 * <p>We could make direct use of the KPL's protobuf definition and generated classes,
 * but doing so requires bringing in 20+ transitive dependencies. Since the protobuf format is
 * simple and well-documented, we implement a minimal custom parser using the protobuf wire format
 * instead. Additionally, we have integration tests to verify compatibility.
 *
 * @see <a href="https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md">KPL Aggregation Format</a>
 */
final class KplDeaggregator {

    static final byte[] KPL_MAGIC = {(byte) 0xF3, (byte) 0x89, (byte) 0x9A, (byte) 0xC2};
    private static final int MD5_DIGEST_LENGTH = 16;
    private static final int MIN_AGGREGATED_LENGTH = KPL_MAGIC.length + MD5_DIGEST_LENGTH + 1;

    private static final int FIELD_PARTITION_KEY_TABLE = 1;
    private static final int FIELD_EXPLICIT_HASH_KEY_TABLE = 2;
    private static final int FIELD_RECORDS = 3;

    private static final int RECORD_FIELD_PARTITION_KEY_INDEX = 1;
    private static final int RECORD_FIELD_EXPLICIT_HASH_KEY_INDEX = 2;
    private static final int RECORD_FIELD_DATA = 3;

    private KplDeaggregator() {
    }

    /**
     * Deaggregates a list of Kinesis records, expanding any KPL-aggregated records into
     * their constituent sub-records.
     *
     * @param shardId the shard these records were fetched from
     * @param records raw Kinesis records from the API
     * @return list of deaggregated records preserving original order
     */
    static List<DeaggregatedRecord> deaggregate(final String shardId, final List<Record> records) {
        final List<DeaggregatedRecord> result = new ArrayList<>();
        for (final Record record : records) {
            deaggregateRecord(shardId, record, result);
        }
        return result;
    }

    private static void deaggregateRecord(final String shardId, final Record record, final List<DeaggregatedRecord> out) {
        final byte[] data = record.data().asByteArrayUnsafe();

        if (!isAggregated(data)) {
            out.add(passthrough(shardId, record, data));
            return;
        }

        final int protobufOffset = KPL_MAGIC.length;
        final int protobufLength = data.length - KPL_MAGIC.length - MD5_DIGEST_LENGTH;

        if (!verifyMd5(data, protobufOffset, protobufLength)) {
            out.add(passthrough(shardId, record, data));
            return;
        }

        try {
            parseAggregatedRecord(shardId, record, data, protobufOffset, protobufLength, out);
        } catch (final Exception e) {
            out.add(passthrough(shardId, record, data));
        }
    }

    static boolean isAggregated(final byte[] data) {
        if (data.length < MIN_AGGREGATED_LENGTH) {
            return false;
        }
        return data[0] == KPL_MAGIC[0]
                && data[1] == KPL_MAGIC[1]
                && data[2] == KPL_MAGIC[2]
                && data[3] == KPL_MAGIC[3];
    }

    private static boolean verifyMd5(final byte[] data, final int protobufOffset, final int protobufLength) {
        try {
            final MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(data, protobufOffset, protobufLength);
            final byte[] computed = md5.digest();
            final int md5Offset = protobufOffset + protobufLength;
            return Arrays.equals(computed, 0, MD5_DIGEST_LENGTH, data, md5Offset, md5Offset + MD5_DIGEST_LENGTH);
        } catch (final NoSuchAlgorithmException e) {
            return false;
        }
    }

    private static void parseAggregatedRecord(final String shardId, final Record kinesisRecord,
            final byte[] data, final int protobufOffset, final int protobufLength, final List<DeaggregatedRecord> out) throws Exception {

        final List<String> partitionKeyTable = new ArrayList<>();
        final List<byte[]> subRecordDataList = new ArrayList<>();
        final List<Integer> subRecordPkIndexList = new ArrayList<>();

        final CodedInputStream input = CodedInputStream.newInstance(data, protobufOffset, protobufLength);
        while (!input.isAtEnd()) {
            final int tag = input.readTag();
            final int fieldNumber = WireFormat.getTagFieldNumber(tag);
            switch (fieldNumber) {
                case FIELD_PARTITION_KEY_TABLE:
                    partitionKeyTable.add(input.readString());
                    break;
                case FIELD_EXPLICIT_HASH_KEY_TABLE:
                    input.readString();
                    break;
                case FIELD_RECORDS:
                    final int length = input.readRawVarint32();
                    final int oldLimit = input.pushLimit(length);
                    int pkIndex = 0;
                    byte[] subData = new byte[0];
                    while (!input.isAtEnd()) {
                        final int innerTag = input.readTag();
                        final int innerField = WireFormat.getTagFieldNumber(innerTag);
                        switch (innerField) {
                            case RECORD_FIELD_PARTITION_KEY_INDEX:
                                pkIndex = (int) input.readUInt64();
                                break;
                            case RECORD_FIELD_EXPLICIT_HASH_KEY_INDEX:
                                input.readUInt64();
                                break;
                            case RECORD_FIELD_DATA:
                                subData = input.readByteArray();
                                break;
                            default:
                                input.skipField(innerTag);
                                break;
                        }
                    }
                    input.popLimit(oldLimit);
                    subRecordDataList.add(subData);
                    subRecordPkIndexList.add(pkIndex);
                    break;
                default:
                    input.skipField(tag);
                    break;
            }
        }

        final String sequenceNumber = kinesisRecord.sequenceNumber();
        final Instant arrival = kinesisRecord.approximateArrivalTimestamp();
        final String fallbackPartitionKey = kinesisRecord.partitionKey();

        for (int i = 0; i < subRecordDataList.size(); i++) {
            final int pkIdx = subRecordPkIndexList.get(i);
            final String partitionKey = pkIdx < partitionKeyTable.size()
                    ? partitionKeyTable.get(pkIdx)
                    : fallbackPartitionKey;
            out.add(new DeaggregatedRecord(shardId, sequenceNumber, i, partitionKey, subRecordDataList.get(i), arrival));
        }
    }

    private static DeaggregatedRecord passthrough(final String shardId, final Record record, final byte[] data) {
        return new DeaggregatedRecord(
                shardId,
                record.sequenceNumber(),
                0,
                record.partitionKey(),
                data,
                record.approximateArrivalTimestamp());
    }
}
