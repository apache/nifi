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

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.kpl.Messages;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerLibraryDeaggregatorTest {

    private static final Instant ARRIVAL = Instant.parse("2025-06-15T12:00:00Z");
    private static final String TEST_SHARD_ID = "shardId-000000000000";

    @Test
    void testNonAggregatedPassthrough() {
        final byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        final Record record = buildKinesisRecord("seq-001", "pk-1", payload);

        final List<UserRecord> result = ProducerLibraryDeaggregator.deaggregate(TEST_SHARD_ID, List.of(record));

        assertEquals(1, result.size());
        final UserRecord dr = result.getFirst();
        assertEquals(TEST_SHARD_ID, dr.shardId());
        assertEquals("seq-001", dr.sequenceNumber());
        assertEquals(0, dr.subSequenceNumber());
        assertEquals("pk-1", dr.partitionKey());
        assertArrayEquals(payload, dr.data());
        assertEquals(ARRIVAL, dr.approximateArrivalTimestamp());
    }

    @Test
    void testSingleSubRecord() throws Exception {
        final byte[] aggregated = buildAggregatedPayload(
                List.of("pk-A"),
                List.of(new SubRecord(0, "data-A".getBytes(StandardCharsets.UTF_8))));
        final Record record = buildKinesisRecord("seq-100", "agg-pk", aggregated);

        final List<UserRecord> result = ProducerLibraryDeaggregator.deaggregate(TEST_SHARD_ID, List.of(record));

        assertEquals(1, result.size());
        final UserRecord dr = result.getFirst();
        assertEquals("seq-100", dr.sequenceNumber());
        assertEquals(0, dr.subSequenceNumber());
        assertEquals("pk-A", dr.partitionKey());
        assertArrayEquals("data-A".getBytes(StandardCharsets.UTF_8), dr.data());
    }

    @Test
    void testMultipleSubRecords() throws Exception {
        final byte[] aggregated = buildAggregatedPayload(
                List.of("pk-X", "pk-Y"),
                List.of(
                        new SubRecord(0, "first".getBytes(StandardCharsets.UTF_8)),
                        new SubRecord(1, "second".getBytes(StandardCharsets.UTF_8)),
                        new SubRecord(0, "third".getBytes(StandardCharsets.UTF_8))));
        final Record record = buildKinesisRecord("seq-200", "agg-pk", aggregated);

        final List<UserRecord> result = ProducerLibraryDeaggregator.deaggregate(TEST_SHARD_ID, List.of(record));

        assertEquals(3, result.size());

        assertEquals("pk-X", result.get(0).partitionKey());
        assertEquals(0, result.get(0).subSequenceNumber());
        assertArrayEquals("first".getBytes(StandardCharsets.UTF_8), result.get(0).data());

        assertEquals("pk-Y", result.get(1).partitionKey());
        assertEquals(1, result.get(1).subSequenceNumber());
        assertArrayEquals("second".getBytes(StandardCharsets.UTF_8), result.get(1).data());

        assertEquals("pk-X", result.get(2).partitionKey());
        assertEquals(2, result.get(2).subSequenceNumber());
        assertArrayEquals("third".getBytes(StandardCharsets.UTF_8), result.get(2).data());

        for (final UserRecord dr : result) {
            assertEquals("seq-200", dr.sequenceNumber());
            assertEquals(ARRIVAL, dr.approximateArrivalTimestamp());
        }
    }

    @Test
    void testMixedAggregatedAndNonAggregated() throws Exception {
        final byte[] plainPayload = "plain-data".getBytes(StandardCharsets.UTF_8);
        final Record plainRecord = buildKinesisRecord("seq-001", "pk-plain", plainPayload);

        final byte[] aggregated = buildAggregatedPayload(
                List.of("pk-agg"),
                List.of(new SubRecord(0, "agg-data".getBytes(StandardCharsets.UTF_8))));
        final Record aggRecord = buildKinesisRecord("seq-002", "pk-outer", aggregated);

        final List<UserRecord> result = ProducerLibraryDeaggregator.deaggregate(TEST_SHARD_ID, List.of(plainRecord, aggRecord));

        assertEquals(2, result.size());
        assertEquals("seq-001", result.get(0).sequenceNumber());
        assertArrayEquals(plainPayload, result.get(0).data());
        assertEquals("seq-002", result.get(1).sequenceNumber());
        assertArrayEquals("agg-data".getBytes(StandardCharsets.UTF_8), result.get(1).data());
    }

    @Test
    void testCorruptedProtobufFallsBackToPassthrough() {
        final byte[] corrupted = new byte[ProducerLibraryDeaggregator.KPL_MAGIC.length + 20 + 16];
        System.arraycopy(ProducerLibraryDeaggregator.KPL_MAGIC, 0, corrupted, 0, ProducerLibraryDeaggregator.KPL_MAGIC.length);
        final byte[] protobufPart = new byte[20];
        protobufPart[0] = (byte) 0xFF;
        System.arraycopy(protobufPart, 0, corrupted, ProducerLibraryDeaggregator.KPL_MAGIC.length, 20);
        try {
            final byte[] md5 = MessageDigest.getInstance("MD5").digest(protobufPart);
            System.arraycopy(md5, 0, corrupted, ProducerLibraryDeaggregator.KPL_MAGIC.length + 20, 16);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        final Record record = buildKinesisRecord("seq-bad", "pk-bad", corrupted);
        final List<UserRecord> result = ProducerLibraryDeaggregator.deaggregate(TEST_SHARD_ID, List.of(record));

        assertEquals(1, result.size());
        assertEquals("seq-bad", result.get(0).sequenceNumber());
        assertEquals(0, result.get(0).subSequenceNumber());
        assertArrayEquals(corrupted, result.get(0).data());
    }

    @Test
    void testMd5MismatchFallsBackToPassthrough() throws Exception {
        final byte[] aggregated = buildAggregatedPayload(
                List.of("pk-1"),
                List.of(new SubRecord(0, "data".getBytes(StandardCharsets.UTF_8))));

        aggregated[aggregated.length - 1] ^= 0xFF;

        final Record record = buildKinesisRecord("seq-md5", "pk-md5", aggregated);
        final List<UserRecord> result = ProducerLibraryDeaggregator.deaggregate(TEST_SHARD_ID, List.of(record));

        assertEquals(1, result.size());
        assertEquals(0, result.get(0).subSequenceNumber());
        assertArrayEquals(aggregated, result.get(0).data());
    }

    @Test
    void testIsAggregatedDetection() {
        assertFalse(ProducerLibraryDeaggregator.isAggregated(new byte[0]));
        assertFalse(ProducerLibraryDeaggregator.isAggregated(new byte[]{0x01, 0x02}));
        assertFalse(ProducerLibraryDeaggregator.isAggregated("regular data".getBytes(StandardCharsets.UTF_8)));

        final byte[] withMagic = new byte[ProducerLibraryDeaggregator.KPL_MAGIC.length + 16 + 1];
        System.arraycopy(ProducerLibraryDeaggregator.KPL_MAGIC, 0, withMagic, 0, ProducerLibraryDeaggregator.KPL_MAGIC.length);
        assertTrue(ProducerLibraryDeaggregator.isAggregated(withMagic));
    }

    @Test
    void testEmptyRecordList() {
        final List<UserRecord> result = ProducerLibraryDeaggregator.deaggregate(TEST_SHARD_ID, List.of());
        assertTrue(result.isEmpty());
    }

    // ---- KCL cross-validation tests ----
    // These tests use the KCL's own protobuf Messages class to create aggregated records,
    // then verify that our ProducerLibraryDeaggregator produces the same results as the KCL's AggregatorUtil.

    @Test
    void testKclAggregatedSingleRecord() {
        final Messages.AggregatedRecord aggProto = Messages.AggregatedRecord.newBuilder()
                .addPartitionKeyTable("pk-kcl-1")
                .addRecords(Messages.Record.newBuilder()
                        .setPartitionKeyIndex(0)
                        .setData(ByteString.copyFromUtf8("hello from KPL")))
                .build();
        final byte[] payload = wrapAsKplPayload(aggProto.toByteArray());
        final Record kinesisRecord = buildKinesisRecord("seq-kcl-1", "outer-pk", payload);

        final List<UserRecord> ourResult = ProducerLibraryDeaggregator.deaggregate(TEST_SHARD_ID, List.of(kinesisRecord));
        final List<KinesisClientRecord> kclResult = deaggregateViaKcl(kinesisRecord);

        assertEquals(1, ourResult.size());
        assertEquals(kclResult.size(), ourResult.size());

        assertDeaggregatedMatchesKcl(ourResult.getFirst(), kclResult.getFirst());
        assertEquals("pk-kcl-1", ourResult.getFirst().partitionKey());
        assertArrayEquals("hello from KPL".getBytes(StandardCharsets.UTF_8), ourResult.getFirst().data());
    }

    @Test
    void testKclAggregatedMultipleRecords() {
        final Messages.AggregatedRecord aggProto = Messages.AggregatedRecord.newBuilder()
                .addPartitionKeyTable("pk-alpha")
                .addPartitionKeyTable("pk-beta")
                .addRecords(Messages.Record.newBuilder()
                        .setPartitionKeyIndex(0)
                        .setData(ByteString.copyFromUtf8("record-0")))
                .addRecords(Messages.Record.newBuilder()
                        .setPartitionKeyIndex(1)
                        .setData(ByteString.copyFromUtf8("record-1")))
                .addRecords(Messages.Record.newBuilder()
                        .setPartitionKeyIndex(0)
                        .setData(ByteString.copyFromUtf8("record-2")))
                .build();
        final byte[] payload = wrapAsKplPayload(aggProto.toByteArray());
        final Record kinesisRecord = buildKinesisRecord("seq-kcl-multi", "outer-pk", payload);

        final List<UserRecord> ourResult = ProducerLibraryDeaggregator.deaggregate(TEST_SHARD_ID, List.of(kinesisRecord));
        final List<KinesisClientRecord> kclResult = deaggregateViaKcl(kinesisRecord);

        assertEquals(3, ourResult.size());
        assertEquals(kclResult.size(), ourResult.size());

        for (int i = 0; i < ourResult.size(); i++) {
            assertDeaggregatedMatchesKcl(ourResult.get(i), kclResult.get(i));
        }

        assertEquals("pk-alpha", ourResult.get(0).partitionKey());
        assertEquals(0, ourResult.get(0).subSequenceNumber());
        assertArrayEquals("record-0".getBytes(StandardCharsets.UTF_8), ourResult.get(0).data());

        assertEquals("pk-beta", ourResult.get(1).partitionKey());
        assertEquals(1, ourResult.get(1).subSequenceNumber());
        assertArrayEquals("record-1".getBytes(StandardCharsets.UTF_8), ourResult.get(1).data());

        assertEquals("pk-alpha", ourResult.get(2).partitionKey());
        assertEquals(2, ourResult.get(2).subSequenceNumber());
        assertArrayEquals("record-2".getBytes(StandardCharsets.UTF_8), ourResult.get(2).data());
    }

    @Test
    void testKclAggregatedMixedWithPlainRecords() {
        final Record plainRecord = buildKinesisRecord("seq-plain", "pk-plain",
                "plain-data".getBytes(StandardCharsets.UTF_8));

        final Messages.AggregatedRecord aggProto = Messages.AggregatedRecord.newBuilder()
                .addPartitionKeyTable("pk-inner")
                .addRecords(Messages.Record.newBuilder()
                        .setPartitionKeyIndex(0)
                        .setData(ByteString.copyFromUtf8("agg-data")))
                .build();
        final Record aggRecord = buildKinesisRecord("seq-agg", "outer-pk",
                wrapAsKplPayload(aggProto.toByteArray()));

        final List<UserRecord> ourResult = ProducerLibraryDeaggregator.deaggregate(TEST_SHARD_ID, List.of(plainRecord, aggRecord));

        final List<KinesisClientRecord> kclPlain = deaggregateViaKcl(plainRecord);
        final List<KinesisClientRecord> kclAgg = deaggregateViaKcl(aggRecord);

        assertEquals(2, ourResult.size());
        assertEquals(1, kclPlain.size());
        assertEquals(1, kclAgg.size());

        assertDeaggregatedMatchesKcl(ourResult.get(0), kclPlain.getFirst());
        assertDeaggregatedMatchesKcl(ourResult.get(1), kclAgg.getFirst());
    }

    @Test
    void testKclAggregatedWithExplicitHashKeys() {
        final Messages.AggregatedRecord aggProto = Messages.AggregatedRecord.newBuilder()
                .addPartitionKeyTable("pk-0")
                .addExplicitHashKeyTable("12345678901234567890")
                .addRecords(Messages.Record.newBuilder()
                        .setPartitionKeyIndex(0)
                        .setExplicitHashKeyIndex(0)
                        .setData(ByteString.copyFromUtf8("with-ehk")))
                .build();
        final byte[] payload = wrapAsKplPayload(aggProto.toByteArray());
        final Record kinesisRecord = buildKinesisRecord("seq-ehk", "outer-pk", payload);

        final List<UserRecord> ourResult = ProducerLibraryDeaggregator.deaggregate(TEST_SHARD_ID, List.of(kinesisRecord));
        final List<KinesisClientRecord> kclResult = deaggregateViaKcl(kinesisRecord);

        assertEquals(1, ourResult.size());
        assertEquals(kclResult.size(), ourResult.size());
        assertDeaggregatedMatchesKcl(ourResult.getFirst(), kclResult.getFirst());
        assertArrayEquals("with-ehk".getBytes(StandardCharsets.UTF_8), ourResult.getFirst().data());
    }

    @Test
    void testKclAggregatedLargeBatch() {
        final Messages.AggregatedRecord.Builder builder = Messages.AggregatedRecord.newBuilder()
                .addPartitionKeyTable("pk-batch");
        for (int i = 0; i < 100; i++) {
            builder.addRecords(Messages.Record.newBuilder()
                    .setPartitionKeyIndex(0)
                    .setData(ByteString.copyFromUtf8("record-" + i)));
        }
        final byte[] payload = wrapAsKplPayload(builder.build().toByteArray());
        final Record kinesisRecord = buildKinesisRecord("seq-batch", "outer-pk", payload);

        final List<UserRecord> ourResult = ProducerLibraryDeaggregator.deaggregate(TEST_SHARD_ID, List.of(kinesisRecord));
        final List<KinesisClientRecord> kclResult = deaggregateViaKcl(kinesisRecord);

        assertEquals(100, ourResult.size());
        assertEquals(kclResult.size(), ourResult.size());

        for (int i = 0; i < ourResult.size(); i++) {
            assertDeaggregatedMatchesKcl(ourResult.get(i), kclResult.get(i));
            assertEquals(i, ourResult.get(i).subSequenceNumber());
            assertArrayEquals(("record-" + i).getBytes(StandardCharsets.UTF_8), ourResult.get(i).data());
        }
    }

    // ---- Test helpers ----

    private record SubRecord(int partitionKeyIndex, byte[] data) { }

    private static Record buildKinesisRecord(final String sequenceNumber, final String partitionKey, final byte[] data) {
        return Record.builder()
                .sequenceNumber(sequenceNumber)
                .partitionKey(partitionKey)
                .data(SdkBytes.fromByteArray(data))
                .approximateArrivalTimestamp(ARRIVAL)
                .build();
    }

    /**
     * Wraps raw protobuf bytes in the KPL envelope format (magic + protobuf + MD5).
     *
     * @param protobufBytes serialized protobuf content
     * @return complete KPL aggregated payload
     */
    private static byte[] wrapAsKplPayload(final byte[] protobufBytes) {
        try {
            final byte[] md5 = MessageDigest.getInstance("MD5").digest(protobufBytes);
            final ByteArrayOutputStream result = new ByteArrayOutputStream();
            result.write(ProducerLibraryDeaggregator.KPL_MAGIC);
            result.write(protobufBytes);
            result.write(md5);
            return result.toByteArray();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deaggregates a single SDK v2 Record using the KCL's own {@link AggregatorUtil}.
     *
     * @param record the SDK v2 Kinesis record
     * @return deaggregated KCL records
     */
    private static List<KinesisClientRecord> deaggregateViaKcl(final Record record) {
        final KinesisClientRecord kcr = KinesisClientRecord.fromRecord(record);
        return new AggregatorUtil().deaggregate(List.of(kcr));
    }

    private static void assertDeaggregatedMatchesKcl(final UserRecord ours, final KinesisClientRecord kcl) {
        assertEquals(kcl.sequenceNumber(), ours.sequenceNumber(), "sequence number mismatch");
        assertEquals(kcl.subSequenceNumber(), ours.subSequenceNumber(), "sub-sequence number mismatch");
        assertEquals(kcl.partitionKey(), ours.partitionKey(), "partition key mismatch");
        assertArrayEquals(toBytes(kcl.data()), ours.data(), "data mismatch");
    }

    private static byte[] toBytes(final ByteBuffer buffer) {
        final ByteBuffer dup = buffer.duplicate();
        final byte[] bytes = new byte[dup.remaining()];
        dup.get(bytes);
        return bytes;
    }

    private static byte[] buildAggregatedPayload(final List<String> partitionKeys, final List<SubRecord> subRecords)
            throws Exception {
        final ByteArrayOutputStream protobufBuffer = new ByteArrayOutputStream();
        final CodedOutputStream cos = CodedOutputStream.newInstance(protobufBuffer);

        for (final String pk : partitionKeys) {
            cos.writeString(1, pk);
        }

        for (final SubRecord sub : subRecords) {
            final int innerSize = computeInnerRecordSize(sub);
            cos.writeTag(3, 2);
            cos.writeUInt32NoTag(innerSize);
            cos.writeUInt64(1, sub.partitionKeyIndex);
            cos.writeByteArray(3, sub.data);
        }

        cos.flush();
        final byte[] protobufBytes = protobufBuffer.toByteArray();
        return wrapAsKplPayload(protobufBytes);
    }

    private static int computeInnerRecordSize(final SubRecord sub) {
        int size = 0;
        size += CodedOutputStream.computeUInt64Size(1, sub.partitionKeyIndex);
        size += CodedOutputStream.computeByteArraySize(3, sub.data);
        return size;
    }
}
