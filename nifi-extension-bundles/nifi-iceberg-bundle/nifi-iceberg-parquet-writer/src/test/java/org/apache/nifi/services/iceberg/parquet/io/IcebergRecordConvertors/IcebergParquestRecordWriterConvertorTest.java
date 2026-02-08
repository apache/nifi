package org.apache.nifi.services.iceberg.parquet.io.IcebergRecordConvertors;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.nifi.services.iceberg.parquet.io.IcbergRecordConvertors.IcebergParquetRecordWriterConvertor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

class IcebergParquetRecordWriterConvertorTest {

    private IcebergParquetRecordWriterConvertor convertor;

    @BeforeEach
    void setUp() {
        convertor = new IcebergParquetRecordWriterConvertor();
    }

    @Test
    void testConvertTimestampToLocalDateTime() {
        final Schema schema = new Schema(
                Types.NestedField.required(1, "ts_field", Types.TimestampType.withoutZone())
        );
        final Timestamp timestamp = Timestamp.valueOf("2026-06-06 06:06:06");
        final Record record = GenericRecord.create(schema);
        record.set(0, timestamp);

        convertor.convertRecord(record);

        Object result = record.get(0);
        assertInstanceOf(LocalDateTime.class, result);
        assertEquals(timestamp.toLocalDateTime(), result);
    }

    @Test
    void testConvertNestedRecord() {
        final Schema innerSchema = new Schema(
                Types.NestedField.required(1, "inner_ts", Types.TimestampType.withoutZone())
        );
        final Schema outerSchema = new Schema(
                Types.NestedField.required(2, "nested_record", innerSchema.asStruct())
        );

        final Record innerRecord = GenericRecord.create(innerSchema);
        final Timestamp timestamp = Timestamp.valueOf("2026-06-06 06:06:06");
        innerRecord.set(0, timestamp);

        final Record outerRecord = GenericRecord.create(outerSchema);
        outerRecord.set(0, innerRecord);

        convertor.convertRecord(outerRecord);

        Record resultInner = (Record) outerRecord.get(0);
        assertInstanceOf(LocalDateTime.class, resultInner.get(0));
    }

    @Test
    void testConvertListWithRecords() {
        final Schema elementSchema = new Schema(
                Types.NestedField.required(1, "ts", Types.TimestampType.withoutZone())
        );
        final Record recordInList = GenericRecord.create(elementSchema);
        recordInList.set(0, Timestamp.valueOf("2026-06-06 06:06:06"));

        final List<Record> list = Collections.singletonList(recordInList);

        convertor.convertList(list);

        assertInstanceOf(LocalDateTime.class, recordInList.get(0));
    }

    @Test
    void testNullValueHandling() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "nullable_field", Types.TimestampType.withoutZone())
        );
        final Record record = GenericRecord.create(schema);
        record.set(0, null);

        convertor.convertRecord(record);

        assertNull(record.get(0));
    }
}