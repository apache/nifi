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
package org.apache.nifi.parquet;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ParquetTestUtils {

    public static File createUsersParquetFile(final int numUsers) throws IOException {
        return createUsersParquetFile(IntStream
                .range(0, numUsers)
                .mapToObj(ParquetTestUtils::createUser)
                .collect(toList())
        );
    }

    public static Map<String, Object> createUser(final int i) {
        return Map.of("name", "Bob" + i, "favorite_number", i, "favorite_color", "blue" + i);
    }

    private static File createUsersParquetFile(final Collection<Map<String, Object>> users) throws IOException {
        final Schema schema = getSchema();
        final File parquetFile = new File("target/TestParquetReader-testReadUsers-" + System.currentTimeMillis());

        // write some users to the parquet file...
        try (final ParquetWriter<GenericRecord> writer = createParquetWriter(schema, parquetFile)) {
            users.forEach(user -> {
                final GenericRecord record = new GenericData.Record(schema);
                user.forEach(record::put);
                try {
                    writer.write(record);
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return parquetFile;
    }

    private static Schema getSchema() throws IOException {
        try (InputStream schemaInputStream = ParquetTestUtils.class.getClassLoader().getResourceAsStream("avro/user.avsc")) {
            assertNotNull(schemaInputStream);
            final String schemaString = IOUtils.toString(schemaInputStream, StandardCharsets.UTF_8);
            return new Schema.Parser().parse(schemaString);
        }
    }

    private static ParquetWriter<GenericRecord> createParquetWriter(final Schema schema, final File parquetFile) throws IOException {
        final Configuration conf = new Configuration();
        final Path parquetPath = new Path(parquetFile.getPath());

        return AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(parquetPath, conf))
                .withSchema(schema)
                .withConf(conf)
                .withRowGroupSize(8192L)
                .build();
    }

    /**
     * Creates a parquet file with all temporal logical types for comprehensive testing of NIFI-15548.
     * This tests: date, time-millis, time-micros, timestamp-millis, timestamp-micros,
     * and a nullable timestamp-millis field (union with null).
     *
     * @param numRecords Number of records to create
     * @param directory Directory where the parquet file will be created
     * @return The created parquet file
     * @throws IOException if file creation fails
     */
    public static File createAllTemporalTypesParquetFile(final int numRecords, final File directory) throws IOException {
        // Create schemas for all temporal logical types
        final Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        final Schema timeMillisSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
        final Schema timeMicrosSchema = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
        final Schema timestampMillisSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        final Schema timestampMicrosSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        // Nullable timestamp (union with null) to test union handling
        final Schema nullableTimestampSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), timestampMillisSchema);

        final Schema recordSchema = Schema.createRecord("AllTemporalTypesRecord", null, "test", false);
        recordSchema.setFields(List.of(
                new Schema.Field("id", Schema.create(Schema.Type.INT)),
                new Schema.Field("date_field", dateSchema),
                new Schema.Field("time_millis_field", timeMillisSchema),
                new Schema.Field("time_micros_field", timeMicrosSchema),
                new Schema.Field("timestamp_millis_field", timestampMillisSchema),
                new Schema.Field("timestamp_micros_field", timestampMicrosSchema),
                new Schema.Field("nullable_timestamp_field", nullableTimestampSchema)
        ));

        final File parquetFile = new File(directory, "TestParquetReader-testAllTemporalTypes-" + System.currentTimeMillis());

        try (final ParquetWriter<GenericRecord> writer = createParquetWriter(recordSchema, parquetFile)) {
            for (int i = 0; i < numRecords; i++) {
                final GenericRecord record = new GenericData.Record(recordSchema);
                record.put("id", i);
                // date: days since epoch
                record.put("date_field", (int) LocalDate.now().toEpochDay());
                // time-millis: milliseconds since midnight
                record.put("time_millis_field", LocalTime.now().toSecondOfDay() * 1000);
                // time-micros: microseconds since midnight
                record.put("time_micros_field", LocalTime.now().toNanoOfDay() / 1000L);
                // timestamp-millis: milliseconds since epoch
                record.put("timestamp_millis_field", Instant.now().toEpochMilli());
                // timestamp-micros: microseconds since epoch
                record.put("timestamp_micros_field", Instant.now().toEpochMilli() * 1000L);
                // nullable timestamp: milliseconds since epoch (non-null value)
                record.put("nullable_timestamp_field", Instant.now().toEpochMilli());
                writer.write(record);
            }
        }

        return parquetFile;
    }

    private ParquetTestUtils() { }
}
