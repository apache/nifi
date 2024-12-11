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

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

public class ParquetTestUtils {

    public static File createUsersParquetFile(int numUsers) throws IOException {
        return createUsersParquetFile(IntStream
                .range(0, numUsers)
                .mapToObj(ParquetTestUtils::createUser)
                .collect(toList())
        );
    }

    public static Map<String, Object> createUser(int i) {
        return Map.of("name", "Bob" + i, "favorite_number", i, "favorite_color", "blue" + i);
    }

    private static File createUsersParquetFile(Collection<Map<String, Object>> users) throws IOException {
        final Schema schema = getSchema();
        final File parquetFile = new File("target/TestParquetReader-testReadUsers-" + System.currentTimeMillis());

        // write some users to the parquet file...
        try (final ParquetWriter<GenericRecord> writer = createParquetWriter(schema, parquetFile)) {
            users.forEach(user -> {
                final GenericRecord record = new GenericData.Record(schema);
                user.forEach(record::put);
                try {
                    writer.write(record);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return parquetFile;
    }

    private static Schema getSchema() throws IOException {
        try (InputStream schemaInputStream = ParquetTestUtils.class.getClassLoader().getResourceAsStream("avro/user.avsc")) {
            assert schemaInputStream != null;
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

    private ParquetTestUtils() { }
}
