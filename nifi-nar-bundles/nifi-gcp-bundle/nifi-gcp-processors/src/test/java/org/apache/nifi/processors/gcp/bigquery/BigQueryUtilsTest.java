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

package org.apache.nifi.processors.gcp.bigquery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Field.Mode;

public class BigQueryUtilsTest {

    @Test
    public void can_create_a_simple_boolean_schema() throws IOException {
        final String jsonRead = new String(
                Files.readAllBytes(Paths.get("src/test/resources/schemas/simple_boolean_schema.json"))
        );
        Field booleanField = Field.newBuilder("boolean", LegacySQLTypeName.BOOLEAN).setMode(Mode.NULLABLE).build();
        Schema expected = Schema.of(booleanField);
        Assert.assertEquals(expected, BigQueryUtils.schemaFromString(jsonRead));
    }

    @Test
    public void can_create_a_simple_record_schema_with_bad_type_case() throws IOException {
        final String jsonRead = new String(
                Files.readAllBytes(Paths.get("src/test/resources/schemas/simple_record_schema_with_bad_type_case.json"))
        );
        Field booleanField = Field.newBuilder("boolean", LegacySQLTypeName.BOOLEAN).setMode(Mode.NULLABLE).build();
        Schema expected = Schema.of(Field.of("Consent", LegacySQLTypeName.RECORD,
                booleanField
                ).toBuilder().setMode(Mode.NULLABLE).build());
        Assert.assertEquals(expected, BigQueryUtils.schemaFromString(jsonRead));
    }

    @Test
    public void can_create_a_simple_record_schema() throws IOException {
        final String jsonRead = new String(
                Files.readAllBytes(Paths.get("src/test/resources/schemas/simple_record_schema.json"))
        );
        Field booleanField = Field.newBuilder("boolean", LegacySQLTypeName.BOOLEAN).setMode(Mode.NULLABLE).build();
        Schema expected = Schema.of(Field.of("Consent", LegacySQLTypeName.RECORD,
                booleanField
                ).toBuilder().setMode(Mode.NULLABLE).build());
        Assert.assertEquals(expected, BigQueryUtils.schemaFromString(jsonRead));
    }


    @Test
    public void can_create_a_nested_record_schema() throws IOException {
        final String jsonRead = new String(
                Files.readAllBytes(Paths.get("src/test/resources/schemas/nested_record_schema.json"))
        );
        Field booleanField = Field.newBuilder("boolean", LegacySQLTypeName.BOOLEAN).setMode(Mode.NULLABLE).build();
        Field nestedRecord = Field.of("detail", LegacySQLTypeName.RECORD,
                booleanField
                ).toBuilder().setMode(Mode.NULLABLE).build();
        Schema expected = Schema.of(Field.of("Consent", LegacySQLTypeName.RECORD,
                nestedRecord
                ).toBuilder().setMode(Mode.NULLABLE).build());
        Assert.assertEquals(expected, BigQueryUtils.schemaFromString(jsonRead));
    }
}
