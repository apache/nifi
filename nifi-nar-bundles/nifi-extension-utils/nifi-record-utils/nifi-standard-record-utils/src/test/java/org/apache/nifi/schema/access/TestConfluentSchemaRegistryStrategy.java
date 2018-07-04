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
package org.apache.nifi.schema.access;

import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.when;

public class TestConfluentSchemaRegistryStrategy extends AbstractSchemaAccessStrategyTest {

    @Test
    public void testGetSchemaWithValidEncoding() throws IOException, SchemaNotFoundException {
        final SchemaAccessStrategy schemaAccessStrategy = new ConfluentSchemaRegistryStrategy(schemaRegistry);

        final int schemaId = 123456;

        try (final ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(bytesOut)) {
            out.write(0);
            out.writeInt(schemaId);
            out.flush();

            try (final ByteArrayInputStream in = new ByteArrayInputStream(bytesOut.toByteArray())) {

                // the confluent strategy will read the id from the input stream and use '1' as the version
                final SchemaIdentifier expectedSchemaIdentifier = SchemaIdentifier.builder()
                        .id((long)schemaId)
                        .version(1)
                        .build();

                when(schemaRegistry.retrieveSchema(argThat(new SchemaIdentifierMatcher(expectedSchemaIdentifier))))
                        .thenReturn(recordSchema);

                final RecordSchema retrievedSchema = schemaAccessStrategy.getSchema(Collections.emptyMap(), in, recordSchema);
                assertNotNull(retrievedSchema);
            }
        }
    }

    @Test(expected = SchemaNotFoundException.class)
    public void testGetSchemaWithInvalidEncoding() throws IOException, SchemaNotFoundException {
        final SchemaAccessStrategy schemaAccessStrategy = new ConfluentSchemaRegistryStrategy(schemaRegistry);

        final int schemaId = 123456;

        try (final ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(bytesOut)) {
            out.write(1); // write an invalid magic byte
            out.writeInt(schemaId);
            out.flush();

            try (final ByteArrayInputStream in = new ByteArrayInputStream(bytesOut.toByteArray())) {
                schemaAccessStrategy.getSchema(Collections.emptyMap(), in, recordSchema);
            }
        }
    }
}
