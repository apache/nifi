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
package org.apache.nifi.confluent.schemaregistry;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfluentEncodedSchemaReferenceReaderTest {
    private static final String SERVICE_ID = ConfluentEncodedSchemaReferenceReader.class.getSimpleName();

    private static final byte MAGIC_BYTE = 0;

    private static final int INVALID_MAGIC_BYTE = 1;

    private static final int ID = 123456;

    private ConfluentEncodedSchemaReferenceReader reader;

    @BeforeEach
    void setRunner() throws InitializationException {
        reader = new ConfluentEncodedSchemaReferenceReader();

        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService(SERVICE_ID, reader);
        runner.enableControllerService(reader);
    }

    @Test
    public void testGetSchemaIdentifier() throws IOException, SchemaNotFoundException {
        try (final ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(bytesOut)) {
            out.write(MAGIC_BYTE);
            out.writeInt(ID);
            out.flush();

            try (final ByteArrayInputStream in = new ByteArrayInputStream(bytesOut.toByteArray())) {
                final SchemaIdentifier schemaIdentifier = reader.getSchemaIdentifier(Collections.emptyMap(), in);

                final OptionalLong schemaVersionId = schemaIdentifier.getSchemaVersionId();
                assertTrue(schemaVersionId.isPresent());
                assertEquals(schemaVersionId.getAsLong(), ID);
            }
        }
    }

    @Test
    public void testGetSchemaIdentifierInvalid() {
        assertThrows(SchemaNotFoundException.class, () -> {
            try (final ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                 final DataOutputStream out = new DataOutputStream(bytesOut)) {
                out.write(INVALID_MAGIC_BYTE);
                out.writeInt(ID);
                out.flush();

                try (final ByteArrayInputStream in = new ByteArrayInputStream(bytesOut.toByteArray())) {
                    reader.getSchemaIdentifier(Collections.emptyMap(), in);
                }
            }
        });
    }
}
