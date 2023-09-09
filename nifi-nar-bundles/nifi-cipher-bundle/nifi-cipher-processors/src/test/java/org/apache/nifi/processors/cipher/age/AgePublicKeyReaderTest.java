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
package org.apache.nifi.processors.cipher.age;

import com.exceptionfactory.jagged.RecipientStanzaWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AgePublicKeyReaderTest {
    private static final String PUBLIC_KEY_ENCODED = "age1qqnete0p2wzcc7y55trm3c4jzr608g4xdvyfw9jurugyt6maauussgn5gg";

    private static final String PUBLIC_KEY_CHECKSUM_INVALID = "age1qqnete0p2wzcc7y55trm3c4jzr608g4xdvyfw9jurugyt6maauussgn000";

    private AgePublicKeyReader reader;

    @BeforeEach
    void setReader() {
        reader = new AgePublicKeyReader();
    }

    @Test
    void testRead() throws IOException {
        final InputStream inputStream = new ByteArrayInputStream(PUBLIC_KEY_ENCODED.getBytes(StandardCharsets.UTF_8));

        final Iterator<RecipientStanzaWriter> recipientStanzaWriters = reader.read(inputStream).iterator();

        assertTrue(recipientStanzaWriters.hasNext());

        final RecipientStanzaWriter recipientStanzaWriter = recipientStanzaWriters.next();
        assertNotNull(recipientStanzaWriter);

        assertFalse(recipientStanzaWriters.hasNext());
    }

    @Test
    void testReadChecksumInvalid() {
        final InputStream inputStream = new ByteArrayInputStream(PUBLIC_KEY_CHECKSUM_INVALID.getBytes(StandardCharsets.UTF_8));

        assertThrows(IOException.class, () -> reader.read(inputStream));
    }
}
