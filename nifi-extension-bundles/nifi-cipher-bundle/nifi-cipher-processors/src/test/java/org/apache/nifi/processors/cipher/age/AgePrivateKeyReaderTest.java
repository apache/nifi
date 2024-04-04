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

import com.exceptionfactory.jagged.RecipientStanzaReader;
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

class AgePrivateKeyReaderTest {
    private static final String PRIVATE_KEY_ENCODED = "AGE-SECRET-KEY-1NZKRS8L39Y2KVLXT7XX4DHFVDUUWN0699NJYR2EJS8RWGRYN279Q8GSFTN";

    private static final String PRIVATE_KEY_CHECKSUM_INVALID = "AGE-SECRET-KEY-1NZKRS8L39Y2KVLXT7XX4DHFVDUUWN0699NJYR2EJS8RWGRYN279Q8GSFFF";

    private AgePrivateKeyReader reader;

    @BeforeEach
    void setReader() {
        reader = new AgePrivateKeyReader();
    }

    @Test
    void testRead() throws IOException {
        final InputStream inputStream = new ByteArrayInputStream(PRIVATE_KEY_ENCODED.getBytes(StandardCharsets.UTF_8));

        final Iterator<RecipientStanzaReader> recipientStanzaReaders = reader.read(inputStream).iterator();

        assertTrue(recipientStanzaReaders.hasNext());

        final RecipientStanzaReader recipientStanzaReader = recipientStanzaReaders.next();
        assertNotNull(recipientStanzaReader);

        assertFalse(recipientStanzaReaders.hasNext());
    }

    @Test
    void testReadChecksumInvalid() {
        final InputStream inputStream = new ByteArrayInputStream(PRIVATE_KEY_CHECKSUM_INVALID.getBytes(StandardCharsets.UTF_8));

        assertThrows(IOException.class, () -> reader.read(inputStream));
    }
}
