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
package org.apache.nifi.security.crypto.key.detection;

import org.apache.nifi.security.crypto.key.DerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.DerivedKeyParameterSpecReader;
import org.apache.nifi.security.crypto.key.argon2.Argon2DerivedKeyParameterSpecReader;
import org.apache.nifi.security.crypto.key.bcrypt.BcryptDerivedKeyParameterSpecReader;
import org.apache.nifi.security.crypto.key.io.ByteBufferSearch;
import org.apache.nifi.security.crypto.key.pbkdf2.Pbkdf2DerivedKeyParameterSpecReader;
import org.apache.nifi.security.crypto.key.scrypt.ScryptDerivedKeyParameterSpecReader;

import java.nio.ByteBuffer;

/**
 * Delegating implementation capable of selecting a reader based on detected header bytes of serialized parameters
 */
public class DetectedDerivedKeyParameterSpecReader implements DerivedKeyParameterSpecReader<DerivedKeyParameterSpec> {
    /** Argon2id header as implemented in NiFi 1.12.0 */
    private static final byte[] ARGON2_ID_DELIMITER = {'$', 'a', 'r', 'g', 'o', 'n', '2', 'i', 'd', '$'};

    /** bcrypt 2a header as implemented in NiFi 0.5.0 */
    private static final byte[] BCRYPT_2A_DELIMITER = {'$', '2', 'a', '$'};

    /** scrypt header as implemented in NiFi 0.5.0 */
    private static final byte[] SCRYPT_DELIMITER = {'$', 's', '0', '$'};

    private static final Argon2DerivedKeyParameterSpecReader argon2Reader = new Argon2DerivedKeyParameterSpecReader();

    private static final BcryptDerivedKeyParameterSpecReader bcryptReader = new BcryptDerivedKeyParameterSpecReader();

    private static final ScryptDerivedKeyParameterSpecReader scryptReader = new ScryptDerivedKeyParameterSpecReader();

    private static final Pbkdf2DerivedKeyParameterSpecReader pbkdf2Reader = new Pbkdf2DerivedKeyParameterSpecReader();

    /**
     * Read Parameter Specification selects a Reader based on header bytes defaulting to PBKDF2 in absence of a matched pattern
     *
     * @param serializedParameters Serialized parameters
     * @return Derived Key Parameter Specification read from serialized parameters
     */
    @Override
    public DerivedKeyParameterSpec read(final byte[] serializedParameters) {
        final ByteBuffer buffer = ByteBuffer.wrap(serializedParameters);
        final DerivedKeyParameterSpecReader<? extends DerivedKeyParameterSpec> reader = getReader(buffer);
        return reader.read(serializedParameters);
    }

    private DerivedKeyParameterSpecReader<? extends DerivedKeyParameterSpec> getReader(final ByteBuffer buffer) {
        final DerivedKeyParameterSpecReader<? extends DerivedKeyParameterSpec> reader;
        if (ByteBufferSearch.indexOf(buffer, ARGON2_ID_DELIMITER) == 0) {
            reader = argon2Reader;
        } else if (ByteBufferSearch.indexOf(buffer, BCRYPT_2A_DELIMITER) == 0) {
            reader = bcryptReader;
        } else if (ByteBufferSearch.indexOf(buffer, SCRYPT_DELIMITER) == 0) {
            reader = scryptReader;
        } else {
            reader = pbkdf2Reader;
        }
        return reader;
    }
}
