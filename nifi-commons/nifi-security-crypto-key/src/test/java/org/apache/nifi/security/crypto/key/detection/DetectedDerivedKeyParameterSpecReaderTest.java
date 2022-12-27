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
import org.apache.nifi.security.crypto.key.argon2.Argon2DerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.bcrypt.BcryptDerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.pbkdf2.Pbkdf2DerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.scrypt.ScryptDerivedKeyParameterSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class DetectedDerivedKeyParameterSpecReaderTest {

    private static final String UNSPECIFIED_PARAMETERS = "unspecified";

    private static final String ARGON2_PARAMETERS = "$argon2id$v=19$m=65536,t=2,p=1$QXJnb24yU2FsdFN0cmluZw";

    private static final String BCRYPT_PARAMETERS = "$2a$12$R9h/cIPz0gi.URNNX3kh2O";

    private static final String SCRYPT_PARAMETERS = "$s0$e0801$epIxT/h6HbbwHaehFnh/bw";

    DetectedDerivedKeyParameterSpecReader reader;

    @BeforeEach
    void setReader() {
        reader = new DetectedDerivedKeyParameterSpecReader();
    }

    @Test
    void testArgon2() {
        assertParameterSpecInstanceOf(Argon2DerivedKeyParameterSpec.class, ARGON2_PARAMETERS);
    }

    @Test
    void testBcrypt() {
        assertParameterSpecInstanceOf(BcryptDerivedKeyParameterSpec.class, BCRYPT_PARAMETERS);
    }

    @Test
    void testScrypt() {
        assertParameterSpecInstanceOf(ScryptDerivedKeyParameterSpec.class, SCRYPT_PARAMETERS);
    }

    @Test
    void testPbkdf2() {
        assertParameterSpecInstanceOf(Pbkdf2DerivedKeyParameterSpec.class, UNSPECIFIED_PARAMETERS);
    }

    private void assertParameterSpecInstanceOf(final Class<? extends DerivedKeyParameterSpec> parameterSpecClass, final String parameters) {
        final byte[] serializedParameters = parameters.getBytes(StandardCharsets.UTF_8);

        final DerivedKeyParameterSpec parameterSpec = reader.read(serializedParameters);

        assertInstanceOf(parameterSpecClass, parameterSpec);
    }
}
