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
package org.apache.nifi.toolkit.config.crypto;

import org.apache.nifi.security.crypto.key.DerivedKey;
import org.apache.nifi.security.crypto.key.DerivedKeySpec;
import org.apache.nifi.security.crypto.key.StandardDerivedKeySpec;
import org.apache.nifi.security.crypto.key.scrypt.ScryptDerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.scrypt.ScryptDerivedKeyProvider;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Standard implementation of Derived Key Generator using scrypt with compatible parameters and preset salt for reproducible outputs
 */
public class StandardDerivedKeyGenerator implements DerivedKeyGenerator {

    private static final String SALT = "NIFI_SCRYPT_SALT";

    private static final ScryptDerivedKeyProvider KEY_PROVIDER = new ScryptDerivedKeyProvider();

    private static final ScryptDerivedKeyParameterSpec KEY_PARAMETER_SPEC = new ScryptDerivedKeyParameterSpec(65536, 8, 1, SALT.getBytes(StandardCharsets.UTF_8));

    private static final String KEY_ALGORITHM = "AES";

    private static final int KEY_LENGTH = 32;

    /**
     * Get Derived Key based on the provided password using scrypt and return an encoded representation
     *
     * @param password Password source for key derivation
     * @return Derived key encoded using hexadecimal
     */
    @Override
    public String getDerivedKeyEncoded(char[] password) {
        Objects.requireNonNull(password, "Password required");

        final DerivedKeySpec<ScryptDerivedKeyParameterSpec> derivedKeySpec = new StandardDerivedKeySpec<>(password, KEY_LENGTH, KEY_ALGORITHM, KEY_PARAMETER_SPEC);
        final DerivedKey derivedKey = KEY_PROVIDER.getDerivedKey(derivedKeySpec);
        return HexFormat.of().formatHex(derivedKey.getEncoded());
    }
}
