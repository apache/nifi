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
package org.apache.nifi.security.crypto.key.bcrypt;

import org.apache.nifi.security.crypto.key.DerivedKeyParameterSpecReader;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * bcrypt implementation reads a US-ASCII string of bytes using the Modular Crypt Format specification as added in NiFi 0.5.0
 */
public class BcryptDerivedKeyParameterSpecReader implements DerivedKeyParameterSpecReader<BcryptDerivedKeyParameterSpec> {
    /** Modular Crypt Format containing the cost as a zero-padded number with a trailing bcrypt-Base64 encoded 16 byte salt and optional hash */
    private static final Pattern MODULAR_CRYPT_FORMAT = Pattern.compile("^\\$2[abxy]\\$(\\d{2})\\$([\\w/.]{22})([\\w/.]{31})?$");

    private static final int COST_GROUP = 1;

    private static final int SALT_GROUP = 2;

    private static final Charset PARAMETERS_CHARACTER_SET = StandardCharsets.US_ASCII;

    /**
     * Read serialized parameters parsed from US-ASCII string of bytes
     *
     * @param serializedParameters Serialized parameters
     * @return bcrypt Parameter Specification
     */
    @Override
    public BcryptDerivedKeyParameterSpec read(final byte[] serializedParameters) {
        Objects.requireNonNull(serializedParameters, "Parameters required");
        final String parameters = new String(serializedParameters, PARAMETERS_CHARACTER_SET);

        final Matcher matcher = MODULAR_CRYPT_FORMAT.matcher(parameters);
        if (matcher.matches()) {
            final String costGroup = matcher.group(COST_GROUP);
            final String saltGroup = matcher.group(SALT_GROUP);

            final int cost = Integer.parseInt(costGroup);
            final byte[] salt = BcryptBase64Decoder.decode(saltGroup);

            return new BcryptDerivedKeyParameterSpec(cost, salt);
        } else {
            final String message = String.format("bcrypt serialized parameters [%s] format not matched", parameters);
            throw new IllegalArgumentException(message);
        }
    }
}
