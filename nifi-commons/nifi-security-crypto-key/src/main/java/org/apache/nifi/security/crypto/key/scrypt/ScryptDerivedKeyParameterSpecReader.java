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
package org.apache.nifi.security.crypto.key.scrypt;

import org.apache.nifi.security.crypto.key.DerivedKeyParameterSpecReader;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * scrypt implementation reads a US-ASCII string of bytes using a Modular Crypt Format defined according to the com.lambdaworks:scrypt library
 */
public class ScryptDerivedKeyParameterSpecReader implements DerivedKeyParameterSpecReader<ScryptDerivedKeyParameterSpec> {
    /** Modular Crypt Format containing the parameters encoded as a 32-bit hexadecimal number with a trailing Base64 encoded salt and optional hash */
    private static final Pattern MODULAR_CRYPT_FORMAT = Pattern.compile("^\\$s0\\$([a-f0-9]{5,})\\$([\\w/=+]{11,64})\\$?([\\w/=+]{1,256})?$");

    private static final int PARAMETERS_GROUP = 1;

    private static final int SALT_GROUP = 2;

    private static final Charset PARAMETERS_CHARACTER_SET = StandardCharsets.US_ASCII;

    private static final int HEXADECIMAL_RADIX = 16;

    private static final int LOG_BASE_2 = 2;

    private static final int COST_BITS = 16;

    private static final int SIZE_BITS = 8;

    private static final int SIXTEEN_BIT_SHIFT = 0xffff;

    private static final int EIGHT_BIT_SHIFT = 0xff;

    private static final Base64.Decoder decoder = Base64.getDecoder();

    @Override
    public ScryptDerivedKeyParameterSpec read(final byte[] serializedParameters) {
        Objects.requireNonNull(serializedParameters, "Parameters required");
        final String parameters = new String(serializedParameters, PARAMETERS_CHARACTER_SET);

        final Matcher matcher = MODULAR_CRYPT_FORMAT.matcher(parameters);
        if (matcher.matches()) {
            final String parametersGroup = matcher.group(PARAMETERS_GROUP);
            final String saltGroup = matcher.group(SALT_GROUP);
            return readParameters(parametersGroup, saltGroup);
        } else {
            final String message = String.format("scrypt serialized parameters [%s] format not matched", parameters);
            throw new IllegalArgumentException(message);
        }
    }

    private ScryptDerivedKeyParameterSpec readParameters(final String parametersEncoded, final String saltEncoded) {
        final long parameters = Long.parseLong(parametersEncoded, HEXADECIMAL_RADIX);
        final long costExponent = parameters >> COST_BITS & SIXTEEN_BIT_SHIFT;

        final int cost = (int) Math.pow(LOG_BASE_2, costExponent);
        final int blockSize = (int) parameters >> SIZE_BITS & EIGHT_BIT_SHIFT;
        final int parallelization = (int) parameters & EIGHT_BIT_SHIFT;

        final byte[] salt = decoder.decode(saltEncoded);
        return new ScryptDerivedKeyParameterSpec(cost, blockSize, parallelization, salt);
    }
}
