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
package org.apache.nifi.security.crypto.key.argon2;

import org.apache.nifi.security.crypto.key.DerivedKeyParameterSpecReader;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Argon2 implementation reads a US-ASCII string of bytes using the Password-Hashing-Competition String Format specification
 */
public class Argon2DerivedKeyParameterSpecReader implements DerivedKeyParameterSpecReader<Argon2DerivedKeyParameterSpec> {
    /** Argon2id hybrid with version 1.3 and 22 character Base64 encoded salt with optional trailing hash parameter */
    private static final Pattern PHC_STRING_FORMAT = Pattern.compile("^\\$argon2id\\$v=19\\$m=(\\d+),t=(\\d+),p=(\\d+)\\$([\\w/+]{22})(\\$[\\w/+]+)?$");

    private static final int MEMORY_GROUP = 1;

    private static final int ITERATIONS_GROUP = 2;

    private static final int PARALLELISM_GROUP = 3;

    private static final int SALT_GROUP = 4;

    private static final Charset PARAMETERS_CHARACTER_SET = StandardCharsets.US_ASCII;

    private static final Base64.Decoder decoder = Base64.getDecoder();

    /**
     * Read serialized parameters parsed from US-ASCII string of bytes
     *
     * @param serializedParameters Serialized parameters
     * @return Argon2 Parameter Specification
     */
    @Override
    public Argon2DerivedKeyParameterSpec read(final byte[] serializedParameters) {
        Objects.requireNonNull(serializedParameters, "Parameters required");
        final String parameters = new String(serializedParameters, PARAMETERS_CHARACTER_SET);

        final Matcher matcher = PHC_STRING_FORMAT.matcher(parameters);
        if (matcher.matches()) {
            final String memoryGroup = matcher.group(MEMORY_GROUP);
            final String iterationsGroup = matcher.group(ITERATIONS_GROUP);
            final String parallelismGroup = matcher.group(PARALLELISM_GROUP);
            final String saltGroup = matcher.group(SALT_GROUP);

            final int memory = Integer.parseInt(memoryGroup);
            final int iterations = Integer.parseInt(iterationsGroup);
            final int parallelism = Integer.parseInt(parallelismGroup);
            final byte[] salt = decoder.decode(saltGroup);
            return new Argon2DerivedKeyParameterSpec(
                    memory,
                    iterations,
                    parallelism,
                    salt
            );
        } else {
            final String message = String.format("Argon2 serialized parameters [%s] format not matched", parameters);
            throw new IllegalArgumentException(message);
        }
    }
}
