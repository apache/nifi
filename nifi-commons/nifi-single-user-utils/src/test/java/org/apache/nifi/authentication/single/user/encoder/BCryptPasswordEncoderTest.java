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
package org.apache.nifi.authentication.single.user.encoder;

import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BCryptPasswordEncoderTest {

    private static final Pattern BCRYPT_PATTERN = Pattern.compile("^\\$2b\\$12\\$.+$");

    @Test
    public void testEncode() {
        final BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();
        final String encoded = encoder.encode(String.class.getSimpleName().toCharArray());
        assertNotNull(encoded, "Encoded Password not found");
        assertTrue(BCRYPT_PATTERN.matcher(encoded).matches(), "Encoded Password bcrypt hash not found");
    }

    @Test
    public void testEncodeMatches() {
        final BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();
        final char[] password = String.class.getSimpleName().toCharArray();
        final String encoded = encoder.encode(password);
        assertTrue(encoder.matches(password, encoded), "Encoded Password not matched");
    }
}
