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

import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BCryptPasswordEncoderTest {

    private static final Pattern BCRYPT_PATTERN = Pattern.compile("^\\$2b\\$12\\$.+$");

    @Test
    public void testEncode() {
        final BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();
        final String encoded = encoder.encode(String.class.getSimpleName().toCharArray());
        assertNotNull("Encoded Password not found", encoded);
        assertTrue("Encoded Password bcrypt hash not found", BCRYPT_PATTERN.matcher(encoded).matches());
    }

    @Test
    public void testEncodeMatches() {
        final BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();
        final char[] password = String.class.getSimpleName().toCharArray();
        final String encoded = encoder.encode(password);
        assertTrue("Encoded Password not matched", encoder.matches(password, encoded));
    }
}
