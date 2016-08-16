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

package org.apache.nifi.toolkit.tls.util;

import org.junit.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class PasswordUtilTest {
    @Test
    public void testGeneratePassword() {
        SecureRandom secureRandom = mock(SecureRandom.class);
        PasswordUtil passwordUtil = new PasswordUtil(secureRandom);
        int value = 8675309;
        doAnswer(invocation -> {
            byte[] bytes = (byte[]) invocation.getArguments()[0];
            assertEquals(32, bytes.length);
            Arrays.fill(bytes, (byte) 0);
            byte[] val = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
            System.arraycopy(val, 0, bytes, bytes.length - val.length, val.length);
            return null;
        }).when(secureRandom).nextBytes(any(byte[].class));
        byte[] expectedBytes = new byte[32];
        byte[] numberBytes = BigInteger.valueOf(Integer.valueOf(value).longValue()).toByteArray();
        System.arraycopy(numberBytes, 0, expectedBytes, expectedBytes.length - numberBytes.length, numberBytes.length);
        String expected = Base64.getEncoder().encodeToString(expectedBytes).split("=")[0];
        String actual = passwordUtil.generatePassword();
        assertEquals(expected, actual);
    }

    @Test(expected = PasswordsExhaustedException.class)
    public void testPasswordExhausted() {
        Supplier<String> supplier = PasswordUtil.passwordSupplier("exhausted", new String[]{"a", "b"});
        supplier.get();
        supplier.get();
        supplier.get();
    }
}
