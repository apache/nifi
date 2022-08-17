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

import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PasswordUtilTest {
    @Test
    public void testGeneratePassword() {
        SecureRandom secureRandom = new SecureRandom();
        PasswordUtil passwordUtil = new PasswordUtil(secureRandom);
        String generated = passwordUtil.generatePassword();
        assertEquals(43, generated.length());
    }

    @Test
    public void testPasswordExhausted() {
        Supplier<String> supplier = PasswordUtil.passwordSupplier("exhausted", new String[]{"a", "b"});
        supplier.get();
        supplier.get();
        assertThrows(PasswordsExhaustedException.class, supplier::get);
    }
}
