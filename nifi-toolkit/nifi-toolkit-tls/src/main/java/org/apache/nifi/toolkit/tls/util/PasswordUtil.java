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

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class PasswordUtil {
    private final SecureRandom secureRandom;

    public PasswordUtil() {
        this(new SecureRandom());
    }

    public PasswordUtil(SecureRandom secureRandom) {
        this.secureRandom = secureRandom;
    }

    public String generatePassword() {
        // [see http://stackoverflow.com/questions/41107/how-to-generate-a-random-alpha-numeric-string#answer-41156]
        String string = Base64.getEncoder().encodeToString(new BigInteger(256, secureRandom).toByteArray());
        while (string.endsWith("=")) {
            string = string.substring(0, string.length() - 1);
        }
        return string;
    }

    public Supplier<String> passwordSupplier() {
        return () -> generatePassword();
    }

    public static Supplier<String> passwordSupplier(String password) {
        return () -> password;
    }

    public static Supplier<String> passwordSupplier(String exhaustedMessage, String[] passwords) {
        AtomicInteger index = new AtomicInteger(0);
        return () -> {
            int i = index.getAndIncrement();
            if (i < passwords.length) {
                return passwords[i];
            } else {
                throw new PasswordsExhaustedException(exhaustedMessage);
            }
        };
    }
}
