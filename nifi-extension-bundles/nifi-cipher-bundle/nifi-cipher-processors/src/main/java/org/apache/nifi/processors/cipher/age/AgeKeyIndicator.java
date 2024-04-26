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
package org.apache.nifi.processors.cipher.age;

import java.util.regex.Pattern;

/**
 * Pattern indicators for age-encryption.org public and private keys
 */
public enum AgeKeyIndicator {
    /** Human-Readable Part with Separator and enumeration of allowed Bech32 uppercase characters from BIP 0173 */
    PRIVATE_KEY("AGE-SECRET-KEY-1", Pattern.compile("^AGE-SECRET-KEY-1[QPZRY9X8GF2TVDW0S3JN54KHCE6MUA7L]{58}$")),

    /** Human-Readable Part with Separator and enumeration of allowed Bech32 lowercase characters from BIP 0173 */
    PUBLIC_KEY("age1", Pattern.compile("^age1[qpzry9x8gf2tvdw0s3jn54khce6mua7l]{58}$"));

    private final String prefix;

    private final Pattern pattern;

    AgeKeyIndicator(final String prefix, final Pattern pattern) {
        this.prefix = prefix;
        this.pattern = pattern;
    }

    public String getPrefix() {
        return prefix;
    }

    public Pattern getPattern() {
        return pattern;
    }
}
