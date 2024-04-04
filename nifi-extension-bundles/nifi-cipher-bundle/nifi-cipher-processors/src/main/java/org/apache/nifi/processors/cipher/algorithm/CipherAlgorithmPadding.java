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
package org.apache.nifi.processors.cipher.algorithm;

import org.apache.nifi.components.DescribedValue;

/**
 * Cipher Algorithm Paddings as enumerated in Java Cryptography Architecture Standard Algorithm Name Documentation
 */
public enum CipherAlgorithmPadding implements DescribedValue {
    /** No Padding */
    NO_PADDING("NoPadding"),
    /** PKCS5 Padding described in RFC 8018 */
    PKCS5_PADDING("PKCS5Padding");

    private final String padding;

    CipherAlgorithmPadding(final String padding) {
        this.padding = padding;
    }

    @Override
    public String getValue() {
        return padding;
    }

    @Override
    public String getDisplayName() {
        return padding;
    }

    @Override
    public String getDescription() {
        return padding;
    }
}
