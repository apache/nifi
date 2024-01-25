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
 * Digest Algorithm as enumerated in Java Cryptography Architecture Standard Algorithm Name Documentation
 */
public enum DigestAlgorithm implements DescribedValue {
    MD5("MD5", "Message Digest Algorithm 5"),
    SHA1("SHA-1", "Secure Hash Algorithm 1"),
    SHA256("SHA-256", "Secure Hash Algorithm 2 with 256 bits");

    private final String name;
    private final String description;

    DigestAlgorithm(final String name, final String description) {
        this.name = name;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name;
    }

    @Override
    public String getDisplayName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
