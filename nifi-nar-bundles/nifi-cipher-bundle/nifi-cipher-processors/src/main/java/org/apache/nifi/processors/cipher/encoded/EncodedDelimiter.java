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
package org.apache.nifi.processors.cipher.encoded;

import org.apache.nifi.components.DescribedValue;

import java.nio.charset.StandardCharsets;

/**
 * Delimiters for content encoded according to conventions added in NiFi 0.5.0
 */
public enum EncodedDelimiter implements DescribedValue {
    SALT("NiFiSALT", "Eight bytes appended to a stream after the salt bytes according to NiFi 0.5.0 conventions"),
    IV("NiFiIV", "Six bytes appended to a stream after the initialization vector bytes according to NiFi 0.5.0 conventions");

    private final byte[] delimiter;
    private final String description;

    EncodedDelimiter(
            final String delimiterEncoded,
            final String description
    ) {
        this.delimiter = delimiterEncoded.getBytes(StandardCharsets.UTF_8);
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return name();
    }

    @Override
    public String getDescription() {
        return description;
    }

    public byte[] getDelimiter() {
        return delimiter;
    }
}
