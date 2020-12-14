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
package org.apache.nifi.security.util;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Keystore Type enumeration of supported Keystore formats
 */
public enum KeystoreType {
    BCFKS("BCFKS", "Bouncy Castle FIPS Keystore"),
    PKCS12("PKCS12", "PKCS12 Keystore"),
    JKS("JKS", "Java Keystore");

    private final String type;
    private final String description;

    KeystoreType(String type, String description) {
        this.type = type;
        this.description = description;
    }

    public String getType() {
        return this.type;
    }

    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return getType();
    }

    public static boolean isValidKeystoreType(final String type) {
        if (type == null || type.replaceAll("\\s", "").isEmpty()) {
            return false;
        }
        return (Arrays.stream(values()).map(kt -> kt.getType().toLowerCase()).collect(Collectors.toList()).contains(type.toLowerCase()));
    }
}
