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
package org.apache.nifi.ldap;

public enum ProviderProperty {
    KEYSTORE("TLS - Keystore"),

    KEYSTORE_PASSWORD("TLS - Keystore Password"),

    KEYSTORE_TYPE("TLS - Keystore Type"),

    TRUSTSTORE("TLS - Truststore"),

    TRUSTSTORE_PASSWORD("TLS - Truststore Password"),

    TRUSTSTORE_TYPE("TLS - Truststore Type"),

    TLS_PROTOCOL("TLS - Protocol");

    private final String property;

    ProviderProperty(final String property) {
        this.property = property;
    }

    public String getProperty() {
        return property;
    }
}
