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
package org.apache.nifi.security.configuration;

import java.util.Objects;

/**
 * KeyStore Configuration properties
 */
public class KeyStoreConfiguration {
    private final String location;

    private final String password;

    private final String keyStoreType;

    public KeyStoreConfiguration(final String location, final String password, final String keyStoreType) {
        this.location = Objects.requireNonNull(location, "Location required");
        this.password = Objects.requireNonNull(password, "Password required");
        this.keyStoreType = Objects.requireNonNull(keyStoreType, "KeyStore Type required");
    }

    public String getLocation() {
        return location;
    }

    public String getPassword() {
        return password;
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }
}
