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
package org.apache.nifi.security.kms.configuration;

import org.apache.nifi.security.kms.FileBasedKeyProvider;

import javax.crypto.SecretKey;

/**
 * Configuration for File-based Key Provider
 */
public class FileBasedKeyProviderConfiguration implements KeyProviderConfiguration<FileBasedKeyProvider> {
    private final String location;

    private final SecretKey rootKey;

    public FileBasedKeyProviderConfiguration(final String location, final SecretKey rootKey) {
        this.location = location;
        this.rootKey = rootKey;
    }

    @Override
    public Class<FileBasedKeyProvider> getKeyProviderClass() {
        return FileBasedKeyProvider.class;
    }

    public String getLocation() {
        return location;
    }

    public SecretKey getRootKey() {
        return rootKey;
    }
}
