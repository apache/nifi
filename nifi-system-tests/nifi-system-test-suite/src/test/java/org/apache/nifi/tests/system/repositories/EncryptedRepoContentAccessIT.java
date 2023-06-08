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

package org.apache.nifi.tests.system.repositories;

import java.util.HashMap;
import java.util.Map;

public class EncryptedRepoContentAccessIT extends ContentAccessIT {
    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        final Map<String, String> encryptedRepoProperties = new HashMap<>();
        encryptedRepoProperties.put("nifi.content.repository.implementation", "org.apache.nifi.controller.repository.crypto.EncryptedFileSystemRepository");
        encryptedRepoProperties.put("nifi.content.repository.encryption.key", "0123456789ABCDEFFEDCBA9876543210");
        encryptedRepoProperties.put("nifi.content.repository.encryption.key.id", "k1");
        encryptedRepoProperties.put("nifi.content.repository.encryption.key.provider.implementation", "StaticKeyProvider");
        return encryptedRepoProperties;
    }
}
