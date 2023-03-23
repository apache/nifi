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
package org.apache.nifi.kubernetes.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ServiceAccountNamespaceProviderTest {
    ServiceAccountNamespaceProvider provider;

    @BeforeEach
    void setProvider() {
        provider = new ServiceAccountNamespaceProvider();
    }

    @Test
    void testGetNamespace() {
        final String namespace = provider.getNamespace();

        final Path namespacePath = Paths.get(ServiceAccountNamespaceProvider.NAMESPACE_PATH);
        if (Files.isReadable(namespacePath)) {
            assertNotNull(namespace);
        } else {
            assertEquals(ServiceAccountNamespaceProvider.DEFAULT_NAMESPACE, namespace);
        }
    }
}
