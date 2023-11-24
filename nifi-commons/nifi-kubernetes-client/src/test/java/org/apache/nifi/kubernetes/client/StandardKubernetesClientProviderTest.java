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

import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class StandardKubernetesClientProviderTest {
    private static final String DISABLE_AUTO_CONFIG_PROPERTY = "kubernetes.disable.autoConfig";

    StandardKubernetesClientProvider provider;

    @BeforeAll
    static void setDisableAutoConfig() {
        System.setProperty(DISABLE_AUTO_CONFIG_PROPERTY, Boolean.TRUE.toString());
    }

    @AfterAll
    static void clearDisableAutoConfig() {
        System.clearProperty(DISABLE_AUTO_CONFIG_PROPERTY);
    }

    @BeforeEach
    void setProvider() {
        provider = new StandardKubernetesClientProvider();
    }

    @Timeout(5)
    @Test
    void testGetKubernetesClient() {
        final KubernetesClient kubernetesClient = provider.getKubernetesClient();

        assertNotNull(kubernetesClient);
    }
}
