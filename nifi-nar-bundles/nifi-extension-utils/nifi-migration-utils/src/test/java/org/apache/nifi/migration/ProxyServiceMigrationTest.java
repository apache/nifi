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
package org.apache.nifi.migration;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.MockPropertyConfiguration.CreatedControllerService;
import org.apache.nifi.util.PropertyMigrationResult;
import org.junit.jupiter.api.Test;

import java.net.Proxy;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProxyServiceMigrationTest {

    private static final PropertyDescriptor PROXY_SERVICE = new PropertyDescriptor.Builder()
            .name("proxy-service")
            .build();

    private static final String OBSOLETE_PROXY_HOST = "proxy-host";
    private static final String OBSOLETE_PROXY_PORT = "proxy-port";
    private static final String OBSOLETE_PROXY_USERNAME = "proxy-username";
    private static final String OBSOLETE_PROXY_PASSWORD = "proxy-password";

    private static final String PROXY_HOST_VALUE = "localhost";
    private static final String PROXY_PORT_VALUE = "8888";
    private static final String PROXY_USERNAME_VALUE = "user";
    private static final String PROXY_PASSWORD_VALUE = "pass";

    @Test
    void testMigrateProxyProperties() {
        final Map<String, String> properties = Map.of(
                OBSOLETE_PROXY_HOST, PROXY_HOST_VALUE,
                OBSOLETE_PROXY_PORT, PROXY_PORT_VALUE,
                OBSOLETE_PROXY_USERNAME, PROXY_USERNAME_VALUE,
                OBSOLETE_PROXY_PASSWORD, PROXY_PASSWORD_VALUE
        );
        final MockPropertyConfiguration config = new MockPropertyConfiguration(properties);

        ProxyServiceMigration.migrateProxyProperties(config, PROXY_SERVICE, OBSOLETE_PROXY_HOST, OBSOLETE_PROXY_PORT, OBSOLETE_PROXY_USERNAME, OBSOLETE_PROXY_PASSWORD);

        assertFalse(config.hasProperty(OBSOLETE_PROXY_HOST));
        assertFalse(config.hasProperty(OBSOLETE_PROXY_PORT));
        assertFalse(config.hasProperty(OBSOLETE_PROXY_USERNAME));
        assertFalse(config.hasProperty(OBSOLETE_PROXY_PASSWORD));

        assertTrue(config.isPropertySet(PROXY_SERVICE));

        PropertyMigrationResult result = config.toPropertyMigrationResult();
        assertEquals(1, result.getCreatedControllerServices().size());

        final CreatedControllerService createdService = result.getCreatedControllerServices().iterator().next();

        assertEquals(config.getRawPropertyValue(PROXY_SERVICE).get(), createdService.id());
        assertEquals(ProxyServiceMigration.PROXY_SERVICE_CLASSNAME, createdService.implementationClassName());

        assertEquals(Map.of(
                        ProxyServiceMigration.PROXY_SERVICE_TYPE, Proxy.Type.HTTP.name(),
                        ProxyServiceMigration.PROXY_SERVICE_HOST, PROXY_HOST_VALUE,
                        ProxyServiceMigration.PROXY_SERVICE_PORT, PROXY_PORT_VALUE,
                        ProxyServiceMigration.PROXY_SERVICE_USERNAME, PROXY_USERNAME_VALUE,
                        ProxyServiceMigration.PROXY_SERVICE_PASSWORD, PROXY_PASSWORD_VALUE
                ),
                createdService.serviceProperties());
    }
}
