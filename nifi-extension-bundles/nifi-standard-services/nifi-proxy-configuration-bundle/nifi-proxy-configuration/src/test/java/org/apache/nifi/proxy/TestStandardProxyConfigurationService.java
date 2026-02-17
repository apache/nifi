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
package org.apache.nifi.proxy;

import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.PropertyMigrationResult;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestStandardProxyConfigurationService {

    @Test
    void testMigrateProperties() {
        final StandardProxyConfigurationService service = new StandardProxyConfigurationService();
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("proxy-type", StandardProxyConfigurationService.PROXY_TYPE.getName()),
                Map.entry("socks-version", StandardProxyConfigurationService.SOCKS_VERSION.getName()),
                Map.entry("proxy-server-host", StandardProxyConfigurationService.PROXY_SERVER_HOST.getName()),
                Map.entry("proxy-server-port", StandardProxyConfigurationService.PROXY_SERVER_PORT.getName()),
                Map.entry("proxy-user-name", StandardProxyConfigurationService.PROXY_USER_NAME.getName()),
                Map.entry("proxy-user-password", StandardProxyConfigurationService.PROXY_USER_PASSWORD.getName()),
                Map.entry(ProxyConfigurationService.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE.getName())
        );

        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        service.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expectedRenamed, propertiesRenamed);
    }
}
