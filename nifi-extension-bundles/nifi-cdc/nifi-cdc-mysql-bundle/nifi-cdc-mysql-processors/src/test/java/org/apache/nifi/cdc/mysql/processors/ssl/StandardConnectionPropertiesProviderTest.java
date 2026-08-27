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
package org.apache.nifi.cdc.mysql.processors.ssl;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class StandardConnectionPropertiesProviderTest {

    private static final String SSL_MODE_PROPERTY = SecurityProperty.SSL_MODE.getProperty();

    private static final String SSL_CONTEXT_PROVIDER_PROPERTY = SecurityProperty.SSL_CONTEXT_PROVIDER.getProperty();

    @Test
    void testGetConnectionPropertiesSslModeDisabled() {
        assertSslModeMapped(SSLMode.DISABLED);
    }

    @Test
    void testGetConnectionPropertiesSslModePreferred() {
        assertSslModeMapped(SSLMode.PREFERRED);
    }

    @Test
    void testGetConnectionPropertiesSslModeRequired() {
        assertSslModeMapped(SSLMode.REQUIRED);
    }

    @Test
    void testGetConnectionPropertiesSslModeVerifyCa() {
        assertSslModeMapped(SSLMode.VERIFY_CA);
    }

    @Test
    void testGetConnectionPropertiesSslModeVerifyIdentity() {
        assertSslModeMapped(SSLMode.VERIFY_IDENTITY);
    }

    private void assertSslModeMapped(final SSLMode sslMode) {
        final StandardConnectionPropertiesProvider provider = new StandardConnectionPropertiesProvider(sslMode);

        final Map<String, String> properties = provider.getConnectionProperties();

        assertNotNull(properties);
        assertEquals(sslMode.toString(), properties.get(SSL_MODE_PROPERTY));
        assertFalse(properties.containsKey(SSL_CONTEXT_PROVIDER_PROPERTY),
                "The SSLContext provider name is registered per connection attempt and must not be produced by the properties provider");
    }
}
