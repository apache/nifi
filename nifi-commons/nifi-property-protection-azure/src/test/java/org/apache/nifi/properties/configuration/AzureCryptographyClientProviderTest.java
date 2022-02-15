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
package org.apache.nifi.properties.configuration;

import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class AzureCryptographyClientProviderTest {

    @Test
    public void testClientPropertiesNull() {
        final AzureCryptographyClientProvider provider = new AzureCryptographyClientProvider();
        final Optional<CryptographyClient> optionalClient = provider.getClient(null);
        assertFalse(optionalClient.isPresent());
    }

    @Test
    public void testClientPropertiesKeyIdBlank() {
        final AzureCryptographyClientProvider provider = new AzureCryptographyClientProvider();
        final Properties clientProperties = new Properties();
        clientProperties.setProperty(AzureCryptographyClientProvider.KEY_ID_PROPERTY, "");
        final Optional<CryptographyClient> optionalClient = provider.getClient(clientProperties);
        assertFalse(optionalClient.isPresent());
    }
}
