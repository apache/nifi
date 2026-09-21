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
package org.apache.nifi.services.iceberg.catalog;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class StandardRESTClientProviderTest {

    private static final String CATALOG_URI = "https://localhost";

    private static final String CLIENT_CREDENTIALS = "CLIENT_ID:CLIENT_SECRET";

    private final StandardRESTClientProvider provider = new StandardRESTClientProvider();

    @Test
    void testBuild() {
        final Map<String, String> properties = Map.of(CatalogProperties.URI, CATALOG_URI);

        final RESTClient restClient = provider.build(properties);

        assertNotNull(restClient);
        assertInstanceOf(HTTPClient.class, restClient);
    }

    @Test
    void testBuildAccessDelegationHeader() {
        final Map<String, String> properties = Map.of(
                CatalogProperties.URI, CATALOG_URI,
                StandardRESTClientProvider.ICEBERG_ACCESS_DELEGATION_HEADER, AccessDelegationStrategy.VENDED_CREDENTIALS.getValue()
        );

        final RESTClient restClient = provider.build(properties);

        assertNotNull(restClient);
        assertInstanceOf(HTTPClient.class, restClient);
    }

    @Test
    void testBuildClientCredentials() {
        final Map<String, String> properties = Map.of(
                CatalogProperties.URI, CATALOG_URI,
                OAuth2Properties.CREDENTIAL, CLIENT_CREDENTIALS
        );

        final RESTClient restClient = provider.build(properties);

        assertInstanceOf(HTTPClient.class, restClient);
    }
}
