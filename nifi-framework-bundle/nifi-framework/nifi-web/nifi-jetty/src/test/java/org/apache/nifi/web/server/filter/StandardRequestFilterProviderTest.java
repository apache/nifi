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
package org.apache.nifi.web.server.filter;

import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.requests.ContentLengthFilter;
import org.apache.nifi.web.server.log.RequestAuthenticationFilter;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import jakarta.servlet.Filter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StandardRequestFilterProviderTest {
    private static final String HTTPS_PORT = "8443";

    private static final String MAX_CONTENT_SIZE = "1 MB";

    private StandardRequestFilterProvider provider;

    @BeforeEach
    public void setProvider() {
        provider = new StandardRequestFilterProvider();
    }

    @Test
    public void testGetFilters() {
        final NiFiProperties properties = getProperties(Collections.emptyMap());
        final List<FilterHolder> filters = provider.getFilters(properties);

        assertStandardFiltersFound(filters);
    }

    @Test
    public void testGetFiltersContentLengthEnabled() {
        final Map<String, String> configurationProperties = new LinkedHashMap<>();
        configurationProperties.put(NiFiProperties.WEB_MAX_CONTENT_SIZE, MAX_CONTENT_SIZE);

        final NiFiProperties properties = getProperties(configurationProperties);
        final List<FilterHolder> filters = provider.getFilters(properties);

        assertStandardFiltersFound(filters);

        assertFilterClassFound(filters, ContentLengthFilter.class);
    }

    @Test
    public void testGetFiltersHttpsEnabled() {
        final Map<String, String> configurationProperties = new LinkedHashMap<>();
        configurationProperties.put(NiFiProperties.WEB_HTTPS_PORT, HTTPS_PORT);

        final NiFiProperties properties = getProperties(configurationProperties);
        final List<FilterHolder> filters = provider.getFilters(properties);

        assertStandardFiltersFound(filters);

        assertFilterClassFound(filters, RequestAuthenticationFilter.class);

        final FilterHolder firstFilterHolder = filters.getFirst();
        final Class<? extends Filter> firstFilterClass = firstFilterHolder.getHeldClass();
        assertEquals(RequestAuthenticationFilter.class, firstFilterClass);
    }

    private void assertStandardFiltersFound(final List<FilterHolder> filters) {
        assertNotNull(filters);
        assertFalse(filters.isEmpty());

        assertFilterClassFound(filters, DataTransferExcludedDoSFilter.class);
    }

    private void assertFilterClassFound(final List<FilterHolder> filters, final Class<? extends Filter> filterClass) {
        final Optional<FilterHolder> filterHolder = filters.stream().filter(filter -> filterClass.equals(filter.getHeldClass())).findFirst();
        assertTrue(filterHolder.isPresent(), String.format("Filter Class [%s] not found", filterClass));
    }

    private NiFiProperties getProperties(final Map<String, String> properties) {
        return NiFiProperties.createBasicNiFiProperties(null, properties);
    }
}
