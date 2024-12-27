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
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlets.DoSFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import jakarta.servlet.Filter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RestApiRequestFilterProviderTest {
    private RestApiRequestFilterProvider provider;

    @BeforeEach
    public void setProvider() {
        provider = new RestApiRequestFilterProvider();
    }

    @Test
    public void testGetFilters() {
        final NiFiProperties properties = getProperties(Collections.emptyMap());
        final List<FilterHolder> filters = provider.getFilters(properties);

        final Optional<FilterHolder> filterHolder = filters.stream().filter(filter -> filter.getInitParameter(FilterParameter.PATH_SPECIFICATION.name()) != null).findFirst();
        assertTrue(filterHolder.isPresent());

        final FilterHolder holder = filterHolder.get();
        assertEquals(DoSFilter.class, holder.getHeldClass());

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
