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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.requests.ContentLengthFilter;
import org.apache.nifi.web.server.log.RequestAuthenticationFilter;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlets.DoSFilter;

import jakarta.servlet.Filter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Standard implementation of Request Filter Provider
 */
public class StandardRequestFilterProvider implements RequestFilterProvider {
    private static final int MAX_CONTENT_SIZE_DISABLED = 0;

    /**
     * Get Filters using provided NiFi Properties
     *
     * @param properties NiFi Properties required
     * @return List of Filter Holders
     */
    @Override
    public List<FilterHolder> getFilters(final NiFiProperties properties) {
        Objects.requireNonNull(properties, "Properties required");

        final List<FilterHolder> filters = new ArrayList<>();

        if (properties.isHTTPSConfigured()) {
            filters.add(getFilterHolder(RequestAuthenticationFilter.class));
        }

        final int maxContentSize = getMaxContentSize(properties);
        if (maxContentSize > MAX_CONTENT_SIZE_DISABLED) {
            final FilterHolder contentLengthFilter = getContentLengthFilter(maxContentSize);
            filters.add(contentLengthFilter);
        }

        final FilterHolder denialOfServiceFilter = getDenialOfServiceFilter(properties, DataTransferExcludedDoSFilter.class);
        filters.add(denialOfServiceFilter);

        return filters;
    }

    protected FilterHolder getDenialOfServiceFilter(final NiFiProperties properties, final Class<? extends DoSFilter> filterClass) {
        final FilterHolder filter = new FilterHolder(filterClass);

        final int maxWebRequestsPerSecond = properties.getMaxWebRequestsPerSecond();
        filter.setInitParameter("maxRequestsPerSec", Integer.toString(maxWebRequestsPerSecond));

        final long webRequestTimeout = getWebRequestTimeout(properties);
        filter.setInitParameter("maxRequestMs", Long.toString(webRequestTimeout));

        final String webRequestIpWhitelist = properties.getWebRequestIpWhitelist();
        filter.setInitParameter("ipWhitelist", webRequestIpWhitelist);

        filter.setName(DoSFilter.class.getSimpleName());
        return filter;
    }

    private FilterHolder getFilterHolder(final Class<? extends Filter> filterClass) {
        final FilterHolder filter = new FilterHolder(filterClass);
        filter.setName(filterClass.getSimpleName());
        return filter;
    }

    private FilterHolder getContentLengthFilter(final int maxContentSize) {
        final FilterHolder filter = getFilterHolder(ContentLengthFilter.class);
        filter.setInitParameter(ContentLengthFilter.MAX_LENGTH_INIT_PARAM, Integer.toString(maxContentSize));
        filter.setName(ContentLengthFilter.class.getSimpleName());
        return filter;
    }

    private int getMaxContentSize(final NiFiProperties properties) {
        final String webMaxContentSize = properties.getWebMaxContentSize();
        try {
            return StringUtils.isBlank(webMaxContentSize) ? MAX_CONTENT_SIZE_DISABLED : DataUnit.parseDataSize(webMaxContentSize, DataUnit.B).intValue();
        } catch (final IllegalArgumentException e) {
            throw new IllegalStateException(String.format("Property [%s] format invalid", NiFiProperties.WEB_MAX_CONTENT_SIZE), e);
        }
    }

    protected long getWebRequestTimeout(final NiFiProperties properties) {
        final String webRequestTimeout = properties.getWebRequestTimeout();

        try {
            final double webRequestTimeoutParsed = FormatUtils.getPreciseTimeDuration(webRequestTimeout, TimeUnit.MILLISECONDS);
            return Math.round(webRequestTimeoutParsed);
        } catch (final NumberFormatException e) {
            throw new IllegalStateException(String.format("Property [%s] format invalid", NiFiProperties.WEB_REQUEST_TIMEOUT), e);
        }
    }
}
