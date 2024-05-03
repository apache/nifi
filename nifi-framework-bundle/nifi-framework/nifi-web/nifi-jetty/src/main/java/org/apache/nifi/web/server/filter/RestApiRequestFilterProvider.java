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

import java.util.List;

/**
 * Request Filter Provider for REST API Web Application
 */
public class RestApiRequestFilterProvider extends StandardRequestFilterProvider {
    public static final String RELATIVE_PATH_ACCESS_TOKEN = "/access/token";

    private static final int DOS_FILTER_REJECT_REQUEST = -1;

    /**
     * Get Filters using provided NiFi Properties and append filters for Access Token Requests
     *
     * @param properties NiFi Properties required
     * @return List of Filter Holders
     */
    @Override
    public List<FilterHolder> getFilters(final NiFiProperties properties) {
        final List<FilterHolder> filters = super.getFilters(properties);

        final FilterHolder accessTokenDenialOfServiceFilter = getAccessTokenDenialOfServiceFilter(properties);
        filters.add(accessTokenDenialOfServiceFilter);

        return filters;
    }

    private FilterHolder getAccessTokenDenialOfServiceFilter(final NiFiProperties properties) {
        final FilterHolder filter = getDenialOfServiceFilter(properties, DoSFilter.class);

        final int maxWebAccessTokenRequestsPerSecond = properties.getMaxWebAccessTokenRequestsPerSecond();
        filter.setInitParameter("maxRequestsPerSec", Integer.toString(maxWebAccessTokenRequestsPerSecond));

        filter.setInitParameter("maxWaitMs", Integer.toString(DOS_FILTER_REJECT_REQUEST));
        filter.setInitParameter("delayMs", Integer.toString(DOS_FILTER_REJECT_REQUEST));

        filter.setInitParameter(FilterParameter.PATH_SPECIFICATION.name(), RELATIVE_PATH_ACCESS_TOKEN);
        filter.setName("AccessToken-DoSFilter");
        return filter;
    }
}
