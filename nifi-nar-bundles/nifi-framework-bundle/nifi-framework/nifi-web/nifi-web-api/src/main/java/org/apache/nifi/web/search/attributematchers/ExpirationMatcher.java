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
package org.apache.nifi.web.search.attributematchers;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.web.search.query.SearchQuery;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ExpirationMatcher implements AttributeMatcher<Connection> {
    private static final String MATCH_PREFIX = "FlowFile expiration: ";
    private static final Set<String> KEYWORDS = new HashSet<>(Arrays.asList(
            "expires",
            "expiration"));

    @Override
    public void match(final Connection component, final SearchQuery query, final List<String> matches) {
        if (containsKeyword(query)) {
            final int expirationMillis = component.getFlowFileQueue().getFlowFileExpiration(TimeUnit.MILLISECONDS);

            if (expirationMillis > 0) {
                matches.add(MATCH_PREFIX + component.getFlowFileQueue().getFlowFileExpiration());
            }
        }
    }

    private boolean containsKeyword(final SearchQuery query) {
        return KEYWORDS.stream().anyMatch(keyword -> StringUtils.containsIgnoreCase(keyword, query.getTerm()));
    }
}
