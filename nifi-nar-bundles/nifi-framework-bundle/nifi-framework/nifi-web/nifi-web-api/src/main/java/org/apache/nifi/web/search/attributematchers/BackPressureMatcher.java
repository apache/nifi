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
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.web.search.query.SearchQuery;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BackPressureMatcher implements AttributeMatcher<Connection> {
    private static final String MATCH_PREFIX_SIZE = "Back pressure data size: ";
    private static final String MATCH_PREFIX_COUNT = "Back pressure count: ";
    private static final Set<String> KEYWORDS = new HashSet<>(Arrays.asList(
            "back pressure",
            "pressure"));

    @Override
    public void match(final Connection component, final SearchQuery query, final List<String> matches) {
        if (containsKeyword(query)) {
            final String backPressureDataSize = component.getFlowFileQueue().getBackPressureDataSizeThreshold();
            final Double backPressureBytes = DataUnit.parseDataSize(backPressureDataSize, DataUnit.B);
            final long backPressureCount = component.getFlowFileQueue().getBackPressureObjectThreshold();

            if (backPressureBytes > 0) {
                matches.add(MATCH_PREFIX_SIZE + backPressureDataSize);
            }

            if (backPressureCount > 0) {
                matches.add(MATCH_PREFIX_COUNT + backPressureCount);
            }
        }
    }

    private boolean containsKeyword(final SearchQuery query) {
        return KEYWORDS.stream().anyMatch(keyword -> StringUtils.containsIgnoreCase(keyword, query.getTerm()));
    }
}
