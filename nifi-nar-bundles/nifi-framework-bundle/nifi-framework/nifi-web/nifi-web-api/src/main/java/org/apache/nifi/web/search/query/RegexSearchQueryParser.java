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
package org.apache.nifi.web.search.query;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.groups.ProcessGroup;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class RegexSearchQueryParser implements SearchQueryParser {
    private static final String REGEX = "(?<filter>(\\w+:[\\w-]+\\s+)*(\\w+:[\\w-]+)?)(?<term>.*)";
    private static final String FILTER_TOKEN_SEPARATOR = "\\:";
    private static final String FILTER_SEPARATOR = "[\\s]+";
    private static final String FILTER_GROUP = "filter";
    private static final String TERM_GROUP = "term";

    private final Pattern pattern;

    public RegexSearchQueryParser() {
        this.pattern = Pattern.compile(REGEX);
    }

    @Override
    public SearchQuery parse(final String searchLiteral, final NiFiUser user, final ProcessGroup rootGroup, final ProcessGroup activeGroup) {
        final Matcher matcher = pattern.matcher(searchLiteral);
        if (matcher.matches()) {
            final String term = matcher.group(TERM_GROUP);
            final String filters = matcher.group(FILTER_GROUP);
            return new MapBasedSearchQuery(term, processFilters(filters), user, rootGroup, activeGroup);
        } else {
            return new MapBasedSearchQuery(searchLiteral, Collections.emptyMap(), user, rootGroup, activeGroup);
        }
    }

    private Map<String, String> processFilters(final String filters) {
        return Arrays.stream(filters.split(FILTER_SEPARATOR))
                .map(token -> token.split(FILTER_TOKEN_SEPARATOR))
                .filter(filter -> filter.length == 2)
                .collect(Collectors.toMap(filter -> filter[0].trim(), filter -> filter[1].trim(), (first, second) -> first));
    }
}
