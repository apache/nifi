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

package org.apache.nifi.processors.elasticsearch.api;

import org.apache.nifi.components.DescribedValue;

import java.util.Arrays;
import java.util.EnumSet;

public enum ResultOutputStrategy implements DescribedValue {
    PER_HIT("splitUp-yes", "Flowfile per hit."),
    PER_RESPONSE("splitUp-no", "Flowfile per response."),
    PER_QUERY("splitUp-query", "Combine results from all query responses (one flowfile per entire paginated result set of hits). " +
            "Note that aggregations cannot be paged, they are generated across the entire result set and " +
            "returned as part of the first page. Results are output with one JSON object per line " +
            "(allowing hits to be combined from multiple pages without loading all results into memory).");

    private final String value;
    private final String description;

    ResultOutputStrategy(final String value, final String description) {
        this.value = value;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return name();
    }

    @Override
    public String getDescription() {
        return description;
    }

    public static EnumSet<ResultOutputStrategy> getNonPaginatedResponseOutputStrategies() {
        return EnumSet.of(PER_RESPONSE, PER_HIT);
    }

    public static ResultOutputStrategy fromValue(final String value) {
        return Arrays.stream(ResultOutputStrategy.values()).filter(v -> v.getValue().equals(value)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("Unknown value %s", value)));
    }
}
