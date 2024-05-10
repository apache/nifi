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

public enum PaginationType implements DescribedValue {
    SCROLL("pagination-scroll", "Use Elasticsearch \"_scroll\" API to page results. Does not accept additional query parameters."),
    SEARCH_AFTER("pagination-search_after", "Use Elasticsearch \"search_after\" _search API to page sorted results."),
    POINT_IN_TIME("pagination-pit", "Use Elasticsearch (7.10+ with XPack) \"point in time\" _search API to page sorted results. " +
            "Not available for use with AWS OpenSearch.");

    private final String value;
    private final String description;

    PaginationType(final String value, final String description) {
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
}
