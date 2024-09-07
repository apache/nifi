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

package org.apache.nifi.elasticsearch;

import java.util.List;
import java.util.Map;

public class SearchResponse implements OperationResponse {
    private final List<Map<String, Object>> hits;
    private final Map<String, Object> aggregations;
    private final long numberOfHits;
    private final long took;
    private final boolean timedOut;
    private final String pitId;
    private final String scrollId;
    private final String searchAfter;
    private final List<String> warnings;

    public SearchResponse(final List<Map<String, Object>> hits, final Map<String, Object> aggregations, final String pitId,
                          final String scrollId, final String searchAfter, final int numberOfHits, final long took, final boolean timedOut,
                          final List<String> warnings) {
        this.hits = hits;
        this.aggregations = aggregations;
        this.pitId = pitId;
        this.scrollId = scrollId;
        this.numberOfHits = numberOfHits;
        this.took = took;
        this.timedOut = timedOut;
        this.searchAfter = searchAfter;
        this.warnings = warnings;
    }

    public Map<String, Object> getAggregations() {
        return aggregations;
    }

    public List<Map<String, Object>> getHits() {
        return hits;
    }

    public String getPitId() {
        return pitId;
    }

    public String getScrollId() {
        return scrollId;
    }

    public String getSearchAfter() {
        return searchAfter;
    }

    public long getNumberOfHits() {
        return numberOfHits;
    }

    public boolean isTimedOut() {
        return timedOut;
    }

    @Override
    public long getTook() {
        return took;
    }

    public List<String> getWarnings() {
        return this.warnings;
    }

    @Override
    public String toString() {
        return "SearchResponse{" +
                "hits=" + hits +
                ", aggregations=" + aggregations +
                ", numberOfHits=" + numberOfHits +
                ", took=" + took +
                ", timedOut=" + timedOut +
                ", pitId='" + pitId + '\'' +
                ", scrollId='" + scrollId + '\'' +
                ", searchAfter='" + searchAfter + '\'' +
                ", warnings='" + warnings + '\'' +
                '}';
    }
}
