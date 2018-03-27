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

public class SearchResponse {
    private List<Map<String, Object>> hits;
    private Map<String, Object> aggregations;
    private long numberOfHits;
    private int took;
    private boolean timedOut;

    public SearchResponse(List<Map<String, Object>> hits, Map<String, Object> aggregations,
                          int numberOfHits, int took, boolean timedOut) {
        this.hits = hits;
        this.aggregations = aggregations;
        this.numberOfHits = numberOfHits;
        this.took = took;
        this.timedOut = timedOut;
    }

    public Map<String, Object> getAggregations() {
        return aggregations;
    }

    public List<Map<String, Object>> getHits() {
        return hits;
    }

    public long getNumberOfHits() {
        return numberOfHits;
    }

    public boolean isTimedOut() {
        return timedOut;
    }

    public int getTook() {
        return took;
    }

    @Override
    public String toString() {
        return "SearchResponse{" +
                "hits=" + hits +
                ", aggregations=" + aggregations +
                ", numberOfHits=" + numberOfHits +
                '}';
    }
}
