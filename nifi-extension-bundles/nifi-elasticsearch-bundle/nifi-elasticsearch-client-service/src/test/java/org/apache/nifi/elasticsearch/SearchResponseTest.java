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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SearchResponseTest {
    @Test
    void test() {
        final List<Map<String, Object>> results = new ArrayList<>();
        final Map<String, Object> aggs    = new HashMap<>();
        final String pitId = "pitId";
        final String scrollId = "scrollId";
        final String searchAfter = "searchAfter";
        final int num     = 10;
        final long took    = 100;
        final boolean timeout = false;
        final List<String> warnings = Collections.singletonList("auth");
        final SearchResponse response = new SearchResponse(results, aggs, pitId, scrollId, searchAfter, num, took, timeout, warnings);
        final String str = response.toString();

        assertEquals(results, response.getHits());
        assertEquals(aggs, response.getAggregations());
        assertEquals(pitId, response.getPitId());
        assertEquals(scrollId, response.getScrollId());
        assertEquals(num, response.getNumberOfHits());
        assertEquals(took, response.getTook());
        assertEquals(timeout, response.isTimedOut());
        assertEquals(warnings, response.getWarnings());
        assertTrue(str.contains("aggregations"));
        assertTrue(str.contains("hits"));
        assertTrue(str.contains("pitId"));
        assertTrue(str.contains("scrollId"));
        assertTrue(str.contains("searchAfter"));
        assertTrue(str.contains("numberOfHits"));
        assertTrue(str.contains("took"));
        assertTrue(str.contains("timedOut"));
        assertTrue(str.contains("warnings"));
    }

    @Test
    void testLongTookTime() {
        final List<Map<String, Object>> results = new ArrayList<>();
        final Map<String, Object> aggs    = new HashMap<>();
        final String pitId = "pitId";
        final String scrollId = "scrollId";
        final String searchAfter = "searchAfter";
        final int num     = 10;
        final long took    = 34493262031L; // example "took" from Elasticsearch 8.15.0
        final boolean timeout = false;
        final List<String> warnings = Collections.singletonList("auth");
        final SearchResponse response = new SearchResponse(results, aggs, pitId, scrollId, searchAfter, num, took, timeout, warnings);

        assertEquals(took, response.getTook());
    }
}
