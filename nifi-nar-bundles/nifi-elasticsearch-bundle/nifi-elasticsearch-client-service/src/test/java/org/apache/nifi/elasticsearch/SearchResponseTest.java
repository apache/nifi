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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SearchResponseTest {
    @Test
    void test() {
        List<Map<String, Object>> results = new ArrayList<>();
        Map<String, Object> aggs    = new HashMap<>();
        String pitId = "pitId";
        String scrollId = "scrollId";
        String searchAfter = "searchAfter";
        int num     = 10;
        int took    = 100;
        boolean timeout = false;
        List<String> warnings = Arrays.asList("auth");
        SearchResponse response = new SearchResponse(results, aggs, pitId, scrollId, searchAfter, num, took, timeout, warnings);
        String str = response.toString();

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
}
