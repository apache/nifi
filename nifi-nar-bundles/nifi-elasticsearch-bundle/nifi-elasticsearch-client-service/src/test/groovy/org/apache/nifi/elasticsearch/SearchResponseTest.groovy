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

package org.apache.nifi.elasticsearch

import org.junit.jupiter.api.Test
import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class SearchResponseTest {
    @Test
    void test() {
        def results = []
        def aggs    = [:]
        def pitId = "pitId"
        def scrollId = "scrollId"
        def searchAfter = "searchAfter"
        def num     = 10
        def took    = 100
        def timeout = false
        def warnings = ["auth"]
        def response = new SearchResponse(results, aggs as Map<String, Object>, pitId, scrollId, searchAfter, num, took, timeout, warnings)
        def str = response.toString()

        assertEquals(results, response.hits)
        assertEquals(aggs, response.aggregations)
        assertEquals(pitId, response.pitId)
        assertEquals(scrollId, response.scrollId)
        assertEquals(num, response.numberOfHits)
        assertEquals(took, response.took)
        assertEquals(timeout, response.timedOut)
        assertEquals(warnings, response.warnings)
        assertTrue(str.contains("aggregations"))
        assertTrue(str.contains("hits"))
        assertTrue(str.contains("pitId"))
        assertTrue(str.contains("scrollId"))
        assertTrue(str.contains("searchAfter"))
        assertTrue(str.contains("numberOfHits"))
        assertTrue(str.contains("took"))
        assertTrue(str.contains("timedOut"))
        assertTrue(str.contains("warnings"))
    }
}
