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

import org.junit.Assert
import org.junit.Test

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

        Assert.assertEquals(results, response.hits)
        Assert.assertEquals(aggs, response.aggregations)
        Assert.assertEquals(pitId, response.pitId)
        Assert.assertEquals(scrollId, response.scrollId)
        Assert.assertEquals(num, response.numberOfHits)
        Assert.assertEquals(took, response.took)
        Assert.assertEquals(timeout, response.timedOut)
        Assert.assertEquals(warnings, response.warnings)
        Assert.assertTrue(str.contains("aggregations"))
        Assert.assertTrue(str.contains("hits"))
        Assert.assertTrue(str.contains("pitId"))
        Assert.assertTrue(str.contains("scrollId"))
        Assert.assertTrue(str.contains("searchAfter"))
        Assert.assertTrue(str.contains("numberOfHits"))
        Assert.assertTrue(str.contains("took"))
        Assert.assertTrue(str.contains("timedOut"))
        Assert.assertTrue(str.contains("warnings"))
    }
}
