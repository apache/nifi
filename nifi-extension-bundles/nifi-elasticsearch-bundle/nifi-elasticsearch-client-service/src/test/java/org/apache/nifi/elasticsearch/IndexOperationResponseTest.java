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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IndexOperationResponseTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testTook() {
        long took = 100;
        final IndexOperationResponse response = new IndexOperationResponse(took);
        assertEquals(took, response.getTook());
    }

    @Test
    void testFromJson() throws Exception {
        final long took    = 100;
        final boolean errors = false;
        final List<Map<String, Object>> items = Collections.emptyList();

        final Map<String, Object> responseMap = Map.of(
                "took", Long.valueOf(took).intValue(),
                "errors", errors,
                "items", items
        );
        final String responseJson = OBJECT_MAPPER.writeValueAsString(responseMap);
        final IndexOperationResponse response = IndexOperationResponse.fromJsonResponse(responseJson);

        assertEquals(took, response.getTook());
        assertEquals(errors, response.hasErrors());
        assertEquals(items, response.getItems());
    }

    @Test
    void testLongTookTime() {
        final long took    = 34493262031L; // example "took" from Elasticsearch 8.15.0
        final IndexOperationResponse response = new IndexOperationResponse(took);
        assertEquals(took, response.getTook());
    }

    @Test
    void testFromJsonLongTook() throws Exception {
        final long took    = 34493262031L; // example "took" from Elasticsearch 8.15.0
        final boolean errors = true;
        final List<Map<String, Object>> items = Collections.singletonList(Collections.emptyMap());

        final Map<String, Object> responseMap = Map.of(
            "took", took,
            "errors", errors,
            "items", items
        );
        final String responseJson = OBJECT_MAPPER.writeValueAsString(responseMap);
        final IndexOperationResponse response = IndexOperationResponse.fromJsonResponse(responseJson);

        assertEquals(took, response.getTook());
        assertEquals(errors, response.hasErrors());
        assertEquals(items, response.getItems());
    }
}
