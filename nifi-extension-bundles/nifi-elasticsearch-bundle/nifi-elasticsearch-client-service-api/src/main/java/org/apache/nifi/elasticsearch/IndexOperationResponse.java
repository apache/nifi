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

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IndexOperationResponse implements OperationResponse {
    private final long took;
    private boolean hasErrors;
    private List<Map<String, Object>> items;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public IndexOperationResponse(final long took) {
        this.took = took;
    }

    @Override
    public long getTook() {
        return took;
    }

    public boolean hasErrors() {
        return hasErrors;
    }

    @SuppressWarnings("unchecked")
    public static IndexOperationResponse fromJsonResponse(final String response) throws IOException {
        final Map<String, Object> parsedResponse = OBJECT_MAPPER.readValue(response, Map.class);
        // took should be an int, but could be a long (bg in Elasticsearch 8.15.0)
        final long took = Long.parseLong(String.valueOf(parsedResponse.get("took")));
        final boolean hasErrors = (boolean) parsedResponse.get("errors");
        final List<Map<String, Object>> items = (List<Map<String, Object>>) parsedResponse.get("items");

        final IndexOperationResponse retVal = new IndexOperationResponse(took);
        retVal.hasErrors = hasErrors;
        retVal.items = items;

        return retVal;
    }

    public List<Map<String, Object>> getItems() {
        return items;
    }

    @Override
    public String toString() {
        return "IndexOperationResponse{" +
                "took=" + took +
                ", hasErrors=" + hasErrors +
                ", items=" + items +
                '}';
    }
}
