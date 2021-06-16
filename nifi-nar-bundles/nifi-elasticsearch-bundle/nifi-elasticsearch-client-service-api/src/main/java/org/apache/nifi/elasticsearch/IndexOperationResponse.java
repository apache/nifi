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

public class IndexOperationResponse {
    private long took;
    private boolean hasErrors;
    private List<Map<String, Object>> items;

    public IndexOperationResponse(long took) {
        this.took = took;
    }

    public long getTook() {
        return took;
    }

    public boolean hasErrors() {
        return hasErrors;
    }

    public static IndexOperationResponse fromJsonResponse(String response) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> parsedResponse = mapper.readValue(response, Map.class);
        int took = (int) parsedResponse.get("took");
        boolean hasErrors = (boolean) parsedResponse.get("errors");
        List<Map<String, Object>> items = (List<Map<String, Object>>)parsedResponse.get("items");

        IndexOperationResponse retVal = new IndexOperationResponse(took);
        retVal.hasErrors = hasErrors;
        retVal.items = items;

        return retVal;
    }

    public List<Map<String, Object>> getItems() {
        return items;
    }
}
