/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

package org.apache.nifi.elasticsearch.integration

import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.elasticsearch.DeleteOperationResponse
import org.apache.nifi.elasticsearch.ElasticSearchClientService
import org.apache.nifi.elasticsearch.IndexOperationRequest
import org.apache.nifi.elasticsearch.IndexOperationResponse
import org.apache.nifi.elasticsearch.SearchResponse

class TestElasticSearchClientService extends AbstractControllerService implements ElasticSearchClientService {
    @Override
    IndexOperationResponse add(IndexOperationRequest operation) throws IOException {
        return null
    }

    @Override
    IndexOperationResponse add(List<IndexOperationRequest> operations) throws IOException {
        return null
    }

    @Override
    DeleteOperationResponse deleteById(String index, String type, String id) throws IOException {
        return null
    }

    @Override
    DeleteOperationResponse deleteById(String index, String type, List<String> ids) throws IOException {
        return null
    }

    @Override
    DeleteOperationResponse deleteByQuery(String query, String index, String type) throws IOException {
        return null
    }

    @Override
    Map<String, Object> get(String index, String type, String id) throws IOException {
        return null
    }

    @Override
    SearchResponse search(String query, String index, String type) throws IOException {
        List hits = [[
            "_source": [
                "username": "john.smith",
                "password": "testing1234",
                "email": "john.smith@test.com",
                "position": "Software Engineer"
            ]
        ]]
        return new SearchResponse(hits, null, 1, 100, false)
    }

    @Override
    String getTransitUrl(String index, String type) {
        return ""
    }
}
