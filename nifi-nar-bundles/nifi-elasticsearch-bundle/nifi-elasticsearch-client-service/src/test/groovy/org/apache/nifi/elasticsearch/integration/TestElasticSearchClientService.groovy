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
import org.apache.nifi.elasticsearch.UpdateOperationResponse

class TestElasticSearchClientService extends AbstractControllerService implements ElasticSearchClientService {
    Map data = [
        "username": "john.smith",
        "password": "testing1234",
        "email": "john.smith@test.com",
        "position": "Software Engineer"
    ]

    @Override
    IndexOperationResponse add(IndexOperationRequest operation, Map<String, String> requestParameters) {
        return null
    }

    @Override
    IndexOperationResponse bulk(List<IndexOperationRequest> operations, Map<String, String> requestParameters) {
        return null
    }

    @Override
    Long count(String query, String index, String type, Map<String, String> requestParameters) {
        return null
    }

    @Override
    DeleteOperationResponse deleteById(String index, String type, String id, Map<String, String> requestParameters) {
        return null
    }

    @Override
    DeleteOperationResponse deleteById(String index, String type, List<String> ids, Map<String, String> requestParameters) {
        return null
    }

    @Override
    DeleteOperationResponse deleteByQuery(String query, String index, String type, Map<String, String> requestParameters) {
        return null
    }

    @Override
    UpdateOperationResponse updateByQuery(String query, String index, String type, Map<String, String> requestParameters) {
        return null
    }

    @Override
    void refresh(final String index, final Map<String, String> requestParameters) {
    }

    @Override
    Map<String, Object> get(String index, String type, String id, Map<String, String> requestParameters) {
        return data
    }

    @Override
    SearchResponse search(String query, String index, String type, Map<String, String> requestParameters) {
        List hits = [[
            "_source": data
        ]]
        return new SearchResponse(hits, null, null, null, null, 1, 100, false, null)
    }

    @Override
    SearchResponse scroll(String scroll) {
        return search(null, null, null, null)
    }

    @Override
    String initialisePointInTime(String index, String keepAlive) {
        return null
    }

    @Override
    DeleteOperationResponse deletePointInTime(String pitId) {
        return null
    }

    @Override
    DeleteOperationResponse deleteScroll(String scrollId) {
        return null
    }

    @Override
    String getTransitUrl(String index, String type) {
        return ""
    }
}
