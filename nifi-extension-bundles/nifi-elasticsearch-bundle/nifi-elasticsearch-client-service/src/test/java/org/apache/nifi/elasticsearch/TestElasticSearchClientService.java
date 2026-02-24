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

package org.apache.nifi.elasticsearch;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestElasticSearchClientService extends AbstractControllerService implements ElasticSearchClientService {
    private final Map<String, Object> data;

    public TestElasticSearchClientService() {
        data = new HashMap<>(4, 1);
        data.put("username", "john.smith");
        data.put("password", "testing1234");
        data.put("email", "john.smith@test.com");
        data.put("position", "Software Engineer");
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger, final Map<String, String> variables) {
        return null;
    }

    @Override
    public IndexOperationResponse add(final IndexOperationRequest operation, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public IndexOperationResponse bulk(final List<IndexOperationRequest> operations, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public Long count(final String query, final String index, final String type, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public DeleteOperationResponse deleteById(final String index, final String type, final String id, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public DeleteOperationResponse deleteById(final String index, final String type, final List<String> ids, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public DeleteOperationResponse deleteByQuery(final String query, final String index, final String type, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public UpdateOperationResponse updateByQuery(final String query, final String index, final String type, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public void refresh(final String index, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        // intentionally blank
    }

    @Override
    public boolean exists(final String index, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return true;
    }

    @Override
    public boolean documentExists(final String index, final String type, final String id, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return true;
    }

    @Override
    public Map<String, Object> get(final String index, final String type, final String id, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return data;
    }

    @Override
    public SearchResponse search(final String query, final String index, final String type, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        final List<Map<String, Object>> hits = new ArrayList<>();
        final Map<String, Object> source = new HashMap<>();
        source.put("_source", data);
        hits.add(source);
        return new SearchResponse(hits, null, null, null, null,
                1, 100, false, null);
    }

    @Override
    public SearchResponse scroll(final String scroll, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return search(null, null, null, elasticsearchRequestOptions);
    }

    @Override
    public String initialisePointInTime(final String index, final String keepAlive, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public DeleteOperationResponse deletePointInTime(final String pitId, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public DeleteOperationResponse deleteScroll(final String scrollId, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public String getTransitUrl(final String index, final String type) {
        return "";
    }

    public Map<String, Object> getData() {
        return data;
    }
}
