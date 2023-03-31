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

package org.apache.nifi.processors.elasticsearch

import groovy.json.JsonSlurper
import org.apache.nifi.components.ConfigVerificationResult
import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.controller.ConfigurationContext
import org.apache.nifi.elasticsearch.DeleteOperationResponse
import org.apache.nifi.elasticsearch.ElasticSearchClientService
import org.apache.nifi.elasticsearch.IndexOperationRequest
import org.apache.nifi.elasticsearch.IndexOperationResponse
import org.apache.nifi.elasticsearch.SearchResponse
import org.apache.nifi.elasticsearch.UpdateOperationResponse
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processors.elasticsearch.mock.MockElasticsearchException

class TestElasticsearchClientService extends AbstractControllerService implements ElasticSearchClientService {
    private boolean returnAggs
    private boolean throwErrorInSearch
    private boolean throwErrorInGet
    private boolean throwNotFoundInGet
    private boolean throwErrorInDelete
    private boolean throwErrorInPit
    private boolean throwErrorInUpdate
    private int pageCount = 0
    private int maxPages = 1
    private String query
    private Map<String, String> requestParameters

    TestElasticsearchClientService(boolean returnAggs) {
        this.returnAggs = returnAggs
    }

    private void common(boolean throwError, Map<String, String> requestParameters) {
        if (throwError) {
            if (throwNotFoundInGet) {
                throw new MockElasticsearchException(false, true)
            } else {
                throw new IOException("Simulated IOException")
            }
        }
        this.requestParameters = requestParameters
    }

    @Override
    List<ConfigVerificationResult> verify(ConfigurationContext context, ComponentLog verificationLogger, Map<String, String> variables) {
        return null
    }

    @Override
    IndexOperationResponse add(IndexOperationRequest operation, Map<String, String> requestParameters) {
        return bulk(Arrays.asList(operation) as List<IndexOperationRequest>, requestParameters)
    }

    @Override
    IndexOperationResponse bulk(List<IndexOperationRequest> operations, Map<String, String> requestParameters) {
        common(false, requestParameters)
        return new IndexOperationResponse(100L)
    }

    @Override
    Long count(String query, String index, String type, Map<String, String> requestParameters) {
        common(false, requestParameters)
        return null
    }

    @Override
    DeleteOperationResponse deleteById(String index, String type, String id, Map<String, String> requestParameters) {
        return deleteById(index, type, Arrays.asList(id), requestParameters)
    }

    @Override
    DeleteOperationResponse deleteById(String index, String type, List<String> ids, Map<String, String> requestParameters) {
        common(throwErrorInDelete, requestParameters)
        return new DeleteOperationResponse(100L)
    }

    @Override
    DeleteOperationResponse deleteByQuery(String query, String index, String type, Map<String, String> requestParameters) {
        return deleteById(index, type, Arrays.asList("1"), requestParameters)
    }

    @Override
    UpdateOperationResponse updateByQuery(String query, String index, String type, Map<String, String> requestParameters) {
        common(throwErrorInUpdate, requestParameters)
        return new UpdateOperationResponse(100L)
    }

    @Override
    void refresh(final String index, final Map<String, String> requestParameters) {
    }

    @Override
    boolean exists(final String index, final Map<String, String> requestParameters) {
        return true
    }

    @Override
    boolean documentExists(String index, String type, String id, Map<String, String> requestParameters) {
        return true
    }

    @Override
    Map<String, Object> get(String index, String type, String id, Map<String, String> requestParameters) {
        common(throwErrorInGet || throwNotFoundInGet, requestParameters)
        return [ "msg": "one" ]
    }

    @Override
    SearchResponse search(String query, String index, String type, Map<String, String> requestParameters) {
        common(throwErrorInSearch, requestParameters)

        this.query = query
        final SearchResponse response
        if (pageCount++ < maxPages) {
            def mapper = new JsonSlurper()
            def hits = mapper.parseText(HITS_RESULT)
            def aggs = returnAggs && pageCount == 1 ? mapper.parseText(AGGS_RESULT) :  null
            response = new SearchResponse((hits as List<Map<String, Object>>), aggs as Map<String, Object>, "pitId-${pageCount}", "scrollId-${pageCount}", "[\"searchAfter-${pageCount}\"]", 15, 5, false, null)
        } else {
            response = new SearchResponse([], [:], "pitId-${pageCount}", "scrollId-${pageCount}", "[\"searchAfter-${pageCount}\"]", 0, 1, false, null)
        }
        return response
    }

    @Override
    SearchResponse scroll(String scroll) {
        if (throwErrorInSearch) {
            throw new IOException("Simulated IOException - scroll")
        }

        return search(null, null, null, requestParameters)
    }

    @Override
    String initialisePointInTime(String index, String keepAlive) {
        if (throwErrorInPit) {
            throw new IOException("Simulated IOException - initialisePointInTime")
        }
        pageCount = 0

        return "123"
    }

    @Override
    DeleteOperationResponse deletePointInTime(String pitId) {
        if (throwErrorInDelete) {
            throw new IOException("Simulated IOException - deletePointInTime")
        }
        return new DeleteOperationResponse(100L)
    }

    @Override
    DeleteOperationResponse deleteScroll(String scrollId) {
        if (throwErrorInDelete) {
            throw new IOException("Simulated IOException - deleteScroll")
        }
        return new DeleteOperationResponse(100L)
    }

    @Override
    String getTransitUrl(String index, String type) {
        "http://localhost:9400/${index}/${type}"
    }

    private static final String AGGS_RESULT = "{\n" +
            "    \"term_agg\": {\n" +
            "      \"doc_count_error_upper_bound\": 0,\n" +
            "      \"sum_other_doc_count\": 0,\n" +
            "      \"buckets\": [\n" +
            "        {\n" +
            "          \"key\": \"five\",\n" +
            "          \"doc_count\": 5\n" +
            "        },\n" +
            "        {\n" +
            "          \"key\": \"four\",\n" +
            "          \"doc_count\": 4\n" +
            "        },\n" +
            "        {\n" +
            "          \"key\": \"three\",\n" +
            "          \"doc_count\": 3\n" +
            "        },\n" +
            "        {\n" +
            "          \"key\": \"two\",\n" +
            "          \"doc_count\": 2\n" +
            "        },\n" +
            "        {\n" +
            "          \"key\": \"one\",\n" +
            "          \"doc_count\": 1\n" +
            "        }\n" +
            "      ]\n" +
            "    },\n" +
            "    \"term_agg2\": {\n" +
            "      \"doc_count_error_upper_bound\": 0,\n" +
            "      \"sum_other_doc_count\": 0,\n" +
            "      \"buckets\": [\n" +
            "        {\n" +
            "          \"key\": \"five\",\n" +
            "          \"doc_count\": 5\n" +
            "        },\n" +
            "        {\n" +
            "          \"key\": \"four\",\n" +
            "          \"doc_count\": 4\n" +
            "        },\n" +
            "        {\n" +
            "          \"key\": \"three\",\n" +
            "          \"doc_count\": 3\n" +
            "        },\n" +
            "        {\n" +
            "          \"key\": \"two\",\n" +
            "          \"doc_count\": 2\n" +
            "        },\n" +
            "        {\n" +
            "          \"key\": \"one\",\n" +
            "          \"doc_count\": 1\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  }"

    private static final String HITS_RESULT = "[\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"14\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"five\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"5\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"three\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"8\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"four\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"9\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"four\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"10\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"four\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"12\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"five\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"2\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"two\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"4\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"three\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"6\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"three\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"15\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"five\"\n" +
            "        }\n" +
            "      }\n" +
            "    ]"

    void setThrowNotFoundInGet(boolean throwNotFoundInGet) {
        this.throwNotFoundInGet = throwNotFoundInGet
    }

    void setThrowErrorInGet(boolean throwErrorInGet) {
        this.throwErrorInGet = throwErrorInGet
    }

    void setThrowErrorInSearch(boolean throwErrorInSearch) {
        this.throwErrorInSearch = throwErrorInSearch
    }

    void setThrowErrorInDelete(boolean throwErrorInDelete) {
        this.throwErrorInDelete = throwErrorInDelete
    }

    void setThrowErrorInPit(boolean throwErrorInPit) {
        this.throwErrorInPit = throwErrorInPit
    }

    void setThrowErrorInUpdate(boolean throwErrorInUpdate) {
        this.throwErrorInUpdate = throwErrorInUpdate
    }

    void resetPageCount() {
        this.pageCount = 0
    }

    void setMaxPages(int maxPages) {
        this.maxPages = maxPages
    }

    Map<String, String> getRequestParameters() {
        return this.requestParameters
    }
}
