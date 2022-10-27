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

package org.apache.nifi.processors.elasticsearch.mock

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
import org.apache.nifi.util.StringUtils

class AbstractMockElasticsearchClient extends AbstractControllerService implements ElasticSearchClientService {
    boolean throwRetriableError
    boolean throwFatalError

    @Override
    List<ConfigVerificationResult> verify(ConfigurationContext context, ComponentLog verificationLogger, Map<String, String> variables) {
        return null
    }

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
    boolean exists(final String index, final Map<String, String> requestParameters) {
        return true
    }

    @Override
    boolean documentExists(String index, String type, String id, Map<String, String> requestParameters) {
        return true
    }

    @Override
    Map<String, Object> get(String index, String type, String id, Map<String, String> requestParameters) {
        return null
    }

    @Override
    SearchResponse search(String query, String index, String type, Map<String, String> requestParameters) {
        return null
    }

    @Override
    SearchResponse scroll(String scroll) {
        return null
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
        return String.format("http://localhost:9200/%s/%s", index, StringUtils.isNotBlank(type) ? type : "")
    }
}
