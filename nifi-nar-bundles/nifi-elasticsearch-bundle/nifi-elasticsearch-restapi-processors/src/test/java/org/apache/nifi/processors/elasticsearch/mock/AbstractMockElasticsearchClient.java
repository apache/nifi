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
package org.apache.nifi.processors.elasticsearch.mock;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.elasticsearch.DeleteOperationResponse;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.IndexOperationRequest;
import org.apache.nifi.elasticsearch.IndexOperationResponse;
import org.apache.nifi.elasticsearch.SearchResponse;
import org.apache.nifi.elasticsearch.UpdateOperationResponse;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.StringUtils;

import java.util.List;
import java.util.Map;

public class AbstractMockElasticsearchClient extends AbstractControllerService implements ElasticSearchClientService {
    @Override
    public List<ConfigVerificationResult> verify(ConfigurationContext context, ComponentLog verificationLogger, Map<String, String> variables) {
        return null;
    }

    @Override
    public IndexOperationResponse add(IndexOperationRequest operation, Map<String, String> requestParameters) {
        return null;
    }

    @Override
    public IndexOperationResponse bulk(List<IndexOperationRequest> operations, Map<String, String> requestParameters) {
        return null;
    }

    @Override
    public Long count(String query, String index, String type, Map<String, String> requestParameters) {
        return null;
    }

    @Override
    public DeleteOperationResponse deleteById(String index, String type, String id, Map<String, String> requestParameters) {
        return null;
    }

    @Override
    public DeleteOperationResponse deleteById(String index, String type, List<String> ids, Map<String, String> requestParameters) {
        return null;
    }

    @Override
    public DeleteOperationResponse deleteByQuery(String query, String index, String type, Map<String, String> requestParameters) {
        return null;
    }

    @Override
    public UpdateOperationResponse updateByQuery(String query, String index, String type, Map<String, String> requestParameters) {
        return null;
    }

    @Override
    public void refresh(final String index, final Map<String, String> requestParameters) {
    }

    @Override
    public boolean exists(final String index, final Map<String, String> requestParameters) {
        return true;
    }

    @Override
    public boolean documentExists(String index, String type, String id, Map<String, String> requestParameters) {
        return true;
    }

    @Override
    public Map<String, Object> get(String index, String type, String id, Map<String, String> requestParameters) {
        return null;
    }

    @Override
    public SearchResponse search(String query, String index, String type, Map<String, String> requestParameters) {
        return null;
    }

    @Override
    public SearchResponse scroll(String scroll) {
        return null;
    }

    @Override
    public String initialisePointInTime(String index, String keepAlive) {
        return null;
    }

    @Override
    public DeleteOperationResponse deletePointInTime(String pitId) {
        return null;
    }

    @Override
    public DeleteOperationResponse deleteScroll(String scrollId) {
        return null;
    }

    @Override
    public String getTransitUrl(String index, String type) {
        return String.format("http://localhost:9200/%s/%s", index, StringUtils.isNotBlank(type) ? type : "");
    }

    public boolean getThrowRetriableError() {
        return throwRetriableError;
    }

    public boolean isThrowRetriableError() {
        return throwRetriableError;
    }

    public void setThrowRetriableError(boolean throwRetriableError) {
        this.throwRetriableError = throwRetriableError;
    }

    public boolean getThrowFatalError() {
        return throwFatalError;
    }

    public boolean isThrowFatalError() {
        return throwFatalError;
    }

    public void setThrowFatalError(boolean throwFatalError) {
        this.throwFatalError = throwFatalError;
    }

    private boolean throwRetriableError;
    private boolean throwFatalError;
}
