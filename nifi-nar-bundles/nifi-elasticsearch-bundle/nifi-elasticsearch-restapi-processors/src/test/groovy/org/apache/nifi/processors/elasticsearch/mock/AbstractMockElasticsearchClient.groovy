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

import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.elasticsearch.*

class AbstractMockElasticsearchClient extends AbstractControllerService implements ElasticSearchClientService {
    boolean throwRetriableError
    boolean throwFatalError

    @Override
    IndexOperationResponse add(IndexOperationRequest operation) {
        return null
    }

    @Override
    IndexOperationResponse bulk(List<IndexOperationRequest> operations) {
        return null
    }

    @Override
    Long count(String query, String index, String type) {
        return null
    }

    @Override
    DeleteOperationResponse deleteById(String index, String type, String id) {
        return null
    }

    @Override
    DeleteOperationResponse deleteById(String index, String type, List<String> ids) {
        return null
    }

    @Override
    DeleteOperationResponse deleteByQuery(String query, String index, String type) {
        return null
    }

    @Override
    Map<String, Object> get(String index, String type, String id) {
        return null
    }

    @Override
    SearchResponse search(String query, String index, String type) {
        return null
    }

    @Override
    String getTransitUrl(String index, String type) {
        return null
    }
}
