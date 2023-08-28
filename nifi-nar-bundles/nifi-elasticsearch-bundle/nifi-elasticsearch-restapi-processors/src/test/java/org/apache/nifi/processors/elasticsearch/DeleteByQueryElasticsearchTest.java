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
package org.apache.nifi.processors.elasticsearch;

public class DeleteByQueryElasticsearchTest extends AbstractByQueryElasticsearchTest {
    @Override
    public String queryAttr() {
        return "es.delete.query";
    }

    @Override
    public String tookAttr() {
        return DeleteByQueryElasticsearch.TOOK_ATTRIBUTE;
    }

    @Override
    public String errorAttr() {
        return DeleteByQueryElasticsearch.ERROR_ATTRIBUTE;
    }

    @Override
    public AbstractByQueryElasticsearch getTestProcessor() {
        return new DeleteByQueryElasticsearch();
    }

    @Override
    public void expectError(final TestElasticsearchClientService client) {
        client.setThrowErrorInDelete(true);
    }
}
