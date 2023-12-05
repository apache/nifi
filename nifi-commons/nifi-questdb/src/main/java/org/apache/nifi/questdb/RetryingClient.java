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
package org.apache.nifi.questdb;

import org.apache.nifi.retry.RetryTemplate;

import java.util.function.BiConsumer;

final class RetryingClient implements Client {
    private final Client client;
    private final Client fallbackClient;
    private final RetryTemplate retryTemplate;

    private RetryingClient(
            final RetryTemplate retryTemplate,
            final Client client,
            final Client fallbackClient) {
        this.retryTemplate = retryTemplate;
        this.client = client;
        this.fallbackClient = fallbackClient;
    }

    @Override
    public void execute(final String query)  {
        retryTemplate.executeWithoutValue(() -> client.execute(query), () -> fallbackClient.execute(query));
    }

    @Override
    public void insert(final String tableName, final InsertRowDataSource rowSource) {
        retryTemplate.executeWithoutValue(() -> client.insert(tableName, rowSource), () -> fallbackClient.insert(tableName, rowSource));
    }

    @Override
    public <T> T query(final String query, final QueryResultProcessor<T> rowProcessor) {
        return retryTemplate.execute(() -> client.query(query, rowProcessor), () -> fallbackClient.query(query, rowProcessor));
    }

    @Override
    public void disconnect() throws DatabaseException {
        client.disconnect();
    }

    static RetryingClient getInstance(final int numberOfRetries, final BiConsumer<Integer, Exception> errorAction, final Client client, final Client fallbackClient) {
        final RetryTemplate retryAction = RetryTemplate.builder()
                .times(numberOfRetries)
                .abortingExceptions(ClientIsDisconnectedException.class)
                .errorAction(errorAction)
                .buildSynchronousRetryTemplate();
        return new RetryingClient(retryAction, client, fallbackClient);
    }
}
