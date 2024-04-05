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
package org.apache.nifi.questdb.embedded;

import org.apache.nifi.questdb.Client;
import org.apache.nifi.questdb.DatabaseException;
import org.apache.nifi.questdb.InsertRowDataSource;
import org.apache.nifi.questdb.QueryResultProcessor;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;

import java.util.function.BiConsumer;

final class RetryingClient implements Client {
    private final RetryTemplate retryTemplate;
    private final Client client;
    private final Client fallbackClient;

    private RetryingClient(final RetryTemplate retryTemplate, final Client client, final Client fallbackClient) {
        this.retryTemplate = retryTemplate;
        this.client = client;
        this.fallbackClient = fallbackClient;
    }

    @Override
    public void execute(final String query) throws DatabaseException {
        retryTemplate.execute(
            new RetryWhenConnected<>() {
                @Override
                public Void executeWithRetry(final RetryContext context) throws DatabaseException {
                    client.execute(query);
                    return null;
                }
            },
            (RecoveryCallback<Void>) context -> {
                fallbackClient.execute(query);
                return null;
            }
        );
    }

    @Override
    public void insert(final String tableName, final InsertRowDataSource rowSource) throws DatabaseException {
        retryTemplate.execute(
            new RetryWhenConnected<>() {
                @Override
                public Void executeWithRetry(final RetryContext context) throws DatabaseException {
                    client.insert(tableName, rowSource);
                    return null;
                }
            },
            (RecoveryCallback<Void>) context -> {
                fallbackClient.insert(tableName, rowSource);
                return null;
            }
        );
    }

    @Override
    public <T> T query(final String query, final QueryResultProcessor<T> rowProcessor) throws DatabaseException {
        return retryTemplate.execute(
            new RetryWhenConnected<>() {
                @Override
                public T executeWithRetry(final RetryContext context) throws DatabaseException {
                    return client.query(query, rowProcessor);
                }
            },
            context -> fallbackClient.query(query, rowProcessor)
        );
    }

    @Override
    public void disconnect() throws DatabaseException {
        client.disconnect();
    }

    private static abstract class RetryWhenConnected<R> implements RetryCallback<R, DatabaseException> {
        @Override
        public R doWithRetry(final RetryContext context) throws DatabaseException {
            try {
                return executeWithRetry(context);
            } catch (final ClientDisconnectedException e) {
                context.setExhaustedOnly();
                throw e;
            }
        }

        public abstract R executeWithRetry(final RetryContext context) throws DatabaseException;
    }

    static RetryingClient getInstance(final int numberOfRetries, final BiConsumer<Integer, Throwable> errorAction, final Client client, final Client fallbackClient) {
        final RetryListener listener = new RetryListener() {
            @Override
            public <T, E extends Throwable> void onError(final RetryContext context, final RetryCallback<T, E> callback, final Throwable throwable) {
                errorAction.accept(context.getRetryCount(), throwable);
            }
        };

        final RetryTemplate retryTemplate = RetryTemplate.builder()
            .maxAttempts(numberOfRetries + 1)
            .fixedBackoff(50)
            .withListener(listener)
            .build();

        return new RetryingClient(retryTemplate, client, fallbackClient);
    }
}
