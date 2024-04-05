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

final class ConditionAwareClient implements Client {
    public static final String CONDITION_ERROR = "Condition is not met, cannot call the enveloped client";
    private final Client client;
    private final Condition condition;

    ConditionAwareClient(final Condition condition, final Client client) {
        this.client = client;
        this.condition = condition;
    }

    @Override
    public void execute(final String query) throws DatabaseException {
        if (condition.check()) {
            client.execute(query);
        } else {
            throw new ConditionFailedException(CONDITION_ERROR);
        }
    }

    @Override
    public void insert(final String tableName, final InsertRowDataSource rowSource) throws DatabaseException {
        if (condition.check()) {
            client.insert(tableName, rowSource);
        } else {
            throw new ConditionFailedException(CONDITION_ERROR);
        }
    }

    @Override
    public <T> T query(final String query, final QueryResultProcessor<T> rowProcessor) throws DatabaseException {
        if (condition.check()) {
            return client.query(query, rowProcessor);
        } else {
            throw new ConditionFailedException(CONDITION_ERROR);
        }
    }

    @Override
    public void disconnect() throws DatabaseException {
        client.disconnect();
    }

    @FunctionalInterface
    interface Condition {
        boolean check();
    }

    @Override
    public String toString() {
        return "ConditionAwareQuestDbClient{" +
                "client=" + client +
                ", condition=" + condition +
                '}';
    }
}
