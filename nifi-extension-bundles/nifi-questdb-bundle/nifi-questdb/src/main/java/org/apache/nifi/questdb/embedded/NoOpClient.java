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
import org.apache.nifi.questdb.InsertRowDataSource;
import org.apache.nifi.questdb.QueryResultProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class NoOpClient implements Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(NoOpClient.class);

    @Override
    public void execute(final String query) {
        LOGGER.debug("Execute: {}", query);
    }

    @Override
    public void insert(final String tableName, final InsertRowDataSource rowSource) {
        LOGGER.debug("Inserting rows to Table {}", tableName);
    }

    @Override
    public <T> T query(final String query, final QueryResultProcessor<T> rowProcessor) {
        LOGGER.debug("Querying: {}", query);
        return rowProcessor.getResult();
    }

    @Override
    public void disconnect() {
        LOGGER.debug("Disconnecting");
    }
}
