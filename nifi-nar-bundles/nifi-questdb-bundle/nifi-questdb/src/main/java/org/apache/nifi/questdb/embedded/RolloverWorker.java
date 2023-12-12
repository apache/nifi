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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

final class RolloverWorker implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RolloverWorker.class);

    private final Client client;
    private final Set<ManagedTableDefinition> tableDefinitions = new HashSet<>();

    RolloverWorker(final Client client, final Set<ManagedTableDefinition> tableDefinitions) {
        this.client = client;
        this.tableDefinitions.addAll(tableDefinitions);
    }

    @Override
    public void run() {
        LOGGER.debug("Rollover started");

        for (final ManagedTableDefinition tableDefinition : tableDefinitions) {
            LOGGER.debug("Rollover started for Table {}", tableDefinition.getName());
            tableDefinition.getRolloverStrategy().rollOver(client, tableDefinition.getName());
            LOGGER.debug("Rollover completed for Table {}", tableDefinition.getName());
        }

        LOGGER.debug("Rollover completed");
    }
}
