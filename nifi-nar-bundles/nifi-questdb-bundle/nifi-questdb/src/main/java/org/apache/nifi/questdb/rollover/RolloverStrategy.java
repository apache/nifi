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
package org.apache.nifi.questdb.rollover;

import org.apache.nifi.questdb.Client;

import java.time.ZonedDateTime;

/**
 * Defines a rollover strategy. The rollover strategy describes how the dataset withing a table should be handled in order
 * to avoid constant growing without limitation.
 *
 * The instance is not unique to a given table but unique to a given behaviour. Might be used with multiple tables.
 */
public interface RolloverStrategy {

    /**
     * Executes the rollover strategy on a given table.
     *
     * @param client The client to connect the database.
     * @param tableName The subject table.
     */
    void rollOver(Client client, String tableName);

    static RolloverStrategy keep() {
        return new KeepAllRolloverStrategy();
    }

    static RolloverStrategy deleteOld(final int daysToKeep) {
        return new DeleteOldRolloverStrategy(() -> ZonedDateTime.now(), daysToKeep);
    }
}
