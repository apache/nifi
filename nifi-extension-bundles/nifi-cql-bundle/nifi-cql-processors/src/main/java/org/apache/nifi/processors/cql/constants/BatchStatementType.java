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

package org.apache.nifi.processors.cql.constants;

import org.apache.nifi.components.DescribedValue;

public enum BatchStatementType implements DescribedValue {
    LOGGED_TYPE("LOGGED", "LOGGED",
            "Use a LOGGED batch statement"),
    UNLOGGED_TYPE("UNLOGGED", "UNLOGGED",
            "Use an UNLOGGED batch statement"),
    COUNTER_TYPE("COUNTER", "COUNTER",
            "Use a COUNTER batch statement"),
    BATCH_STATEMENT_TYPE_USE_ATTR_TYPE("USE_ATTR", "Use cql.batch.statement.type Attribute",
            "The value of the cql.batch.statement.type Attribute will be used to determine which type of batch statement (LOGGED, UNLOGGED or COUNTER) " +
                    "will be used to generate and execute the Update statement.");

    private String value;
    private String displayName;
    private String description;

    BatchStatementType(String value, String displayName, String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
