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
package org.apache.nifi.excel;

import org.apache.nifi.components.DescribedValue;

public enum RowEvaluationStrategy implements DescribedValue {
    STANDARD("Standard", "Use %s rows after the starting row.".formatted(RowEvaluationStrategy.NUM_ROWS_TO_DETERMINE_TYPES)),
    ALL("All Rows", "Use all the rows after the starting row.");

    static final int NUM_ROWS_TO_DETERMINE_TYPES = 10; // NOTE: This number is arbitrary.

    private final String displayName;
    private final String description;

    RowEvaluationStrategy(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
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
