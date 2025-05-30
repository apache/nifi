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

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.DescribedValue;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum HeaderSchemaStrategy implements DescribedValue {
    USE_STARTING_ROW_STANDARD("The schema will be derived by using the column names in the header of the first sheet and the following "
            + HeaderSchemaStrategy.NUM_ROWS_TO_DETERMINE_TYPES
            + " rows to determine the type(s) of each column while the configured header rows of subsequent sheets are skipped. "
            + HeaderSchemaStrategy.NOTE),
    USE_STARTING_ROW_ALL("The schema will be derived by using the column names in the header of the first sheet and all "
            + " rows to determine the type(s) of each column while the configured header rows of subsequent sheets are skipped. "
            + HeaderSchemaStrategy.NOTE);

    static final int NUM_ROWS_TO_DETERMINE_TYPES = 10; // NOTE: This number is arbitrary.
    static final String NOTE = "NOTE: If there are duplicate column names then each subsequent duplicate column name is given a one up number. "
            + "For example, column names \"Name\", \"Name\" will be changed to \"Name\", \"Name_1\"";
    static final Set<String> ALL_STRATEGIES = Arrays.stream(HeaderSchemaStrategy.values())
            .map(HeaderSchemaStrategy::getValue)
            .collect(Collectors.toUnmodifiableSet());
    private final String description;

    HeaderSchemaStrategy(String description) {
        this.description = description;
    }

    public AllowableValue getAllowableValue() {
        return new AllowableValue(getValue(), getDisplayName(), getDescription());
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return Arrays.stream(name().split("_"))
                .map(string -> string.charAt(0) + string.substring(1).toLowerCase())
                .collect(Collectors.joining(" "));
    }

    @Override
    public String getDescription() {
        return description;
    }
}
