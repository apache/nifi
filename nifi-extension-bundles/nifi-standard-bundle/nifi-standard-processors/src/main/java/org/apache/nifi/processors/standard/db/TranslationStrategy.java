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
package org.apache.nifi.processors.standard.db;

import org.apache.nifi.components.DescribedValue;

/**
 * Enumeration of supported Database column name Translation Strategies
 */
public enum TranslationStrategy implements DescribedValue {
    REMOVE_UNDERSCORE("Remove Underscore",
            "Underscores '_' will be removed from column names Ex: 'Pics_1_23' becomes 'PICS123'"),
    REMOVE_SPACE("Remove Space",
            "Spaces will be removed from column names Ex. 'User Name' becomes 'USERNAME'"),
    REMOVE_UNDERSCORE_AND_SPACE("Remove Underscores and Spaces",
            "Spaces and Underscores will be removed from column names Ex. 'User_1 Name' becomes 'USER1NAME'"),
    REMOVE_ALL_SPECIAL_CHAR("Remove Regular Expression Characters",
            "Remove Regular Expression Characters " +
                    "Ex. 'user-id' becomes USERID ,total(estimated) become TOTALESTIMATED"),
    PATTERN("Regular Expression",
            "Remove characters matching this Regular Expression from the column names Ex." +
                    "1. '\\d' will  Remove all numbers " +
                    "2. '[^a-zA-Z0-9_]' will remove special characters except underscore");
    private final String displayName;
    private final String description;

    TranslationStrategy(String displayName, String description) {
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