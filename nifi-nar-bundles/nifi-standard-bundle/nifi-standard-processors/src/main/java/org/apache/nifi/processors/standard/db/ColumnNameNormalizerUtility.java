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


import java.util.Objects;
import java.util.regex.Pattern;

/**
 * ColumnNameNormalizerUtility is a utility class that helps to normalize column names. It provides various strategies to
 * modify column names based on the TranslationStrategy enum. Column names can be normalized by removing underscores,
 * spaces, all special characters, or by using a custom regular expression defined by the user.
 */
public class ColumnNameNormalizerUtility {
    // Regular expression to remove all special characters from a string.
    private static final Pattern REMOVE_ALL_SPECIAL_CHAR_REGEX = Pattern.compile("[^a-zA-Z0-9]");

    /**
     * Normalizes the given column name based on the specified strategy.
     *
     * @param colName The column name to be normalized.
     * @param isTranslationEnabled Boolean value to denote normalization is enabled
     * @param strategy The TranslationStrategy for normalizing column name
     * @param translationRegex Regex For translation
     * @return The normalized column name as a String.
     */
    public static String getNormalizedName(final String colName, boolean isTranslationEnabled, TranslationStrategy strategy, Pattern translationRegex) {
        // If the column name is null or translation is not enabled, return the original column name.
        if (colName == null || !isTranslationEnabled) {
            return colName;
        }

        return switch (Objects.requireNonNull(strategy)) {
            case REMOVE_UNDERSCORE -> colName.toUpperCase().replace("_", "");
            case REMOVE_SPACE -> colName.toUpperCase().replace(" ", "");
            case REMOVE_ALL_SPECIAL_CHAR -> REMOVE_ALL_SPECIAL_CHAR_REGEX .matcher(colName.toUpperCase()).replaceAll("");
            case PATTERN -> translationRegex.matcher(colName.toUpperCase()).replaceAll("");
        };
    }

}