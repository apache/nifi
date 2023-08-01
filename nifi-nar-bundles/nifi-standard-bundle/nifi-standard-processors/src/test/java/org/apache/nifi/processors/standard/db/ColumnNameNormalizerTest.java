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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class ColumnNameNormalizerTest {

    private ColumnNameNormalizer normalizer;

    @Test
    void testNormalizingColumnName_RemoveUnderscore() {
        String inputColumnName = "example_column_name";
        String expectedNormalized = "EXAMPLECOLUMNNAME";
        normalizer = new ColumnNameNormalizer(true, TranslationStrategy.REMOVE_UNDERSCORE, null);

        String normalized = normalizer.getColName(inputColumnName);

        assertEquals(expectedNormalized, normalized);
    }

    @Test
    void testNormalizingColumnName_RemoveSpace() {
        String inputColumnName = "Column Name With Spaces";
        String expectedNormalized = "COLUMNNAMEWITHSPACES";
        normalizer = new ColumnNameNormalizer(true, TranslationStrategy.REMOVE_SPACE, null);
        String normalized = normalizer.getColName(inputColumnName);

        assertEquals(expectedNormalized, normalized);
    }

    @Test
    void testNormalizingColumnName_RemoveAllSpecialCharacters() {
        String inputColumnName = "Special!Characters@Here$";
        String expectedNormalized = "SPECIALCHARACTERSHERE";
        normalizer = new ColumnNameNormalizer(true, TranslationStrategy.REMOVE_ALL_SPECIAL_CHAR, null);
        String normalized = normalizer.getColName(inputColumnName);

        assertEquals(expectedNormalized, normalized);
    }

    @Test
    void testNormalizingColumnName_Regex() {
        String inputColumnName = "Your @Input -String Here";
        String translationRegex = "[@-]";
        String expectedNormalized = inputColumnName.toUpperCase().replaceAll(translationRegex, "");

        ColumnNameNormalizer regexNormalizer = new ColumnNameNormalizer(true, TranslationStrategy.REGEX, translationRegex);
        String normalized = regexNormalizer.getColName(inputColumnName);

        assertEquals(expectedNormalized, normalized);
    }

    @Test
    void testNormalizingColumnName_NullInput() {
        normalizer = new ColumnNameNormalizer(true, null, null);
        String normalized = normalizer.getColName(null);

        assertNull(normalized);
    }

    @Test
    void testNormalizingColumnName_NotEnabled() {
        normalizer = new ColumnNameNormalizer(false, TranslationStrategy.REMOVE_UNDERSCORE, null);

        String inputColumnName = "example_column_name";

        String normalized = normalizer.getColName(inputColumnName);

        assertEquals(inputColumnName, normalized);
    }
}
