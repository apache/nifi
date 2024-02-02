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

import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class ColumnNameNormalizerUtilityTest {



    @Test
    void testNormalizingColumnName_RemoveUnderscore() {
        String inputColumnName = "example_column_name";
        String expectedNormalized = "EXAMPLECOLUMNNAME";
        String normalized = ColumnNameNormalizerUtility.getNormalizedName(inputColumnName,true, TranslationStrategy.REMOVE_UNDERSCORE, null);

        assertEquals(expectedNormalized, normalized);
    }

    @Test
    void testNormalizingColumnName_RemoveSpace() {
        String inputColumnName = "Column Name With Spaces";
        String expectedNormalized = "COLUMNNAMEWITHSPACES";
        String normalized = ColumnNameNormalizerUtility.getNormalizedName(inputColumnName,true, TranslationStrategy.REMOVE_SPACE, null);

        assertEquals(expectedNormalized, normalized);
    }

    @Test
    void testNormalizingColumnName_RemoveAllSpecialCharacters() {
        String inputColumnName = "Special!Characters@Here$";
        String expectedNormalized = "SPECIALCHARACTERSHERE";
        String normalized = ColumnNameNormalizerUtility.getNormalizedName(inputColumnName, true, TranslationStrategy.REMOVE_ALL_SPECIAL_CHAR, null);

        assertEquals(expectedNormalized, normalized);
    }

    @Test
    void testNormalizingColumnName_Regex() {
        String inputColumnName = "Your @Input -String Here";
        Pattern translationPattern = Pattern.compile("[@-]");
        String expectedNormalized = "YOUR INPUT STRING HERE";
        String normalized = ColumnNameNormalizerUtility.getNormalizedName(inputColumnName, true, TranslationStrategy.PATTERN, translationPattern);

        assertEquals(expectedNormalized, normalized);
    }


    @Test
    void testNormalizingColumnName_NotEnabled() {
        String inputColumnName = "example_column_name";

        String normalized = ColumnNameNormalizerUtility.getNormalizedName(inputColumnName, false, TranslationStrategy.REMOVE_UNDERSCORE, null);

        assertEquals(inputColumnName, normalized);
    }
}
