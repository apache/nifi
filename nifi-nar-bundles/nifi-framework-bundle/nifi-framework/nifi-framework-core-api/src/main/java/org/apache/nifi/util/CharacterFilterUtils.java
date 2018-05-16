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
package org.apache.nifi.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.translate.AggregateTranslator;
import org.apache.commons.lang3.text.translate.CharSequenceTranslator;
import org.apache.commons.lang3.text.translate.LookupTranslator;
import org.apache.commons.lang3.text.translate.UnicodeUnpairedSurrogateRemover;

import java.util.Arrays;
import java.util.List;

public class CharacterFilterUtils {

    private static final List<String> INVALID_XML_CHARACTERS = Arrays.asList(
            "\u0000", "\u0001", "\u0002", "\u0003", "\u0004", "\u0005", "\u0006", "\u0007", "\u0008", "\u000b",
            "\u000c", "\u000e", "\u000f", "\u0010", "\u0011", "\u0012", "\u0013", "\u0014", "\u0015", "\u0016",
            "\u0017", "\u0018", "\u0019", "\u001a", "\u001b", "\u001c", "\u001d", "\u001e", "\u001f", "\ufffe",
            "\uffff");

    private static final String[][] INVALID_XML_CHARACTER_MAPPING = INVALID_XML_CHARACTERS.stream()
            .map(invalidCharacter -> new String[] { invalidCharacter, StringUtils.EMPTY })
            .toArray(String[][]::new);

    private static final CharSequenceTranslator INVALID_XML_CHARACTER_FILTER = new AggregateTranslator(
            new LookupTranslator(INVALID_XML_CHARACTER_MAPPING),
            new UnicodeUnpairedSurrogateRemover());

    public static String filterInvalidXmlCharacters(final String value) {
        if (value == null) {
            return null;
        }
        return INVALID_XML_CHARACTER_FILTER.translate(value);
    }
}