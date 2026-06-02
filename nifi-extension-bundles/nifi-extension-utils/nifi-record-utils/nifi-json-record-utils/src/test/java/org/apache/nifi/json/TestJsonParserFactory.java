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
package org.apache.nifi.json;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestJsonParserFactory {
    private static final String LENIENT_JSON = """
            {
              // Java-style single line comment
              /* C-style multi-line
                 comment */
              "numbers": {
                "leadingZero": 00042,
                "leadingPlus": +123,
                "missingDecimal": 10.
              },
              "quotes": {
                'singleQuotedKey': 'value',
                unquotedKey: "value"
              },
              "arrayMissingValue": [
                "first",
                ,
                "third",
              ],
              "objectTrailing": {
                "key": "value",
              }
            }
            """;

    @ParameterizedTest
    @MethodSource("jsonParsing")
    void testJsonParsing(ParsingStrategy parsingStrategy) throws Exception {
        final StreamReadConstraints streamReadConstraints = StreamReadConstraints.builder().build();
        final JsonParserFactory jsonParserFactory = new JsonParserFactory(streamReadConstraints, parsingStrategy);
        final InputStream inputStream = new ByteArrayInputStream(LENIENT_JSON.getBytes(StandardCharsets.UTF_8));
        final JsonParser jsonParser = jsonParserFactory.getJsonParser(inputStream);

        if (ParsingStrategy.LENIENT == parsingStrategy) {
            assertDoesNotThrow(() -> jsonParser.readValueAsTree());
        } else {
            assertThrows(JsonParseException.class, jsonParser::readValueAsTree);
        }
    }

    private static Stream<Arguments> jsonParsing() {
        return Stream.of(
                Arguments.argumentSet("Standard", ParsingStrategy.STANDARD),
                Arguments.argumentSet("Lenient", ParsingStrategy.LENIENT)
        );
    }
}
