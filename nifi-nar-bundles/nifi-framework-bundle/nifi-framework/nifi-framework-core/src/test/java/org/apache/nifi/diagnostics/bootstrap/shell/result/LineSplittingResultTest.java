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
package org.apache.nifi.diagnostics.bootstrap.shell.result;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LineSplittingResultTest {
    private static final String REGEX_FOR_SPLITTING = "\\s+";
    private static final List<String> RESULT_LABELS = Arrays.asList("Label1", "Label2", "Label3", "Label4");
    private static final String COMMAND_NAME = "Test command";
    private static final String EMPTY_RESPONSE = "";
    private static final String RESPONSE = "data1   data2  data3  data4";
    private static final String RESPONSE_WITH_EMPTY_LINES = "\ndata1   data2  data3  data4\n";
    private static final List<String> EXPECTED_RESPONSE = Arrays.asList("Label1 : data1", "Label2 : data2", "Label3 : data3", "Label4 : data4");
    private static LineSplittingResult lineSplittingResult;

    @BeforeAll
    public static void setUp() {
        lineSplittingResult = new LineSplittingResult(REGEX_FOR_SPLITTING, RESULT_LABELS, COMMAND_NAME);
    }

    @Test
    public void testEmptyResponse() {
        final InputStream inputStream = new ByteArrayInputStream(EMPTY_RESPONSE.getBytes(StandardCharsets.UTF_8));

        assertTrue(lineSplittingResult.createResult(inputStream).isEmpty());
    }

    @Test
    public void testResponseWithoutEmptyLines() {
        final InputStream inputStream = new ByteArrayInputStream(RESPONSE.getBytes(StandardCharsets.UTF_8));

        assertEquals(EXPECTED_RESPONSE, lineSplittingResult.createResult(inputStream));
    }

    @Test
    public void testResponseWithEmptyLines() {
        final InputStream inputStream = new ByteArrayInputStream(RESPONSE_WITH_EMPTY_LINES.getBytes(StandardCharsets.UTF_8));

        assertEquals(EXPECTED_RESPONSE, lineSplittingResult.createResult(inputStream));
    }
}