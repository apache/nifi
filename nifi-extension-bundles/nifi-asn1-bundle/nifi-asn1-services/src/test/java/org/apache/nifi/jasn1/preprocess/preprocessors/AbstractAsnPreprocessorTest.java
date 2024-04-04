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
package org.apache.nifi.jasn1.preprocess.preprocessors;

import org.apache.nifi.jasn1.preprocess.AsnPreprocessor;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class AbstractAsnPreprocessorTest {
    protected AsnPreprocessor testSubject;

    protected void testPreprocess(String input) throws IOException, URISyntaxException {
        // GIVEN
        List<String> lines = Files.readAllLines(Paths.get(getClass().getClassLoader().getResource(input).toURI()));

        // WHEN
        String actual = testSubject.preprocessAsn(lines)
                .stream()
                .collect(Collectors.joining(System.lineSeparator()));

        // THEN
        String expected = new String(Files.readAllBytes(Paths.get(getClass().getClassLoader().getResource("preprocessed_" + input).toURI())), StandardCharsets.UTF_8)
                .replace("\n", System.lineSeparator());

        assertEquals(expected, actual);
    }
}
