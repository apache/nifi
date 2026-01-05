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
package org.apache.nifi.processors.standard;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestReplaceText {
    private TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(ReplaceText.class);

        /*
         * we have to disable validation of expression language because the scope of the evaluation
         * for the search value depends on another property (the evaluation mode). If not disabling
         * the validation, it'll throw an error about the eval
         */
        runner.setValidateExpressionUsage(false);
    }

    @Test
    public void testLiteralReplaceWithExpressionLanguageInSearchEntireText() {
        runner.enqueue("Me, you, and the other", Collections.singletonMap("search.value", "you, and"));
        runner.setProperty(ReplaceText.SEARCH_VALUE, "Me, ${search.value}");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "\"Replacement\"");
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.ENTIRE_TEXT);
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.LITERAL_REPLACE.getValue());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("\"Replacement\" the other");
    }

    @Test
    public void testLiteralReplaceWithExpressionLanguageInReplacementEntireText() {
        runner.enqueue("Me, you, and the other", Collections.singletonMap("replacement.value", "us"));
        runner.setProperty(ReplaceText.SEARCH_VALUE, "Me, you,");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "With ${replacement.value}");
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.ENTIRE_TEXT);
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.LITERAL_REPLACE.getValue());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("With us and the other");
    }

    @Test
    public void testLiteralReplaceWithExpressionLanguageInSearchLineByLine() {
        runner.enqueue("Me, you, and the other", Collections.singletonMap("search.value", "you, and"));
        runner.setProperty(ReplaceText.SEARCH_VALUE, "Me, ${search.value}");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "\"Replacement\"");
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.LITERAL_REPLACE.getValue());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("\"Replacement\" the other");
    }

    @Test
    public void testLiteralReplaceWithExpressionLanguageInReplacementLineByLine() {
        runner.enqueue("Me, you, and the other", Collections.singletonMap("replacement.value", "us"));
        runner.setProperty(ReplaceText.SEARCH_VALUE, "Me, you,");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "With ${replacement.value}");
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.LITERAL_REPLACE.getValue());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("With us and the other");
    }

    @Test
    public void testConfigurationCornerCase() throws IOException {
        final Path helloText = Paths.get("src/test/resources/hello.txt");
        runner.run();
        runner.enqueue(helloText);
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(helloText);
    }

    @Test
    public void testIterativeRegexReplace() {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "\"([a-z]+?)\":\"(.*?)\"");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "\"${'$1':toUpper()}\":\"$2\"");

        runner.enqueue("{\"name\":\"Smith\",\"middle\":\"nifi\",\"firstname\":\"John\"}");
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("{\"NAME\":\"Smith\",\"MIDDLE\":\"nifi\",\"FIRSTNAME\":\"John\"}");
    }

    @Test
    public void testIterativeRegexReplaceLineByLine() {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "\"([a-z]+?)\":\"(.*?)\"");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "\"${'$1':toUpper()}\":\"$2\"");

        runner.enqueue("{\"name\":\"Smith\",\"middle\":\"nifi\",\"firstname\":\"John\"}");
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("{\"NAME\":\"Smith\",\"MIDDLE\":\"nifi\",\"FIRSTNAME\":\"John\"}");
    }


    @Test
    public void testSimple() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "ell");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "lle");

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("Hlleo, World!".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testWithEscapedDollarSignInReplacement() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(?s:^.*$)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "a\\$b");

        runner.enqueue("a$a,b,c,d");
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("a\\$b".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testWithUnEscapedDollarSignInReplacement() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(?s:^.*$)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "a$b");

        runner.enqueue("a$a,b,c,d");
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("a$b".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testPrependSimple() throws IOException {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "TEST");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.PREPEND);

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("TESTHello, World!".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testPrependLineByLine() throws IOException {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "_");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.PREPEND);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);

        runner.enqueue("hello\nthere\nmadam".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("_hello\n_there\n_madam".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testPrependFirstLine() throws IOException {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "_");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.PREPEND);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.FIRST_LINE);

        runner.enqueue("hello\nthere\nmadam".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("_hello\nthere\nmadam".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testPrependLastLine() throws IOException {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "_");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.PREPEND);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.LAST_LINE);

        runner.enqueue("hello\nthere\nmadam".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("hello\nthere\n_madam".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testPrependExceptFirstLine() throws IOException {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "_");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.PREPEND);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.EXCEPT_FIRST_LINE);

        runner.enqueue("hello\nthere\nmadam".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("hello\n_there\n_madam".getBytes(StandardCharsets.UTF_8));
    }


    @Test
    public void testPrependExceptLastLine() throws IOException {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "_");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.PREPEND);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.EXCEPT_LAST_LINE);

        runner.enqueue("hello\nthere\nmadam".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("_hello\n_there\nmadam".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testAppendSimple() throws IOException {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "TEST");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.APPEND);

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("Hello, World!TEST".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testAppendWithCarriageReturn() {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "!");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.APPEND);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);

        runner.enqueue("hello\rthere\rsir".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("hello!\rthere!\rsir!");
    }


    @Test
    public void testAppendFirstLineWithCarriageReturn() {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "!");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.APPEND);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.FIRST_LINE);

        runner.enqueue("hello\rthere\rsir".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("hello!\rthere\rsir");
    }

    @Test
    public void testAppendExceptFirstLineWithCarriageReturn() {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "!");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.APPEND);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.EXCEPT_FIRST_LINE);

        runner.enqueue("hello\rthere\rsir".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("hello\rthere!\rsir!");
    }

    @Test
    public void testAppendLastLineWithCarriageReturn() {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "!");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.APPEND);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.LAST_LINE);

        runner.enqueue("hello\rthere\rsir".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("hello\rthere\rsir!");
    }

    @Test
    public void testAppendExceptLastLineWithCarriageReturn() {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "!");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.APPEND);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.EXCEPT_LAST_LINE);

        runner.enqueue("hello\rthere\rsir".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("hello!\rthere!\rsir");
    }

    @Test
    public void testAppendWithNewLine() {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "!");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.APPEND);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);

        runner.enqueue("hello\nthere\nsir".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("hello!\nthere!\nsir!");
    }

    @Test
    public void testAppendWithCarriageReturnNewLine() {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "!");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.APPEND);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);

        runner.enqueue("hello\r\nthere\r\nsir".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("hello!\r\nthere!\r\nsir!");
    }

    @Test
    public void testAppendFirstLineWithCarriageReturnNewLine() {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "!");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.APPEND);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.FIRST_LINE);

        runner.enqueue("hello\r\nthere\r\nsir".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("hello!\r\nthere\r\nsir");
    }


    @Test
    public void testAppendLastLineWithCarriageReturnNewLine() {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "!");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.APPEND);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.LAST_LINE);

        runner.enqueue("hello\r\nthere\r\nsir".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("hello\r\nthere\r\nsir!");
    }


    @Test
    public void testAppendExceptFistLineWithCarriageReturnNewLine() {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "!");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.APPEND);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.EXCEPT_FIRST_LINE);

        runner.enqueue("hello\r\nthere\r\nsir".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("hello\r\nthere!\r\nsir!");
    }


    @Test
    public void testAppendExceptLastLineWithCarriageReturnNewLine() {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "!");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.APPEND);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.EXCEPT_LAST_LINE);


        runner.enqueue("hello\r\nthere\r\nsir".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("hello!\r\nthere!\r\nsir");
    }

    @Test
    public void testLiteralSimple() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "ell");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "lle");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.LITERAL_REPLACE);

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("Hlleo, World!".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testLiteralBackReference() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "ell");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "[$1]");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.LITERAL_REPLACE);

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("H[$1]o, World!");
    }

    @Test
    public void testLiteral() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, ".ell.");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "test");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.LITERAL_REPLACE);

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));
        runner.run();
        runner.enqueue("H.ell.o, World! .ell.".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 2);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("Hello, World!");
        final MockFlowFile out2 = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(1);
        out2.assertContentEquals("Htesto, World! test");
    }

    @Test
    public void testBackReference() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(ell)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "[$1]");

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("H[ell]o, World!");
    }

    @Test
    public void testBackRefFollowedByNumbers() throws IOException {
        String expected = "Hell23o, World!";
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(ell)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$123");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "notSupported");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        final String actual = new String(out.toByteArray(), StandardCharsets.UTF_8);
        assertEquals(expected, actual);
    }

    @Test
    public void testBackRefWithNoCapturingGroup() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "ell");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$0123");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "notSupported");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        final String actual = new String(out.toByteArray(), StandardCharsets.UTF_8);
        assertEquals("Hell123o, World!", actual);
    }

    @Test
    public void testReplacementWithExpressionLanguage() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "${replaceKey}");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "GoodBye");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "H.*o");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("Hello, World!");
    }

    @Test
    public void testReplacementWithExpressionLanguageIsEscaped() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(ell)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "[${abc}]");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "$1");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("H[$1]o, World!");
    }

    @Test
    public void testRegexWithExpressionLanguage() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "${replaceKey}");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${replaceValue}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "Hello");
        attributes.put("replaceValue", "Good-bye");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("Good-bye, World!");
    }

    @Test
    public void testRegexWithExpressionLanguageIsEscaped() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "${replaceKey}");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${replaceValue}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "H.*o");
        attributes.put("replaceValue", "Good-bye");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("Hello, World!");
    }

    @Test
    public void testBackReferenceWithTooLargeOfIndexIsEscaped() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(ell)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$1$2");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "H.*o");
        attributes.put("replaceValue", "Good-bye");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("Hell$2o, World!");
    }

    @Test
    public void testBackReferenceWithInvalidReferenceIsEscaped() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(ell)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$d");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "H.*o");
        attributes.put("replaceValue", "Good-bye");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("H$do, World!");
    }

    @Test
    public void testEscapingDollarSign() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(ell)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "\\$1");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "H.*o");
        attributes.put("replaceValue", "Good-bye");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("H$1o, World!");
    }

    @Test
    public void testReplaceWithEmptyString() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(ell)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "");

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("Ho, World!");
    }

    @Test
    public void testWithNoMatch() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "Z");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "Morning");

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("Hello, World!");
    }

    @Test
    public void testWithMultipleMatches() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "l");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "R");

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("HeRRo, WorRd!");
    }

    @Test
    public void testAttributeToContent() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, ".*");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${abc}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "Good");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("Good");
    }

    @Test
    public void testRoutesToFailureIfTooLarge() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "[123]");
        runner.setProperty(ReplaceText.MAX_BUFFER_SIZE, "1 b");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${abc}");
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.ENTIRE_TEXT);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "Good");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_FAILURE, 1);
    }

    @Test
    public void testRoutesToFailureIfLineTooLarge() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "[123]");
        runner.setProperty(ReplaceText.MAX_BUFFER_SIZE, "5 kb");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${abc}");
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "Good");
        runner.enqueue(Paths.get("src/test/resources/TestReplaceTextLineByLine/apacheLicenseOnOneLine.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_FAILURE, 1);
    }

    @Test
    public void testRoutesToSuccessIfTooLargeButRegexIsDotAsterisk() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, ".*");
        runner.setProperty(ReplaceText.MAX_BUFFER_SIZE, "1 b");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${abc}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "Good");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("Good");
    }

    @Test
    public void testProblematicCase1() throws IOException {
        runner.setProperty(ReplaceText.SEARCH_VALUE, ".*");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${filename}\t${now():format(\"yyyy/MM/dd'T'HHmmss'Z'\")}\t${fileSize}\n");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "abc.txt");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        final String outContent = new String(out.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(outContent.startsWith("abc.txt\t"));
        assertTrue(outContent.endsWith("13\n"));
    }

    @Test
    public void testGetExistingContent() {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(?s)(^.*)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "attribute header\n\n${filename}\n\ndata header\n\n$1\n\nfooter");
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.ENTIRE_TEXT);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "abc.txt");
        runner.enqueue("Hello\nWorld!".getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        final String outContent = new String(out.toByteArray(), StandardCharsets.UTF_8);
        assertEquals("attribute header\n\nabc.txt\n\ndata header\n\nHello\nWorld!\n\nfooter", outContent);
    }

    @Test
    public void testReplaceWithinCurlyBraces() {
        runner.setProperty(ReplaceText.SEARCH_VALUE, ".+");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "{ ${filename} }");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "abc.txt");
        runner.enqueue("Hello".getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("{ abc.txt }");
    }

    @Test
    public void testDefaultReplacement() {
        final String defaultValue = "default-replacement-value";

        // leave the default regex settings
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, defaultValue);

        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue("original-text".getBytes(StandardCharsets.UTF_8), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(defaultValue);
    }

    @Test
    public void testDefaultMultilineReplacement() {
        final String defaultValue = "default-replacement-value";

        // leave the default regex settings
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, defaultValue);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.ENTIRE_TEXT);

        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue(("original-text-line-1" + System.lineSeparator() + "original-text-line-2").getBytes(StandardCharsets.UTF_8), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(defaultValue);
    }

    /* Line by Line */

    @Test
    public void testSimpleLineByLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "odo");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "ood");

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/food.txt")));
    }


    @Test
    public void testZeroByteContentFileLineByLine(@TempDir Path tempDir) throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "odo");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "ood");

        final Path zeroByteFile = tempDir.resolve("zeroByte.txt");
        Files.createFile(zeroByteFile);
        runner.enqueue(translateNewLines(zeroByteFile));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(zeroByteFile));
    }


    @Test
    public void testPrependSimpleLineByLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.PREPEND);
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "TEST ");

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/PrependLineByLineTest.txt")));
    }

    @Test
    public void testAppendSimpleLineByLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.APPEND);
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, " TEST");

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/AppendLineByLineTest.txt")));
    }

    @Test
    public void testAppendEndlineCR() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "TEST");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.APPEND);

        runner.enqueue("Hello \rWorld \r".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("Hello TEST\rWorld TEST\r".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testAppendEndlineCRLF() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "TEST");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.APPEND);

        runner.enqueue("Hello \r\nWorld \r\n".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("Hello TEST\r\nWorld TEST\r\n".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testSimpleLiteral() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "odo");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "ood");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.LITERAL_REPLACE);

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/food.txt")));
    }

    @Test
    public void testLiteralBackReferenceLineByLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "jo");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "[$1]");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.LITERAL_REPLACE);

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/cu[$1]_Po[$1].txt")));
    }

    @Test
    public void testLiteralLineByLine() {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, ".ell.");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "test");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.LITERAL_REPLACE);

        runner.enqueue("H.ell.o, World! .ell. \n .ell. .ell.".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("Htesto, World! test \n test test");
    }

    @Test
    public void testBackReferenceLineByLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(DODO)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "[$1]");

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/[DODO].txt")));
    }

    @Test
    public void testReplacementWithExpressionLanguageIsEscapedLineByLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(jo)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "[${abc}]");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "$1");
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/cu[$1]_Po[$1].txt")));
    }

    @Test
    public void testRegexWithExpressionLanguageLineByLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "${replaceKey}");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${replaceValue}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "Riley");
        attributes.put("replaceValue", "Spider");
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/Spider.txt")));
    }

    @Test
    public void testRegexWithExpressionLanguageIsEscapedLineByLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "${replaceKey}");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${replaceValue}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "R.*y");
        attributes.put("replaceValue", "Spider");
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
    }

    @Test
    public void testBackReferenceWithTooLargeOfIndexIsEscapedLineByLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(lu)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$1$2");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "R.*y");
        attributes.put("replaceValue", "Spiderman");
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/Blu$2e_clu$2e.txt")));
    }


    @Test
    public void testBackReferenceWithTooLargeOfIndexIsEscapedFirstLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.FIRST_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(H)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$1$2");

        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/ReplaceFirstLine.txt")));
    }


    @Test
    public void testBackReferenceWithTooLargeOfIndexIsEscapedLastLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.LAST_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(O)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$1$2");

        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/ReplaceLastLine.txt")));
    }



    @Test
    public void testBackReferenceWithTooLargeOfIndexIsEscapedExceptFirstLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.EXCEPT_FIRST_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(H)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$1$2");

        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/ReplaceExceptFirstLine.txt")));
    }




    @Test
    public void testBackReferenceWithTooLargeOfIndexIsEscapedExceptLastLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.EXCEPT_LAST_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(O)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$1$2");

        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/ReplaceExceptLastLine.txt")));
    }



    @Test
    public void testLiteralBackReferenceFistLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.FIRST_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "H");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "[$1]");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.LITERAL_REPLACE);

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/LiteralReplaceFirstLine.txt")));
    }




    @Test
    public void testLiteralBackReferenceExceptFirstLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.EXCEPT_FIRST_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "H");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "[$1]");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.LITERAL_REPLACE);

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/LiteralReplaceExceptFirstLine.txt")));
    }


    @Test
    public void testLiteralBackReferenceLastLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.LAST_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "O");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "[$1]");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.LITERAL_REPLACE);

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/LiteralReplaceLastLine.txt")));
    }


    @Test
    public void testLiteralBackReferenceExceptLastLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.LINE_BY_LINE_EVALUATION_MODE, ReplaceText.EXCEPT_LAST_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "O");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "[$1]");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.LITERAL_REPLACE);

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/LiteralReplaceExceptLastLine.txt")));
    }

    @Test
    public void testBackReferenceWithInvalidReferenceIsEscapedLineByLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(ew)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$d");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "H.*o");
        attributes.put("replaceValue", "Good-bye");
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/D$d_h$d.txt")));
    }

    @Test
    public void testEscapingDollarSignLineByLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(DO)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "\\$1");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "H.*o");
        attributes.put("replaceValue", "Good-bye");
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/$1$1.txt")));
    }

    @Test
    public void testReplaceWithEmptyStringLineByLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(jo)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "");

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/cu_Po.txt")));
    }

    @Test
    public void testWithNoMatchLineByLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "Z");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "Morning");

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
    }

    @Test
    public void testWithMultipleMatchesLineByLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "l");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "R");

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/BRue_cRue_RiRey.txt")));
    }

    @Test
    public void testAttributeToContentLineByLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, ".*");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${abc}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "Good");
        runner.enqueue(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("Good\nGood\nGood\nGood\nGood\nGood\nGood\nGood\nGood\nGood\nGood");
    }

    @Test
    public void testAttributeToContentWindows() {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, ".*");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${abc}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "Good");
        runner.enqueue("<<<HEADER>>>\r\n<<BODY>>\r\n<<<FOOTER>>>\r".getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("Good\r\nGood\r\nGood\r");
    }

    @Test
    public void testProblematicCase1LineByLine() throws IOException {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, ".*");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${filename}\t${now():format(\"yyyy/MM/dd'T'HHmmss'Z'\")}\t${fileSize}\n");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "abc.txt");
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        final String outContent = translateNewLines(new String(out.toByteArray(), StandardCharsets.UTF_8));
        assertTrue(outContent.startsWith("abc.txt\t"));
        assertTrue(outContent.endsWith("193\n") || outContent.endsWith("203\r\n"));
    }

    @Test
    public void testGetExistingContentLineByLine() {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(?s)(^.*)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "attribute header\n\n${filename}\n\ndata header\n\n$1\n\nfooter\n");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "abc.txt");
        runner.enqueue("Hello\nWorld!".getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        final String outContent = new String(out.toByteArray(), StandardCharsets.UTF_8);
        final String expectedContent = """
                attribute header

                abc.txt

                data header

                Hello


                footer
                attribute header

                abc.txt

                data header

                World!

                footer
                """;
        assertEquals(expectedContent, outContent);
    }

    @Test
    public void testCapturingGroupInExpressionLanguage() {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(.*?),(.*?),(\\d+.*)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$1,$2,${ '$3':toDate('ddMMMyyyy'):format('yyyy/MM/dd') }");

        final String csvIn =
                """
                        2006,10-01-2004,10may2004
                        2007,15-05-2006,10jun2005\r
                        2009,8-8-2008,10aug2008""";
        final String expectedCsvOut =
                """
                        2006,10-01-2004,2004/05/10
                        2007,15-05-2006,2005/06/10\r
                        2009,8-8-2008,2008/08/10""";

        runner.enqueue(csvIn.getBytes());

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(expectedCsvOut);
    }

    @Test
    public void testCapturingGroupInExpressionLanguage2() {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(.*)/(.*?).jpg");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$1/${ '$2':substring(0,1) }.png");

        final String csvIn =
              "1,2,3,https://123.jpg,email@mydomain.com\n"
            + "3,2,1,https://321.jpg,other.email@mydomain.com";
        final String expectedCsvOut =
            "1,2,3,https://1.png,email@mydomain.com\n"
            + "3,2,1,https://3.png,other.email@mydomain.com";

        runner.enqueue(csvIn.getBytes());

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(expectedCsvOut);
    }

    @Test
    public void testAlwaysReplaceEntireText() {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.ENTIRE_TEXT);
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.ALWAYS_REPLACE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "i do not exist anywhere in the text");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${filename}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "abc.txt");
        runner.enqueue("Hello\nWorld!".getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("abc.txt");
    }

    @Test
    public void testAlwaysReplaceLineByLine() {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.ALWAYS_REPLACE);
        runner.setProperty(ReplaceText.SEARCH_VALUE, "i do not exist anywhere in the text");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${filename}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "abc.txt");
        runner.enqueue("Hello\nWorld!\r\ntoday!\n".getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("abc.txt\nabc.txt\r\nabc.txt\n");
    }

    @Test
    public void testRegexWithBadCaptureGroup() {
        // Test the old Default Regex and with a custom Replacement Value that should fail because the
        // Perl regex "(?s:^.*$)" must be written "(?s)(^.*$)" in Java for there to be a capture group.
        //      private static final String DEFAULT_REGEX = "(?s:^.*$)";
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(?s:^.*$)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${'$1':toUpper()}"); // should uppercase group but there is none
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.REGEX_REPLACE);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.ENTIRE_TEXT);

        runner.enqueue("testing\n123".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("");
    }

    @Test
    public void testRegexWithGoodCaptureGroup() {
        // Test the new Default Regex and with a custom Replacement Values that should succeed.
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(?s)(^.*$)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${'$1':toUpper()}"); // will uppercase group with good Java regex
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.REGEX_REPLACE);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.ENTIRE_TEXT);

        runner.enqueue("testing\n123".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("TESTING\n123");
    }

    @Test
    public void testRegexNoCaptureDefaultReplacement() {
        // Test the old Default Regex and new Default Regex with the default replacement.  This should fail
        // because the regex does not create a capture group.
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(?s:^.*$)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$1");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.REGEX_REPLACE);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.ENTIRE_TEXT);

        runner.enqueue("testing\n123".getBytes());
        final AssertionError e = assertThrows(AssertionError.class, () -> runner.run());
        assertInstanceOf(IndexOutOfBoundsException.class, e.getCause());
        assertTrue(e.getMessage().contains("java.lang.IndexOutOfBoundsException: No group 1"));
    }

    @Test
    public void testProcessorConfigurationRegexNotValid() {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(?<!\\),*");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "hello");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.REGEX_REPLACE);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.ENTIRE_TEXT);
        runner.assertNotValid();

        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.LITERAL_REPLACE);
        runner.assertValid();
        runner.enqueue("(?<!\\),*".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("hello");

        runner.setProperty(ReplaceText.SEARCH_VALUE, "");
        runner.assertNotValid();

        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.APPEND);
        runner.assertValid();
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.PREPEND);
        runner.assertValid();
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.ALWAYS_REPLACE);
        runner.assertValid();
    }

    @Test
    public void testSurroundWithEntireText() {
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.SURROUND);
        runner.setProperty(ReplaceText.PREPEND_TEXT, "<pre>");
        runner.setProperty(ReplaceText.APPEND_TEXT, "<post>");
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.ENTIRE_TEXT);

        final String input = "Hello\nThere\nHow are you\nToday?";
        runner.enqueue(input);
        runner.run();
        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);

        final MockFlowFile output = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        output.assertContentEquals("<pre>" + input + "<post>");
    }

    @Test
    public void testSurroundLineByLine() {
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.SURROUND);
        runner.setProperty(ReplaceText.PREPEND_TEXT, "<pre>");
        runner.setProperty(ReplaceText.APPEND_TEXT, "<post>");
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);

        final String input = "Hello\nThere\nHow are you\nToday?";
        runner.enqueue(input);
        runner.run();
        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);

        final MockFlowFile output = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        output.assertContentEquals("<pre>Hello<post>\n<pre>There<post>\n<pre>How are you<post>\n<pre>Today?<post>");
    }


    @Test
    public void testBackReferenceEscapeWithRegexReplaceUsingEL() {
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(?s)(^.*$)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${'$1':toUpper()}");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.REGEX_REPLACE);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.ENTIRE_TEXT);
        runner.assertValid();

        runner.enqueue("wo$rd".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals("WO$RD");

        runner.enqueue("wo$1rd".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 2);
        out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(1);
        out.assertContentEquals("WO$1RD");

        runner.enqueue("wo$1r$2d".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 3);
        out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(2);
        out.assertContentEquals("WO$1R$2D");
    }

    @Test
    public void testSubstituteVariablesWithEvaluationModeEntireText() {
        testSubstituteVariables("Line1: ${var1}\nLine2: ${var2}", "Line1: foo\nLine2: bar", ReplaceText.ENTIRE_TEXT, createAttributesMap());
    }

    @Test
    public void testSubstituteVariablesWithEvaluationModeLineByLine() {
        testSubstituteVariables("Line1: ${var1}\nLine2: ${var2}", "Line1: foo\nLine2: bar", ReplaceText.LINE_BY_LINE, createAttributesMap());
    }

    @Test
    public void testSubstituteVariablesWhenVariableValueMissing() {
        testSubstituteVariables("Line1: ${var1}\nLine2: ${var2}", "Line1: ${var1}\nLine2: ${var2}", ReplaceText.ENTIRE_TEXT, Collections.emptyMap());
    }

    @Test
    public void testSubstituteVariablesWhenVariableReferenceEscaped() {
        testSubstituteVariables("Line1: $${var1}\nLine2: $${var2}", "Line1: ${var1}\nLine2: ${var2}", ReplaceText.ENTIRE_TEXT, createAttributesMap());
    }

    @Test
    public void testSubstituteVariablesWhenVariableNameEmpty() {
        testSubstituteVariables("Line1: ${}\nLine2: ${}", "Line1: ${}\nLine2: ${}", ReplaceText.ENTIRE_TEXT, createAttributesMap());
    }

    private void testSubstituteVariables(String inputContent, String expectedContent, String evaluationMode, Map<String, String> attributesMap) {
        runner.setProperty(ReplaceText.EVALUATION_MODE, evaluationMode);
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.SUBSTITUTE_VARIABLES.getValue());
        runner.enqueue(inputContent, attributesMap);
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).getFirst();
        out.assertContentEquals(expectedContent);
    }

    private Map<String, String> createAttributesMap() {
        final Map<String, String> attributesMap = new HashMap<>();
        attributesMap.put("var1", "foo");
        attributesMap.put("var2", "bar");
        return attributesMap;
    }

    /*
     * A repeated alternation regex such as (A|B)* can lead to StackOverflowError
     * on large input strings.
     */
    @Test
    public void testForStackOverflow() {
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "New text");
        runner.setProperty(ReplaceText.REPLACEMENT_STRATEGY, ReplaceText.REGEX_REPLACE);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.ENTIRE_TEXT);
        runner.setProperty(ReplaceText.MAX_BUFFER_SIZE, "10 MB");
        runner.setProperty(ReplaceText.SEARCH_VALUE, "(?s)(^(A|B)*$)");
        runner.assertValid();

        char[] data = new char[1_000_000];
        Arrays.fill(data, 'A');
        runner.enqueue(new String(data));

        runner.run();

        // we want the large file to fail, rather than rollback and yield
        runner.assertAllFlowFilesTransferred(ReplaceText.REL_FAILURE, 1);
    }

    /**
     * Related to
     * <a href="https://issues.apache.org/jira/browse/NIFI-5761">NIFI-5761</a>. It
     * verifies that if a runtime exception is raised during replace text
     * evaluation, it sends the error to failure relationship.
     */
    @Test
    public void testWithInvalidExpression() {
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.ENTIRE_TEXT);
        runner.setProperty(ReplaceText.SEARCH_VALUE, ".*");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${date:toDate(\"yyyy/MM/dd\")}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("date", "12");
        runner.enqueue("hi", attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_FAILURE).getFirst();
        final String outContent = translateNewLines(new String(out.toByteArray(), StandardCharsets.UTF_8));
        assertEquals("hi", outContent);
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.of("Regular Expression", ReplaceText.SEARCH_VALUE.getName());

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }

    private String translateNewLines(final File file) throws IOException {
        return translateNewLines(file.toPath());
    }

    private String translateNewLines(final Path path) throws IOException {
        final byte[] data = Files.readAllBytes(path);
        final String text = new String(data, StandardCharsets.UTF_8);
        return translateNewLines(text);
    }

    private String translateNewLines(final String text) {
        final String lineSeparator = System.lineSeparator();
        final Pattern pattern = Pattern.compile("\n", Pattern.MULTILINE);
        final String translated = pattern.matcher(text).replaceAll(lineSeparator);
        return translated;
    }
}
