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
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class TestReplaceText {

    @Test
    public void testSimple() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, "ell");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "lle");

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("Hlleo, World!".getBytes("UTF-8"));
    }

    @Test
    public void testBackReference() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, "(ell)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "[$1]");

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("H[ell]o, World!");
    }

    @Test
    public void testBackRefFollowedByNumbers() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        String expected = "Hell23o, World!";
        runner.setProperty(ReplaceText.REGEX, "(ell)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$123");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "notSupported");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        final String actual = new String(out.toByteArray(), StandardCharsets.UTF_8);
        System.out.println(actual);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testBackRefWithNoCapturingGroup() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, "ell");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$0123");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "notSupported");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        final String actual = new String(out.toByteArray(), StandardCharsets.UTF_8);
        Assert.assertEquals("Hell123o, World!", actual);
    }

    @Test
    public void testAmy3() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, "${replaceKey}");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "GoodBye");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "H.*o");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("Hello, World!");
    }

    @Test
    public void testReplacementWithExpressionLanguageIsEscaped() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, "(ell)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "[${abc}]");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "$1");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("H[$1]o, World!");
    }

    @Test
    public void testRegexWithExpressionLanguage() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, "${replaceKey}");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${replaceValue}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "Hello");
        attributes.put("replaceValue", "Good-bye");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("Good-bye, World!");
    }

    @Test
    public void testRegexWithExpressionLanguageIsEscaped() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, "${replaceKey}");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${replaceValue}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "H.*o");
        attributes.put("replaceValue", "Good-bye");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("Hello, World!");
    }

    @Test
    public void testBackReferenceWithTooLargeOfIndexIsEscaped() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, "(ell)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$1$2");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "H.*o");
        attributes.put("replaceValue", "Good-bye");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("Hell$2o, World!");
    }

    @Test
    public void testBackReferenceWithInvalidReferenceIsEscaped() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, "(ell)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$d");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "H.*o");
        attributes.put("replaceValue", "Good-bye");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("H$do, World!");
    }

    @Test
    public void testEscapingDollarSign() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, "(ell)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "\\$1");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "H.*o");
        attributes.put("replaceValue", "Good-bye");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("H$1o, World!");
    }

    @Test
    public void testReplaceWithEmptyString() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, "(ell)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "");

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("Ho, World!");
    }

    @Test
    public void testWithNoMatch() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, "Z");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "Morning");

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("Hello, World!");
    }

    @Test
    public void testWithMultipleMatches() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, "l");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "R");

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("HeRRo, WorRd!");
    }

    @Test
    public void testAttributeToContent() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, ".*");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${abc}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "Good");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("Good");
    }

    @Test
    public void testRoutesToFailureIfTooLarge() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, "[123]");
        runner.setProperty(ReplaceText.MAX_BUFFER_SIZE, "1 b");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${abc}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "Good");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_FAILURE, 1);
    }

    @Test
    public void testRoutesToSuccessIfTooLargeButRegexIsDotAsterisk() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, ".*");
        runner.setProperty(ReplaceText.MAX_BUFFER_SIZE, "1 b");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${abc}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "Good");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("Good");
    }

    @Test
    public void testProblematicCase1() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, ".*");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${filename}\t${now():format(\"yyyy/MM/dd'T'HHmmss'Z'\")}\t${fileSize}\n");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "abc.txt");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        final String outContent = new String(out.toByteArray(), StandardCharsets.UTF_8);
        Assert.assertTrue(outContent.startsWith("abc.txt\t"));
        System.out.println(outContent);
        Assert.assertTrue(outContent.endsWith("13\n"));
    }

    @Test
    public void testGetExistingContent() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, "(?s)(^.*)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "attribute header\n\n${filename}\n\ndata header\n\n$1\n\nfooter");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "abc.txt");
        runner.enqueue("Hello\nWorld!".getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        final String outContent = new String(out.toByteArray(), StandardCharsets.UTF_8);
        Assert.assertTrue(outContent.equals("attribute header\n\nabc.txt\n\ndata header\n\nHello\nWorld!\n\nfooter"));
        System.out.println(outContent);
    }

    @Test
    public void testReplaceWithinCurlyBraces() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REGEX, ".+");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "{ ${filename} }");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "abc.txt");
        runner.enqueue("Hello".getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("{ abc.txt }");
    }

    @Test
    public void testDefaultReplacement() throws Exception {
        final String defaultValue = "default-replacement-value";

        // leave the default regex settings
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, defaultValue);

        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue("original-text".getBytes(StandardCharsets.UTF_8), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals(defaultValue);
    }

    @Test
    public void testDefaultMultilineReplacement() throws Exception {
        final String defaultValue = "default-replacement-value";

        // leave the default regex settings
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, defaultValue);

        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue(("original-text-line-1" + System.lineSeparator() + "original-text-line-2").getBytes(StandardCharsets.UTF_8), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals(defaultValue);
    }
}
