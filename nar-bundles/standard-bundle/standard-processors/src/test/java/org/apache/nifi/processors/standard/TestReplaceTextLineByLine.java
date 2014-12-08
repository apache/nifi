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

import org.apache.nifi.processors.standard.ReplaceText;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Assert;
import org.junit.Test;

public class TestReplaceTextLineByLine {

    @Test
    public void testSimple() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REGEX, "odo");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "ood");

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/food.txt")));
    }

    @Test
    public void testBackReference() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REGEX, "(DODO)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "[$1]");

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/[DODO].txt")));
    }

    @Test
    public void testReplacementWithExpressionLanguageIsEscaped() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REGEX, "(jo)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "[${abc}]");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "$1");
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/cu[$1]_Po[$1].txt")));
    }

    @Test
    public void testRegexWithExpressionLanguage() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REGEX, "${replaceKey}");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${replaceValue}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "Riley");
        attributes.put("replaceValue", "Spider");
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/Spider.txt")));
    }

    @Test
    public void testRegexWithExpressionLanguageIsEscaped() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REGEX, "${replaceKey}");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${replaceValue}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "R.*y");
        attributes.put("replaceValue", "Spider");
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
    }

    @Test
    public void testBackReferenceWithTooLargeOfIndexIsEscaped() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REGEX, "(lu)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$1$2");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "R.*y");
        attributes.put("replaceValue", "Spiderman");
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/Blu$2e_clu$2e.txt")));
    }

    @Test
    public void testBackReferenceWithInvalidReferenceIsEscaped() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REGEX, "(ew)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "$d");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "H.*o");
        attributes.put("replaceValue", "Good-bye");
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/D$d_h$d.txt")));
    }

    @Test
    public void testEscapingDollarSign() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REGEX, "(DO)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "\\$1");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("replaceKey", "H.*o");
        attributes.put("replaceValue", "Good-bye");
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/$1$1.txt")));
    }

    @Test
    public void testReplaceWithEmptyString() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REGEX, "(jo)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "");

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/cu_Po.txt")));
    }

    @Test
    public void testWithNoMatch() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REGEX, "Z");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "Morning");

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
    }

    @Test
    public void testWithMultipleMatches() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REGEX, "l");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "R");

        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/BRue_cRue_RiRey.txt")));
    }

    @Test
    public void testAttributeToContent() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REGEX, ".*");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${abc}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "Good");
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestReplaceTextLineByLine/Good.txt")));
    }

    @Test
    public void testAttributeToContentWindows() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REGEX, ".*");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${abc}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "Good");
        runner.enqueue("<<<HEADER>>>\r\n<<BODY>>\r\n<<<FOOTER>>>\r".getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        out.assertContentEquals("GoodGoodGood");
    }

    @Test
    public void testProblematicCase1() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REGEX, ".*");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "${filename}\t${now():format(\"yyyy/MM/dd'T'HHmmss'Z'\")}\t${fileSize}\n");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "abc.txt");
        runner.enqueue(translateNewLines(Paths.get("src/test/resources/TestReplaceTextLineByLine/testFile.txt")), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        final String outContent = translateNewLines(new String(out.toByteArray(), StandardCharsets.UTF_8));
        Assert.assertTrue(outContent.startsWith("abc.txt\t"));
        System.out.println(outContent);
        Assert.assertTrue(outContent.endsWith("193\n") || outContent.endsWith("203\r\n"));
    }

    @Test
    public void testGetExistingContent() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ReplaceText.EVALUATION_MODE, ReplaceText.LINE_BY_LINE);
        runner.setProperty(ReplaceText.REGEX, "(?s)(^.*)");
        runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "attribute header\n\n${filename}\n\ndata header\n\n$1\n\nfooter\n");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "abc.txt");
        runner.enqueue("Hello\nWorld!".getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
        final String outContent = new String(out.toByteArray(), StandardCharsets.UTF_8);
        System.out.println(outContent);
        Assert.assertTrue(outContent.equals("attribute header\n\nabc.txt\n\ndata header\n\nHello\n\n\nfooter\n"
                + "attribute header\n\nabc.txt\n\ndata header\n\nWorld!\n\nfooter\n"));

    }

    private byte[] translateNewLines(final File file) throws IOException {
        return translateNewLines(file.toPath());
    }

    private byte[] translateNewLines(final Path path) throws IOException {
        final byte[] data = Files.readAllBytes(path);
        final String text = new String(data, StandardCharsets.UTF_8);
        return translateNewLines(text).getBytes(StandardCharsets.UTF_8);
    }

    private String translateNewLines(final String text) {
        final String lineSeparator = System.getProperty("line.separator");
        final Pattern pattern = Pattern.compile("\n", Pattern.MULTILINE);
        final String translated = pattern.matcher(text).replaceAll(lineSeparator);
        return translated;
    }
}
