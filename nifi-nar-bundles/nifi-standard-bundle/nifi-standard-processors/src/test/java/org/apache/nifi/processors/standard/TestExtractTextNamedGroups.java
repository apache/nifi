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

import static org.apache.nifi.processors.standard.ExtractText.ENABLE_NAMED_GROUPS;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestExtractTextNamedGroups {

    final String SAMPLE_STRING = "foo\r\nbar1\r\nbar2\r\nbar3\r\nhello\r\nworld\r\n";

    @Test
    public void testProcessor() throws Exception {

        final TestRunner testRunner = TestRunners.newTestRunner(new ExtractText());

        testRunner.setProperty(ENABLE_NAMED_GROUPS, "true");
        testRunner.setProperty("regex.result1", "(?s)(?<ALL>.*)");
        testRunner.setProperty("regex.result2", "(?s).*(?<BAR1>bar1).*");
        testRunner.setProperty("regex.result3", "(?s).*?(?<BAR1>bar\\d).*");
        testRunner.setProperty("regex.result4", "(?s).*?(?:bar\\d).*?(?<BAR2>bar\\d).*?(?<BAR3>bar3).*");
        testRunner.setProperty("regex.result5", "(?s).*(?<BAR3>bar\\d).*");
        testRunner.setProperty("regex.result6", "(?s)^(?<ALL>.*)$");
        testRunner.setProperty("regex.result7", "(?s)(?<MISS>XXX)");
        testRunner.enqueue(SAMPLE_STRING.getBytes(StandardCharsets.UTF_8));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractText.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ExtractText.REL_MATCH).get(0);
        java.util.Map<String,String> attributes = out.getAttributes();
        out.assertAttributeEquals("regex.result1.ALL", SAMPLE_STRING);
        out.assertAttributeEquals("regex.result2.BAR1", "bar1");
        out.assertAttributeEquals("regex.result3.BAR1", "bar1");
        out.assertAttributeEquals("regex.result4.BAR2", "bar2");
        out.assertAttributeEquals("regex.result4.BAR3", "bar3");
        out.assertAttributeEquals("regex.result5.BAR3", "bar3");
        out.assertAttributeEquals("regex.result6.ALL", SAMPLE_STRING);
        out.assertAttributeEquals("regex.result7.MISS", null);
    }

    @Test
    public void testWithUnmatchedOptionalCapturingGroup() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ExtractText());
        testRunner.setProperty(ENABLE_NAMED_GROUPS, "true");
        testRunner.setProperty("regex", "abc(?<DEF>def)?(?<G>g)");
        testRunner.enqueue("abcg");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractText.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ExtractText.REL_MATCH).get(0);
        out.assertAttributeNotExists("regex.DEF");
        out.assertAttributeEquals("regex.G", "g");

        testRunner.clearTransferState();

        testRunner.enqueue("abcdefg");
        testRunner.run();
        final MockFlowFile out2 = testRunner.getFlowFilesForRelationship(ExtractText.REL_MATCH).get(0);
        out2.assertAttributeEquals("regex.DEF", "def");
        out2.assertAttributeEquals("regex.G", "g");
    }

    @Test
    public void testProcessorWithDotall() throws Exception {

        final TestRunner testRunner = TestRunners.newTestRunner(new ExtractText());
        testRunner.setProperty(ENABLE_NAMED_GROUPS, "true");

        testRunner.setProperty(ExtractText.DOTALL, "true");

        testRunner.setProperty("regex.result1", "(?<TOUT>.*)");
        testRunner.setProperty("regex.result2", ".*(?<BAR1>bar1).*");
        testRunner.setProperty("regex.result3", ".*?(?<BAR1>bar\\d).*"); // reluctant gets first
        testRunner.setProperty("regex.result4", ".*?(?:bar\\d).*?(?<BAR2>bar\\d).*"); // reluctant w/ repeated pattern gets second
        testRunner.setProperty("regex.result5", ".*(?<BAR3>bar\\d).*"); // greedy gets last
        testRunner.setProperty("regex.result6", "^(?<TOUT>.*)$");
        testRunner.setProperty("regex.result7", "^(?<NO>XXX)$");

        testRunner.enqueue(SAMPLE_STRING.getBytes(StandardCharsets.UTF_8));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractText.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ExtractText.REL_MATCH).get(0);
        out.assertAttributeEquals("regex.result1.TOUT", SAMPLE_STRING);
        out.assertAttributeEquals("regex.result2.BAR1", "bar1");
        out.assertAttributeEquals("regex.result3.BAR1", "bar1");
        out.assertAttributeEquals("regex.result4.BAR2", "bar2");
        out.assertAttributeEquals("regex.result5.BAR3", "bar3");
        out.assertAttributeEquals("regex.result6.TOUT", SAMPLE_STRING);
        out.assertAttributeEquals("regex.result7.NO", null);

    }

    @Test
    public void testProcessorWithMultiline() throws Exception {

        final TestRunner testRunner = TestRunners.newTestRunner(new ExtractText());
        testRunner.setProperty(ENABLE_NAMED_GROUPS, "true");

        testRunner.setProperty(ExtractText.MULTILINE, "true");

        testRunner.setProperty("regex.result1", "(?<ALL>.*)");
        testRunner.setProperty("regex.result2", "(?<BAR1>bar1)");
        testRunner.setProperty("regex.result3", ".*?(?<BAR1>bar\\d).*");
        testRunner.setProperty("regex.result4", ".*?(?:bar\\d).*?(?<NULL>bar\\d).*");
        testRunner.setProperty("regex.result4b", "bar\\d\\r\\n(?<BAR2>bar\\d)");
        testRunner.setProperty("regex.result5", ".*(?<BAR2>bar\\d).*");
        testRunner.setProperty("regex.result5b", "(?:bar\\d\\r?\\n)*(?<BAR3>bar\\d)");
        testRunner.setProperty("regex.result6", "^(?<ALL>.*)$");
        testRunner.setProperty("regex.result7", "^(?<NO>XXX)$");

        testRunner.enqueue(SAMPLE_STRING.getBytes(StandardCharsets.UTF_8));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractText.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ExtractText.REL_MATCH).get(0);
        out.assertAttributeEquals("regex.result1.ALL", "foo"); // matches everything on the first line
        out.assertAttributeEquals("regex.result2.BAR1", "bar1");
        out.assertAttributeEquals("regex.result3.BAR1", "bar1");
        out.assertAttributeEquals("regex.result4.NULL", null); // null because no line has two bar's
        out.assertAttributeEquals("regex.result4b.BAR2", "bar2"); // included newlines in regex
        out.assertAttributeEquals("regex.result5.BAR2", "bar1"); //still gets first because no lines with multiple bar's
        out.assertAttributeEquals("regex.result5b.BAR3", "bar3"); // included newlines in regex
        out.assertAttributeEquals("regex.result6.ALL", "foo"); // matches all of first line
        out.assertAttributeEquals("regex.result7.NO", null); // no match
    }

    @Test
    public void testProcessorWithMultilineAndDotall() throws Exception {

        final TestRunner testRunner = TestRunners.newTestRunner(new ExtractText());
        testRunner.setProperty(ENABLE_NAMED_GROUPS, "true");

        testRunner.setProperty(ExtractText.MULTILINE, "true");
        testRunner.setProperty(ExtractText.DOTALL, "true");

        testRunner.setProperty("regex.result1", "(?<ALL>.*)");
        testRunner.setProperty("regex.result2", "(?<BAR1>bar1)");
        testRunner.setProperty("regex.result3", ".*?(?<BAR1>bar\\d).*");
        testRunner.setProperty("regex.result4", ".*?(?:bar\\d).*?(?<BAR2>bar\\d).*");
        testRunner.setProperty("regex.result4b", "bar\\d\\r\\n(?<BAR2>bar\\d)");
        testRunner.setProperty("regex.result5", ".*(?<BAR3>bar\\d).*");
        testRunner.setProperty("regex.result5b", "(?:bar\\d\\r?\\n)*(?<BAR3>bar\\d)");
        testRunner.setProperty("regex.result6", "^(?<ALL>.*)$");
        testRunner.setProperty("regex.result7", "^(?<MISS>XXX)$");

        testRunner.enqueue(SAMPLE_STRING.getBytes(StandardCharsets.UTF_8));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractText.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ExtractText.REL_MATCH).get(0);

        out.assertAttributeEquals("regex.result1.ALL", SAMPLE_STRING);
        out.assertAttributeEquals("regex.result2.BAR1", "bar1");
        out.assertAttributeEquals("regex.result3.BAR1", "bar1");
        out.assertAttributeEquals("regex.result4.BAR2", "bar2");
        out.assertAttributeEquals("regex.result4b.BAR2", "bar2");
        out.assertAttributeEquals("regex.result5.BAR3", "bar3");
        out.assertAttributeEquals("regex.result5b.BAR3", "bar3");
        out.assertAttributeEquals("regex.result6.ALL", SAMPLE_STRING);
        out.assertAttributeEquals("regex.result7.MISS", null);
    }

    @Test
    public void testProcessorWithNoMatches() throws Exception {

        final TestRunner testRunner = TestRunners.newTestRunner(new ExtractText());
        testRunner.setProperty(ENABLE_NAMED_GROUPS, "true");

        testRunner.setProperty(ExtractText.MULTILINE, "true");
        testRunner.setProperty(ExtractText.DOTALL, "true");

        testRunner.setProperty("regex.result2", "(?<NONE>bar1)");
        testRunner.setProperty("regex.result3", ".*?(?<NONE>bar\\d).*");
        testRunner.setProperty("regex.result4", ".*?(?:bar\\d).*?(?<NONE>bar\\d).*");
        testRunner.setProperty("regex.result4b", "bar\\d\\r\\n(?<NONE>bar\\d)");
        testRunner.setProperty("regex.result5", ".*(?<NONE>bar\\d).*");
        testRunner.setProperty("regex.result5b", "(?:bar\\d\\r?\\n)*(?<NONE>bar\\d)");
        testRunner.setProperty("regex.result7", "^(?<NONE>XXX)$");

        testRunner.enqueue("YYY".getBytes(StandardCharsets.UTF_8));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractText.REL_NO_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ExtractText.REL_NO_MATCH).get(0);

        out.assertAttributeEquals("regex.result1.NONE", null);
        out.assertAttributeEquals("regex.result2.NONE", null);
        out.assertAttributeEquals("regex.result3.NONE", null);
        out.assertAttributeEquals("regex.result4.NONE", null);
        out.assertAttributeEquals("regex.result4b.NONE", null);
        out.assertAttributeEquals("regex.result5.NONE", null);
        out.assertAttributeEquals("regex.result5b.NONE", null);
        out.assertAttributeEquals("regex.result6.NONE", null);
        out.assertAttributeEquals("regex.result7.NONE", null);
    }

    @Test
    public void testNoFlowFile() throws UnsupportedEncodingException {
        final TestRunner testRunner = TestRunners.newTestRunner(new ExtractText());
        testRunner.setProperty(ENABLE_NAMED_GROUPS, "true");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ExtractText.REL_MATCH, 0);

    }

    @Test
    public void testMatchOutsideBuffer() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new ExtractText());
        testRunner.setProperty(ENABLE_NAMED_GROUPS, "true");

        testRunner.setProperty(ExtractText.MAX_BUFFER_SIZE, "3 B");//only read the first 3 chars ("foo")

        testRunner.setProperty("regex.result1", "(?<FOO>foo)");
        testRunner.setProperty("regex.result2", "(?<WORLD>world)");

        testRunner.enqueue(SAMPLE_STRING.getBytes(StandardCharsets.UTF_8));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractText.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ExtractText.REL_MATCH).get(0);

        out.assertAttributeEquals("regex.result1.FOO", "foo");
        out.assertAttributeEquals("regex.result2.WORLD", null); // null because outsk
    }

    @Test
    public void testIncludeZeroCaptureGroupProperty() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new ExtractText());
        testRunner.setProperty(ENABLE_NAMED_GROUPS, "true");

        final String attributeKey = "regex.result";

        testRunner.setProperty(attributeKey, "(?s)(?<ALL>.*)");

        testRunner.enqueue(SAMPLE_STRING.getBytes(StandardCharsets.UTF_8));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractText.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ExtractText.REL_MATCH).get(0);

        // Ensure the zero capture group is in the resultant attributes
        out.assertAttributeExists(attributeKey);
        out.assertAttributeExists(attributeKey + ".ALL");
        out.assertAttributeEquals(attributeKey, SAMPLE_STRING);
        out.assertAttributeEquals(attributeKey + ".ALL", SAMPLE_STRING);
    }

    @Test
    public void testFindAll() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new ExtractText());
        testRunner.setProperty(ENABLE_NAMED_GROUPS, "true");
        testRunner.setProperty(ExtractText.ENABLE_REPEATING_CAPTURE_GROUP, "true");
        final String attributeKey = "regex.result";
        testRunner.setProperty(attributeKey, "(?s)(?<W>\\w+)");
        testRunner.enqueue("This is my text".getBytes(StandardCharsets.UTF_8));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ExtractText.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ExtractText.REL_MATCH).get(0);
        // Ensure the zero capture group is in the resultant attributes
        out.assertAttributeExists(attributeKey);
        out.assertAttributeExists(attributeKey + ".W");
        out.assertAttributeExists(attributeKey + ".W.1");
        out.assertAttributeExists(attributeKey + ".W.2");
        out.assertAttributeExists(attributeKey + ".W.3");
        out.assertAttributeEquals(attributeKey, "This");
        out.assertAttributeEquals(attributeKey + ".W", "This");
        out.assertAttributeEquals(attributeKey + ".W.1", "is");
        out.assertAttributeEquals(attributeKey + ".W.2", "my");
        out.assertAttributeEquals(attributeKey + ".W.3", "text");
    }

    @Test
    public void testFindAllPair() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new ExtractText());
        testRunner.setProperty(ENABLE_NAMED_GROUPS, "true");
        testRunner.setProperty(ExtractText.ENABLE_REPEATING_CAPTURE_GROUP, "true");
        final String attributeKey = "regex.result";
        testRunner.setProperty(attributeKey, "(?<LEFT>\\w+)=(?<RIGHT>\\d+)");
        testRunner.enqueue("a=1,b=10,c=100".getBytes(StandardCharsets.UTF_8));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ExtractText.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ExtractText.REL_MATCH).get(0);
        // Ensure the zero capture group is in the resultant attributes
        out.assertAttributeExists(attributeKey);
        out.assertAttributeExists(attributeKey + ".LEFT");
        out.assertAttributeExists(attributeKey + ".RIGHT");
        out.assertAttributeExists(attributeKey + ".LEFT.1");
        out.assertAttributeExists(attributeKey + ".RIGHT.1");
        out.assertAttributeExists(attributeKey + ".LEFT.2");
        out.assertAttributeExists(attributeKey + ".RIGHT.2");
        out.assertAttributeNotExists(attributeKey + ".LEFT.3"); // Ensure there's no more attributes
        out.assertAttributeNotExists(attributeKey + ".RIGHT.3"); // Ensure there's no more attributes
        out.assertAttributeEquals(attributeKey , "a=1");
        out.assertAttributeEquals(attributeKey + ".LEFT", "a");
        out.assertAttributeEquals(attributeKey + ".RIGHT", "1");
        out.assertAttributeEquals(attributeKey + ".LEFT.1", "b");
        out.assertAttributeEquals(attributeKey + ".RIGHT.1", "10");
        out.assertAttributeEquals(attributeKey + ".LEFT.2", "c");
        out.assertAttributeEquals(attributeKey + ".RIGHT.2", "100");
    }

    @Test
    public void testIgnoreZeroCaptureGroupProperty() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new ExtractText());
        testRunner.setProperty(ENABLE_NAMED_GROUPS, "true");

        testRunner.setProperty(ExtractText.INCLUDE_CAPTURE_GROUP_ZERO, "false");

        final String attributeKey = "regex.result";

        testRunner.setProperty(attributeKey, "(?s)(?<ALL>.*)");

        testRunner.enqueue(SAMPLE_STRING.getBytes(StandardCharsets.UTF_8));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractText.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ExtractText.REL_MATCH).get(0);

        // Ensure the zero capture group is not in the resultant attributes
        out.assertAttributeNotExists(attributeKey);
        out.assertAttributeEquals(attributeKey + ".ALL", SAMPLE_STRING);
    }

    @Test
    public void testShouldAllowNoCaptureGroups() throws Exception {
        // Arrange
        final TestRunner testRunner = TestRunners.newTestRunner(new ExtractText());
        testRunner.setProperty(ENABLE_NAMED_GROUPS, "true");
        final String attributeKey = "regex.result";
        testRunner.setProperty(attributeKey, "(?s).*");

        // Act
        testRunner.enqueue(SAMPLE_STRING.getBytes(StandardCharsets.UTF_8));
        testRunner.run();

        // Assert
        testRunner.assertAllFlowFilesTransferred(ExtractText.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ExtractText.REL_MATCH).get(0);

        // There is no global capture group, but no named capture group either
        // so attributeKey has the match
        out.assertAttributeEquals(attributeKey , SAMPLE_STRING);
    }

    @Test(expected = AssertionError.class)
    public void testShouldNotAllowNoCaptureGroupsIfZeroDisabled() throws Exception {
        // Arrange
        final TestRunner testRunner = TestRunners.newTestRunner(new ExtractText());
        testRunner.setProperty(ENABLE_NAMED_GROUPS, "true");
        testRunner.setProperty(ExtractText.INCLUDE_CAPTURE_GROUP_ZERO, "false");
        final String attributeKey = "regex.result";
        testRunner.setProperty(attributeKey, "(?s).*");

        // Act
        testRunner.enqueue(SAMPLE_STRING.getBytes(StandardCharsets.UTF_8));

        // Validation should fail because nothing will match
        testRunner.run();
    }

    @Test(expected = AssertionError.class)
    public void testInvalidIfGroupCountsDoNotMatch() {
        final TestRunner testRunner = TestRunners.newTestRunner(new ExtractText());
        testRunner.setProperty(ENABLE_NAMED_GROUPS, "true");
        testRunner.setProperty(ExtractText.INCLUDE_CAPTURE_GROUP_ZERO, "false");
        final String attributeKey = "notValidOne";
        testRunner.setProperty(attributeKey,"^(beginning)\\s(middle)\\s(?<END>end)$");

        // Act
        testRunner.enqueue("beginning middle end".getBytes(StandardCharsets.UTF_8));

        // Validation should fail because number of groups does not match number of named groups
        testRunner.run();
    }
}
