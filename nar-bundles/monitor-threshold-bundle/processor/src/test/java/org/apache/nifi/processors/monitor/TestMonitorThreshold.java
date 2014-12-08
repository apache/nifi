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
package org.apache.nifi.processors.monitor;
/*
 * NOTE: The term "rule" is synonymous with "threshold" in these tests.
 */

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.monitor.MonitorThreshold.CompareRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessorLog;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestMonitorThreshold {

    private TestRunner runner;

    private final File primaryTestFile = new File("src/test/resources/testFile"); //Has 1,668 bytes
    private final int PRIMARY_TEST_FILE_SIZE = (int) primaryTestFile.length();
    private FileInputStream primaryTestFileStream;

    public static final String DEFAULT_DELIMITER = ".";
    public static final String CATEGORY_ATTRIBUTE_NAME = "category";
    public static final String VALUE_1234 = "1234";
    public static final String VALUE_2345 = "2345";
    public static final String VALUE_3456 = "3456";
    public static final String VALUE_DEFAULT = MonitorThreshold.DEFAULT_THRESHOLDS_KEY;

    public static final String fileCount_category_2345_key = MonitorThreshold.FILE_COUNT_PREFIX + DEFAULT_DELIMITER + CATEGORY_ATTRIBUTE_NAME + DEFAULT_DELIMITER + VALUE_2345;
    public static final String fileThreshold_category_2345_key = MonitorThreshold.FILE_THRESHOLD_PREFIX + DEFAULT_DELIMITER + CATEGORY_ATTRIBUTE_NAME + DEFAULT_DELIMITER + VALUE_2345;

    public static final String byteCount_category_2345_key = MonitorThreshold.BYTE_COUNT_PREFIX + DEFAULT_DELIMITER + CATEGORY_ATTRIBUTE_NAME + DEFAULT_DELIMITER + VALUE_2345;
    public static final String byteThreshold_category_2345_key = MonitorThreshold.BYTE_THRESHOLD_PREFIX + DEFAULT_DELIMITER + CATEGORY_ATTRIBUTE_NAME + DEFAULT_DELIMITER + VALUE_2345;

    public static final String fileCount_category_default_key = MonitorThreshold.FILE_COUNT_PREFIX + DEFAULT_DELIMITER + CATEGORY_ATTRIBUTE_NAME + DEFAULT_DELIMITER + VALUE_DEFAULT;
    public static final String fileThreshold_category_default_key = MonitorThreshold.FILE_THRESHOLD_PREFIX + DEFAULT_DELIMITER + CATEGORY_ATTRIBUTE_NAME + DEFAULT_DELIMITER + VALUE_DEFAULT;

    public static final String byteCount_category_default_key = MonitorThreshold.BYTE_COUNT_PREFIX + DEFAULT_DELIMITER + CATEGORY_ATTRIBUTE_NAME + DEFAULT_DELIMITER + VALUE_DEFAULT;
    public static final String byteThreshold_category_default_key = MonitorThreshold.BYTE_THRESHOLD_PREFIX + DEFAULT_DELIMITER + CATEGORY_ATTRIBUTE_NAME + DEFAULT_DELIMITER + VALUE_DEFAULT;

    public static final String fileCount_category_3456_key = MonitorThreshold.FILE_COUNT_PREFIX + DEFAULT_DELIMITER + CATEGORY_ATTRIBUTE_NAME + DEFAULT_DELIMITER + VALUE_3456;
    public static final String fileThreshold_category_3456_key = MonitorThreshold.FILE_THRESHOLD_PREFIX + DEFAULT_DELIMITER + CATEGORY_ATTRIBUTE_NAME + DEFAULT_DELIMITER + VALUE_3456;

    public static final String byteCount_category_3456_key = MonitorThreshold.BYTE_COUNT_PREFIX + DEFAULT_DELIMITER + CATEGORY_ATTRIBUTE_NAME + DEFAULT_DELIMITER + VALUE_3456;
    public static final String byteThreshold_category_3456_key = MonitorThreshold.BYTE_THRESHOLD_PREFIX + DEFAULT_DELIMITER + CATEGORY_ATTRIBUTE_NAME + DEFAULT_DELIMITER + VALUE_3456;

    public static final String PRIORITY_ATTRIBUTE_NAME = "priority";
    public static final String VALUE_1 = "1";
    public static final String fileCount_priority_1_key = MonitorThreshold.FILE_COUNT_PREFIX + DEFAULT_DELIMITER + PRIORITY_ATTRIBUTE_NAME + DEFAULT_DELIMITER + VALUE_1;
    public static final String fileThreshold_priority_1_key = MonitorThreshold.FILE_THRESHOLD_PREFIX + DEFAULT_DELIMITER + PRIORITY_ATTRIBUTE_NAME + DEFAULT_DELIMITER + VALUE_1;

    public static final String byteCount_priority_1_key = MonitorThreshold.BYTE_COUNT_PREFIX + DEFAULT_DELIMITER + PRIORITY_ATTRIBUTE_NAME + DEFAULT_DELIMITER + VALUE_1;
    public static final String byteThreshold_priority_1_key = MonitorThreshold.BYTE_THRESHOLD_PREFIX + DEFAULT_DELIMITER + PRIORITY_ATTRIBUTE_NAME + DEFAULT_DELIMITER + VALUE_1;

    public static final String APPORTIONMENT_ATTRIBUTE_NAME = "apportionment";
    public static final String IRRELEVANT_ATTRIBUTE_NAME_1 = "irrelevant_1";
    public static final String IRRELEVANT_ATTRIBUTE_NAME_2 = "irrelevant_2";

    @SuppressWarnings("serial")
    final Map<String, String> attributes_with_category_1234 = new HashMap<String, String>() {
        {
            put(CATEGORY_ATTRIBUTE_NAME, VALUE_1234);
        }
    };

    @SuppressWarnings("serial")
    final Map<String, String> attributes_with_category_2345 = new HashMap<String, String>() {
        {
            put(CATEGORY_ATTRIBUTE_NAME, VALUE_2345);
        }
    };

    @SuppressWarnings("serial")
    final Map<String, String> attributes_with_category_value_empty = new HashMap<String, String>() {
        {
            put(CATEGORY_ATTRIBUTE_NAME, "");
        }
    };

    @SuppressWarnings("serial")
    final Map<String, String> attributes_with_category_value_single_space = new HashMap<String, String>() {
        {
            put(CATEGORY_ATTRIBUTE_NAME, " ");
        }
    };

    @SuppressWarnings("serial")
    final Map<String, String> attributes_with_category_value_double_space = new HashMap<String, String>() {
        {
            put(CATEGORY_ATTRIBUTE_NAME, "  ");
        }
    };

    @SuppressWarnings("serial")
    final Map<String, String> attributes_with_category_value_triple_space = new HashMap<String, String>() {
        {
            put(CATEGORY_ATTRIBUTE_NAME, "   ");
        }
    };

    @SuppressWarnings("serial")
    final Map<String, String> attributes_with_category_3456 = new HashMap<String, String>() {
        {
            put(CATEGORY_ATTRIBUTE_NAME, VALUE_3456);
        }
    };

    @SuppressWarnings("serial")
    final Map<String, String> attributes_that_do_not_have_thresholds = new HashMap<String, String>() {
        {
            put(IRRELEVANT_ATTRIBUTE_NAME_1, "0123456789");
            put(IRRELEVANT_ATTRIBUTE_NAME_2, "9876543210");
        }
    };

    private static final String processorId = UUID.randomUUID().toString();
    private static ProcessorLog logger = null;

    // threshold files.
    private final String threshold_settings_allow_0_files = "src/test/resources/threshold_settings_allow_0.xml";
    private final String threshold_settings_allow_1669_bytes = "src/test/resources/threshold_settings_allow_1669_bytes.xml";
    private final String threshold_settings_allow_1_file = "src/test/resources/threshold_settings_allow_1.xml";
    private final String threshold_settings_allow_2_files = "src/test/resources/threshold_settings_allow_2.xml";
    private final String threshold_settings_allow_3336_bytes_default = "src/test/resources/threshold_settings_allow_3336_bytes_default.xml";
    private final String threshold_settings_allow_10_KB = "src/test/resources/threshold_settings_allow_10_KB.xml";
    private final String threshold_settings_allow_10_KB_And_2_KB = "src/test/resources/threshold_settings_allow_10_KB_and_2_KB.xml";
    private final String threshold_settings_allow_2_Default = "src/test/resources/threshold_settings_allow_2_default.xml";
    private final String threshold_settings_with_empty_attribute_value = "src/test/resources/threshold_settings_with_empty_attribute_value.xml";
    private final String threshold_settings_with_spaces_for_attribute_values = "src/test/resources/threshold_settings_with_spaces_for_attribute_values.xml";

    @BeforeClass
    public static void setUpClass() throws NoSuchMethodException, SecurityException {
    }

    @Before
    public void setUp() throws IOException, InitializationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        runner = TestRunners.newTestRunner(new MonitorThreshold());
        TestMonitorThreshold.logger = new MockProcessorLog(processorId, runner.getProcessor());
        runner.setProperty(MonitorThreshold.OPT_COUNT_RESET_MINUTES, "1");
        runner.setProperty(MonitorThreshold.OPT_COUNTS_PERSISTENCE_FILE_PREFIX, "target/counts");
        runner.setProperty(MonitorThreshold.OPT_ADD_ATTRIBUTES, MonitorThreshold.ONLY_WHEN_THRESHOLD_EXCEEDED);

        primaryTestFileStream = new FileInputStream(primaryTestFile);
    }

    @After
    public void tearDown() throws IOException {
        primaryTestFileStream.close();
        runner.clearTransferState();
    }

    //@Ignore("For now...")
    @Test
    public void testAccuratelyReflectsFileThresholdNotExceeded() throws IOException {
        runner.enqueue(primaryTestFileStream, attributes_with_category_2345);
        runner.setAnnotationData(getFileContents(threshold_settings_allow_1_file));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="32000" count="6">Default</noMatchRule>
        //    		<rule id="2345" size="1000000000" count="1"></rule>
        //    	</flowFileAttribute>
        // </configuration>
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 1);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        for (FlowFile flowFile : flowFiles) {
            // since no thresholds exceeded, additional attributes should not be present
            String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
            assertEquals(fileCount_category_2345, null);
            String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
            assertEquals(fileThreshold_category_2345, null);

            String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
            assertEquals(byteCount_category_2345, null);
            String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
            assertEquals(byteThreshold_category_2345, null);
        }

        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    //@Ignore("For now...")
    @Test
    public void testAccuratelyReflectsByteThresholdNotExceeded() throws IOException {
        runner.enqueue(primaryTestFileStream, attributes_with_category_2345);
        runner.setAnnotationData(getFileContents(threshold_settings_allow_1669_bytes));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="320000" count="6">Default</noMatchRule>
        //    		<rule id="2345" size="1669" count="2"></rule>
        //    	</flowFileAttribute>
        //  </configuration>
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 1);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        for (FlowFile flowFile : flowFiles) {

            // since no thresholds exceeded, additional attributes should not be present
            String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
            assertEquals(fileCount_category_2345, null);
            String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
            assertEquals(fileThreshold_category_2345, null);

            String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
            assertEquals(byteCount_category_2345, null);
            String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
            assertEquals(byteThreshold_category_2345, null);
        }

        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());

        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    //@Ignore("For now...")
    @Test
    public void testDoesNotExceedWhenNoThresholdsApply() throws IOException {
        runner.enqueue(primaryTestFileStream, attributes_that_do_not_have_thresholds); // category attribute not present on this FlowFile
        runner.setAnnotationData(getFileContents(threshold_settings_allow_1_file));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="32000" count="6">Default</noMatchRule>
        //    		<rule id="2345" size="1000000000" count="1"></rule>
        //    	</flowFileAttribute>
        // </configuration>
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 1);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        for (FlowFile flowFile : flowFiles) {
            // since no thresholds exceeded, additional attributes should not be present
            String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
            assertEquals(fileCount_category_2345, null);
            String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
            assertEquals(fileThreshold_category_2345, null);

            String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
            assertEquals(byteCount_category_2345, null);
            String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
            assertEquals(byteThreshold_category_2345, null);
        }

        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());

        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    //@Ignore("For now...")
    @Test
    public void testNotifiesWhenExceedsFileThresholdOf_0() throws IOException {
        runner.setAnnotationData(getFileContents(threshold_settings_allow_0_files));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="32000" count="6">Default</noMatchRule>
        //    		<rule id="2345" size="1000000000" count="0"></rule>
        //    	</flowFileAttribute>
        // </configuration>

        runner.assertValid();
        runner.enqueue(primaryTestFileStream, attributes_with_category_2345);
        runner.run();
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 1);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        for (FlowFile flowFile : flowFiles) {
            String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
            assertEquals(Integer.parseInt(numThresholdsExceeded), 1);
            String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);
            assertEquals(Integer.parseInt(numApplicableThresholds), 2);

            // since a file threshold was exceeded, additional 'file' attributes should be present
            String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
            assertEquals(fileCount_category_2345, "1");
            String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
            assertEquals(fileThreshold_category_2345, "0");

            // since a file threshold was exceeded, and not a byte threshold, 'byte' attributes should not be present
            String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
            assertEquals(byteCount_category_2345, null);
            String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
            assertEquals(byteThreshold_category_2345, null);
        }

        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    //@Ignore("For now...")
    @Test
    public void testNotifiesWhenExceedsByteThresholdButNotFileThreshold() throws IOException {
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_2345, 10);  //5*1,668 bytes = 8,340 bytes + 1,668 = 10,008 bytes which exceeds the threshold of 10k bytes
        runner.setAnnotationData(getFileContents(threshold_settings_allow_10_KB));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="32000" count="6">Default</noMatchRule>
        //    		<rule id="2345" size="10000" count="1000000000"></rule>
        //    	</flowFileAttribute>
        // </configuration>
        runner.assertValid();
        runner.run(10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        int success = 0;
        for (FlowFile flowFile : flowFiles) {
            success++;
            String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
            String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

            if (success > 5) {
                assertEquals(Integer.parseInt(numThresholdsExceeded), 1);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);

                // since a file threshold was not exceeded, additional 'file' attributes should not be present
                String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
                assertEquals(fileCount_category_2345, null);
                String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
                assertEquals(fileThreshold_category_2345, null);

                // since a byte threshold was exceeded, 'byte' attributes should be present
                String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
                long byteCount = Long.parseLong(byteCount_category_2345);
                assertEquals(byteCount, success * PRIMARY_TEST_FILE_SIZE);
                String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
                assertEquals(byteThreshold_category_2345, "10000");
            }
        }
        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    //@Ignore("For now...")
    @Test
    public void testUsesAttributeForCountingBytes() throws IOException {
        final Map<String, String> testAttributes = new HashMap<String, String>() {
            {
                put(CATEGORY_ATTRIBUTE_NAME, VALUE_2345);
                put("count", "1670");
            }
        };
        addTestFilesToInputQueue(primaryTestFile, testAttributes, 1);  //actual size = 1,668 bytes

        runner.setAnnotationData(getFileContents(threshold_settings_allow_1669_bytes));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="320000" count="6">Default</noMatchRule>
        //    		<rule id="2345" size="1669" count="2"></rule>
        //    	</flowFileAttribute>
        //  </configuration>
        runner.setProperty(MonitorThreshold.ATTRIBUTE_TO_USE_FOR_COUNTING_BYTES_PROPERTY, "count");

        runner.assertValid();
        runner.run(1);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 1);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        for (FlowFile flowFile : flowFiles) {
            String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
            String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

            assertEquals(Integer.parseInt(numThresholdsExceeded), 1);
            assertEquals(Integer.parseInt(numApplicableThresholds), 2);

            // since a file threshold was not exceeded, additional 'file' attributes should not be present
            String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
            assertEquals(fileCount_category_2345, null);
            String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
            assertEquals(fileThreshold_category_2345, null);

            // since a byte threshold should have been exceeded, 'byte' attributes should be present
            String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
            long byteCount = Long.parseLong(byteCount_category_2345);
            assertEquals(byteCount, 1670);
            String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
            assertEquals(byteThreshold_category_2345, "1669");
        }
        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    //@Ignore("For now...")
    @Test
    public void testIgnoresNegativeAttributeForCountingBytes() throws IOException {
        final Map<String, String> testAttributes = new HashMap<String, String>() {
            {
                put(CATEGORY_ATTRIBUTE_NAME, VALUE_2345);
                put("count", "-1");
            }
        };
        addTestFilesToInputQueue(primaryTestFile, testAttributes, 2);  //size = 1,668 * 2 = 3,336 bytes

        runner.setAnnotationData(getFileContents(threshold_settings_allow_1669_bytes));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="320000" count="6">Default</noMatchRule>
        //    		<rule id="2345" size="1669" count="2"></rule>
        //    	</flowFileAttribute>
        //  </configuration>
        runner.setProperty(MonitorThreshold.ATTRIBUTE_TO_USE_FOR_COUNTING_BYTES_PROPERTY, "count");

        runner.assertValid();
        runner.run(2);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 2);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        int success = 0;
        for (FlowFile flowFile : flowFiles) {
            success++;
            if (success == 1) {
                String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
                String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

                assertEquals(Integer.parseInt(numThresholdsExceeded), 0);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);

                // since a file threshold was not exceeded, additional 'file' attributes should not be present
                String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
                assertEquals(fileCount_category_2345, null);
                String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
                assertEquals(fileThreshold_category_2345, null);

                // since a byte threshold was not exceeded, 'byte' attributes should not be present
                String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
                assertEquals(byteCount_category_2345, null);
                String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
                assertEquals(byteThreshold_category_2345, null);
            } else {
                String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
                String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

                assertEquals(Integer.parseInt(numThresholdsExceeded), 1);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);

                // since a file threshold was not exceeded, additional 'file' attributes should not be present
                String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
                assertEquals(fileCount_category_2345, null);
                String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
                assertEquals(fileThreshold_category_2345, null);

                // since a byte threshold should have been exceeded, 'byte' attributes should be present
                String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
                long byteCount = Long.parseLong(byteCount_category_2345);
                assertEquals(byteCount, 3336);
                String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
                assertEquals(byteThreshold_category_2345, "1669");
            }
        }
        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    //@Ignore("For now...")
    @Test
    public void testNotifiesWhenExceedsFileThresholdOf_2() throws IOException {
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_2345, 5); //2 will be allowed, 3 won't
        runner.setAnnotationData(getFileContents(threshold_settings_allow_2_files));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="320000" count="6">Default</noMatchRule>
        //    		<rule id="2345" size="1000000000" count="2"></rule>
        //    	</flowFileAttribute>
        // </configuration>
        runner.assertValid();
        runner.run(5);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 5);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        int success = 0;
        for (FlowFile flowFile : flowFiles) {
            success++;
            String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
            String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

            if (success > 2) {
                assertEquals(Integer.parseInt(numThresholdsExceeded), 1);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);

                // since a file threshold was exceeded, additional 'file' attributes should be present
                String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
                int fileCount = Integer.parseInt(fileCount_category_2345);
                assertEquals(fileCount, success);
                String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
                assertEquals(fileThreshold_category_2345, "2");

                // since a file threshold was exceeded, and not a byte threshold, 'byte' attributes should not be present
                String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
                assertEquals(byteCount_category_2345, null);
                String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
                assertEquals(byteThreshold_category_2345, null);

            }
        }
        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    //@Ignore("For now...")
    @Test
    public void testNotifiesForEmptyValue() throws IOException {
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_value_empty, 2); //1 will be allowed, 1 won't
        runner.setAnnotationData(getFileContents(threshold_settings_with_empty_attribute_value));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="32000" count="6">Default</noMatchRule>
        //    		<rule id="" size="1668" count="1"></rule>
        //    	</flowFileAttribute>
        // </configuration>

        runner.assertValid();
        runner.run(2);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 2);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        int success = 0;
        for (FlowFile flowFile : flowFiles) {
            success++;
            String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
            String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

            if (success > 1) {
                assertEquals(Integer.parseInt(numThresholdsExceeded), 2);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);
            }
        }
        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    //@Ignore("For now...")
    @Test
    public void testForSpaces() throws IOException {
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_value_single_space, 2); //1 will be allowed, 1 won't
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_value_double_space, 2); //1 will be allowed, 1 won't
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_value_triple_space, 2); //should use default; 1 will be allowed, 1 won't
        runner.setAnnotationData(getFileContents(threshold_settings_with_spaces_for_attribute_values));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="1668" count="1">Default</noMatchRule>
        //    		<rule id=" " size="1668" count="1"></rule>
        //    		<rule id="  " size="1668" count="1"></rule>
        //    	</flowFileAttribute>
        // </configuration>

        runner.assertValid();
        runner.run(6);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 6);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);

        for (int currentFile = 0; currentFile < flowFiles.size(); currentFile++) {
            MockFlowFile flowFile = flowFiles.get(currentFile);

            dumpAttributes(flowFile); // for manual verification of this test

            if ((currentFile == 1) || (currentFile == 3) || (currentFile == 5)) {
                String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
                String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);
                assertEquals(Integer.parseInt(numThresholdsExceeded), 2);
            }
        }
        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    //@Ignore("For now...")
    @Test
    public void testUsesDefaultFileThresholdWhenValueEncounteredWithNoThreshold() throws IOException {
        runner.setProperty(MonitorThreshold.OPT_AGGREGATE_COUNTS_WHEN_NO_THRESHOLD_PROVIDED, "false");
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_2345, 3); // should use default
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_3456, 3); // should use default
        runner.setAnnotationData(getFileContents(threshold_settings_allow_2_Default));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="320000" count="2">Default</noMatchRule>
        //    		<rule id="1234" size="1000000000" count="9"></rule>
        //    	</flowFileAttribute>
        // </configuration>    
        runner.assertValid();
        runner.run(6);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 6);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        // compare expected to what was produced
        List<MockFlowFile> flowFilesProduced = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        int fileNumber = 0;
        for (FlowFile flowFile : flowFilesProduced) {
            fileNumber++;

            if ((fileNumber == 3) | (fileNumber == 6)) {
                String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
                String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

                assertEquals(Integer.parseInt(numThresholdsExceeded), 1);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);

                if (fileNumber == 3) {
                    // since a file threshold was exceeded, additional 'file' attributes should be present
                    String fileCount_category_2345_value = flowFile.getAttribute(fileCount_category_2345_key);
                    int fileCount = Integer.parseInt(fileCount_category_2345_value);
                    assertEquals(fileCount, fileNumber);
                    String fileThreshold_category_2345_value = flowFile.getAttribute(fileThreshold_category_2345_key);
                    assertEquals(fileThreshold_category_2345_value, "2");

                    // since a file threshold was exceeded, and not a byte threshold, 'byte' attributes should not be present
                    String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
                    assertEquals(byteCount_category_2345, null);
                    String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
                    assertEquals(byteThreshold_category_2345, null);
                }

                if (fileNumber == 6) {
                    // since a file threshold was exceeded, additional 'file' attributes should be present
                    String fileCount_category_3456_value = flowFile.getAttribute(fileCount_category_3456_key);
                    int fileCount = Integer.parseInt(fileCount_category_3456_value);
                    assertEquals(fileCount, 3);

                    String fileThreshold_category_3456_value = flowFile.getAttribute(fileThreshold_category_3456_key);
                    assertEquals(fileThreshold_category_3456_value, "2");

                    // since a file threshold was exceeded, and not a byte threshold, 'byte' attributes should not be present
                    String byteCount_category_3456 = flowFile.getAttribute(byteCount_category_3456_key);
                    assertEquals(byteCount_category_3456, null);
                    String byteThreshold_category_3456 = flowFile.getAttribute(byteThreshold_category_3456_key);
                    assertEquals(byteThreshold_category_3456, null);
                }
            }
        }

        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    //@Ignore("For now...")
    @Test
    public void testUsesDefaultByteThresholdWhenValueEncounteredWithNoThreshold() throws IOException {
        runner.setProperty(MonitorThreshold.OPT_AGGREGATE_COUNTS_WHEN_NO_THRESHOLD_PROVIDED, "false");
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_2345, 10); // should use default
        runner.setAnnotationData(getFileContents(threshold_settings_allow_3336_bytes_default));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="3336" count="1000">Default</noMatchRule>
        //    		<rule id="1234" size="1000000000" count="9"></rule>
        //    	</flowFileAttribute>
        // </configuration>
        runner.assertValid();
        runner.run(10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        // compare expected to what was produced
        List<MockFlowFile> flowFilesProduced = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        int fileNumber = 0;
        for (FlowFile flowFile : flowFilesProduced) {
            fileNumber++;
            String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
            String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

            if (fileNumber <= 2) {
                assertEquals(Integer.parseInt(numThresholdsExceeded), 0);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);
            } else {
                assertEquals(Integer.parseInt(numThresholdsExceeded), 1);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);

                // since a byte threshold was exceeded, additional 'byte' attributes should be present
                String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
                int byteCount = Integer.parseInt(byteCount_category_2345);
                assertEquals(byteCount, (fileNumber * PRIMARY_TEST_FILE_SIZE));
                String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
                assertEquals(byteThreshold_category_2345, "3336");

                // since a file threshold was not exceeded, 'file' attributes should not be present
                String fileCount_category_2345_value = flowFile.getAttribute(fileCount_category_2345_key);
                assertEquals(fileCount_category_2345_value, null);

                String fileThreshold_category_2345_value = flowFile.getAttribute(fileThreshold_category_2345_key);
                assertEquals(fileThreshold_category_2345_value, null);
            }
        }

        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    //@Ignore("For now...")
    @Test
    public void testHandlesMultipleThresholdSets() throws IOException {
        for (int i = 0; i < 10; i++) {
            final Map<String, String> metadata = new HashMap<String, String>();
            metadata.put(PRIORITY_ATTRIBUTE_NAME, VALUE_1); //10*1,668 bytes = 16,680 bytes which exceeds the threshold of 2k bytes. 2nd-10th files should exceed.
            metadata.put(CATEGORY_ATTRIBUTE_NAME, VALUE_2345); //5*1,668 bytes = 8,340 bytes + 1,668 = 10,008 bytes which exceeds the threshold of 10k bytes.  6th through 10th files should exceed.
            FileInputStream testFileStream = new FileInputStream(primaryTestFile);
            runner.enqueue(testFileStream, metadata);
            testFileStream.close();
        }
        runner.setAnnotationData(getFileContents(threshold_settings_allow_10_KB_And_2_KB));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="32000" count="6">Default</noMatchRule>
        //    		<rule id="2345" size="10000" count="1000000000"></rule>
        //    	</flowFileAttribute>
        //    	<flowFileAttribute attributeName="priority" id="2">
        //    		<noMatchRule size="100000000" count="10000000">Default</noMatchRule>
        //    		<rule id="0" size="4000" count="1000000000"></rule>
        //    		<rule id="1" size="2000" count="1000000000"></rule>
        //    	</flowFileAttribute>
        // </configuration>
        // 'priority' value of '1' at 2000 bytes will be the most limiting attribute, allowing only 1 file that does not exceed.
        //   however, the 'category' value of '2345' threshold is 10,000, which means 5 will not exceed, 
        //   before 'all' (both priority and category) thresholds are exceeded. 
        runner.assertValid();
        runner.run(10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        // compare expected to what was produced
        List<MockFlowFile> flowFilesProduced = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        int fileNumber = 0;
        for (FlowFile flowFile : flowFilesProduced) {
            fileNumber++;
            String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
            String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

            if (fileNumber == 1) {
                assertEquals(Integer.parseInt(numThresholdsExceeded), 0);
                assertEquals(Integer.parseInt(numApplicableThresholds), 4);
            } else if ((fileNumber >= 2) & (fileNumber <= 5)) {
                assertEquals(Integer.parseInt(numThresholdsExceeded), 1);
                assertEquals(Integer.parseInt(numApplicableThresholds), 4);

                // since a byte threshold was exceeded, additional 'byte' attributes should be present
                String byteCount_priority_1_Value = flowFile.getAttribute(byteCount_priority_1_key);
                int byteCount = Integer.parseInt(byteCount_priority_1_Value);
                assertEquals(byteCount, (fileNumber * PRIMARY_TEST_FILE_SIZE));
                String byteThreshold_priority_1_Value = flowFile.getAttribute(byteThreshold_priority_1_key);
                assertEquals(byteThreshold_priority_1_Value, "2000");

                // since a file threshold was not exceeded, 'file' attributes should not be present
                String fileCount_priority_1_Value = flowFile.getAttribute(fileCount_priority_1_key);
                assertEquals(fileCount_priority_1_Value, null);
                String fileThreshold_priority_1_Value = flowFile.getAttribute(fileThreshold_priority_1_key);
                assertEquals(fileThreshold_priority_1_Value, null);
            } else if (fileNumber > 5) {
                assertEquals(Integer.parseInt(numThresholdsExceeded), 2);
                assertEquals(Integer.parseInt(numApplicableThresholds), 4);

                // since a byte thresholds were exceeded, additional 'byte' attributes should be present
                String byteCount_priority_1_Value = flowFile.getAttribute(byteCount_priority_1_key);
                int byteCount_priority_1 = Integer.parseInt(byteCount_priority_1_Value);
                assertEquals(byteCount_priority_1, (fileNumber * PRIMARY_TEST_FILE_SIZE));
                String byteThreshold_priority_1_Value = flowFile.getAttribute(byteThreshold_priority_1_key);
                assertEquals(byteThreshold_priority_1_Value, "2000");

                String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
                int byteCount = Integer.parseInt(byteCount_category_2345);
                assertEquals(byteCount, (fileNumber * PRIMARY_TEST_FILE_SIZE));
                String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
                assertEquals(byteThreshold_category_2345, "10000");

                // since a file threshold was not exceeded, 'file' attributes should not be present
                String fileCount_priority_1_Value = flowFile.getAttribute(fileCount_priority_1_key);
                assertEquals(fileCount_priority_1_Value, null);
                String fileThreshold_priority_1_Value = flowFile.getAttribute(fileThreshold_priority_1_key);
                assertEquals(fileThreshold_priority_1_Value, null);

                String fileCount_category_2345_value = flowFile.getAttribute(fileCount_category_2345_key);
                assertEquals(fileCount_category_2345_value, null);
                String fileThreshold_category_2345_value = flowFile.getAttribute(fileThreshold_category_2345_key);
                assertEquals(fileThreshold_category_2345_value, null);
            }
        }

        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    @Ignore("This test takes WAAAAY too long")
    @Test
    public void testCountsResetAtExpectedTime() throws FileNotFoundException, IOException {
        // figure out what the rollover time should be.
        final Calendar now = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        final Calendar rolloverTime = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        rolloverTime.add(Calendar.HOUR, 0);
        rolloverTime.add(Calendar.MINUTE, 0);
        rolloverTime.set(Calendar.SECOND, 10);
        rolloverTime.set(Calendar.MILLISECOND, 0);

        // if the rollover time is less than 3 seconds, we'll add another minute so that we are
        // sure that we have time to process all of the files before the rollover time.
        if (rolloverTime.getTimeInMillis() - now.getTimeInMillis() < 3000) {
            rolloverTime.add(Calendar.MINUTE, 1);
        }

        final long millisToWait = rolloverTime.getTimeInMillis() - now.getTimeInMillis();

        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_2345, 20);

        final String rolloverTimeAsString
                = rolloverTime.get(Calendar.HOUR_OF_DAY)
                + ":" + rolloverTime.get(Calendar.MINUTE)
                + ":" + rolloverTime.get(Calendar.SECOND);
        runner.setProperty(MonitorThreshold.OPT_COUNT_RESET_TIME, rolloverTimeAsString);
        runner.setAnnotationData(getFileContents(threshold_settings_allow_10_KB));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="32000" count="6">Default</noMatchRule>
        //    		<rule id="2345" size="10000" count="1000000000"></rule>
        //    	</flowFileAttribute>
        // </configuration>
        runner.assertValid();
        runner.run(10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);
        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test

        try {
            // Wait 60 seconds to ensure that the rollover happens.
            System.out.println("Waiting " + (millisToWait / 1000) + " seconds to test that counts are cleared at appropriate time");
            Thread.sleep(millisToWait);
        } catch (final Exception e) {
        }

        runner.run(10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 20);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    @Ignore("This test takes WAAAAY too long")
    @Test
    public void testCountsResetAfterExpectedMinutesToWaitHasPassed() throws IOException {
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_2345, 20);
        runner.setProperty(MonitorThreshold.OPT_FREQUENCY_TO_SAVE_COUNTS_SECS, "45");
        final MonitorThreshold monitor = (MonitorThreshold) runner.getProcessor();
        runner.setAnnotationData(getFileContents(threshold_settings_allow_10_KB));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="32000" count="6">Default</noMatchRule>
        //    		<rule id="2345" size="10000" count="1000000000"></rule>
        //    	</flowFileAttribute>
        // </configuration>     
        assertFalse(monitor.shouldSaveCounts(runner.getProcessContext()));
        runner.assertValid();
        runner.run(10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);
        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test

        try {
            // Wait 60 seconds to ensure that the rollover happens.
            System.out.println("Waiting 60 seconds to ensure that the rollover happens");
            Thread.sleep(60000L);
        } catch (final Exception e) {
        }

        assertTrue(monitor.shouldSaveCounts(runner.getProcessContext()));
        runner.run(10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 20);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    //@Ignore("For now...")
    @SuppressWarnings("unchecked")
    @Test
    public void testCountsPersisted() throws IOException, ClassNotFoundException {
        runner.setProperty(MonitorThreshold.OPT_AGGREGATE_COUNTS_WHEN_NO_THRESHOLD_PROVIDED, "false");
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_1234, 10);
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_2345, 10);
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_3456, 10);
        runner.setAnnotationData(getFileContents(threshold_settings_allow_2_Default));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="320000" count="2">Default</noMatchRule>
        //    		<rule id="1234" size="1000000000" count="9"></rule>
        //    	</flowFileAttribute>
        // </configuration>
        runner.assertValid();
        runner.run(30);

        final MonitorThreshold monitor = (MonitorThreshold) runner.getProcessor();
        final File countsFile = new File("target/counts");

        monitor.saveCounts(countsFile, logger);

        final FileInputStream fis = new FileInputStream(countsFile);
        final ObjectInputStream ois = new ObjectInputStream(fis);

        ConcurrentHashMap<String, ConcurrentHashMap<String, CompareRecord>> state;
        try {
            final Object obj = ois.readObject();
            state = (ConcurrentHashMap<String, ConcurrentHashMap<String, CompareRecord>>) obj;
        } finally {
            ois.close();
        }

//        final CompareRecord defaultRecord = state.get(CATEGORY_ATTRIBUTE_NAME).get(MonitorThreshold.DEFAULT_THRESHOLDS_KEY);
//        assertNotNull(defaultRecord);
//        assertEquals(0, defaultRecord.getFileCount()); //default used for providing thresholds, not for counting
        assertEquals(3, state.get(CATEGORY_ATTRIBUTE_NAME).size());

        final CompareRecord countsForValue_1234 = state.get(CATEGORY_ATTRIBUTE_NAME).get("1234");
        assertNotNull(countsForValue_1234);
        assertEquals(10, countsForValue_1234.getFileCount());
        assertEquals(16680, countsForValue_1234.getByteCount());

        final CompareRecord countsForValue2345 = state.get(CATEGORY_ATTRIBUTE_NAME).get("2345");
        assertNotNull(countsForValue2345);
        assertEquals(10, countsForValue2345.getFileCount());
        assertEquals(16680, countsForValue2345.getByteCount());

        final CompareRecord countsForValue3456 = state.get(CATEGORY_ATTRIBUTE_NAME).get("3456");
        assertNotNull(countsForValue3456);
        assertEquals(10, countsForValue3456.getFileCount());
        assertEquals(16680, countsForValue3456.getByteCount());

        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    @SuppressWarnings("unchecked")
    //@Ignore("For now...")
    @Test
    public void testExpectedCountsReloadedFromDiskAfterNiFiRestart() throws FileNotFoundException, IOException, ClassNotFoundException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        runner.setProperty(MonitorThreshold.OPT_AGGREGATE_COUNTS_WHEN_NO_THRESHOLD_PROVIDED, "false");
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_2345, 10);
        runner.setAnnotationData(getFileContents(threshold_settings_allow_2_Default)); // will use default threshold  
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="320000" count="2">Default</noMatchRule>
        //    		<rule id="1234" size="1000000000" count="9"></rule>
        //    	</flowFileAttribute>
        // </configuration>
        runner.assertValid();
        runner.run(10);
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test

        final MonitorThreshold compareProcessor = (MonitorThreshold) runner.getProcessor();
        final File countsFile = new File("target/counts");

        compareProcessor.saveCounts(countsFile, logger);

        //Simulate a NiFi restart by creating a new runner, with a new CompareCountsToThreshold class
        TestRunner differentRunner = TestRunners.newTestRunner(new MonitorThreshold());
        differentRunner.setProperty(MonitorThreshold.OPT_AGGREGATE_COUNTS_WHEN_NO_THRESHOLD_PROVIDED, "false");
        differentRunner.setProperty(MonitorThreshold.OPT_COUNT_RESET_MINUTES, "1");
        differentRunner.setProperty(MonitorThreshold.OPT_COUNTS_PERSISTENCE_FILE_PREFIX, "target/counts");
        primaryTestFileStream = new FileInputStream(primaryTestFile);
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_2345, 1);
        runner.setAnnotationData(getFileContents(threshold_settings_allow_2_Default));  // will use default threshold
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="320000" count="2">Default</noMatchRule>
        //    		<rule id="1234" size="1000000000" count="9"></rule>
        //    	</flowFileAttribute>
        // </configuration>
        runner.assertValid();
        runner.run(1);

        //Prove that the old state was successfully read from disk.
        final Field countsField = MonitorThreshold.class.getDeclaredField("counts");
        countsField.setAccessible(true);

        final ConcurrentHashMap<String, ConcurrentHashMap<String, CompareRecord>> state
                = (ConcurrentHashMap<String, ConcurrentHashMap<String, CompareRecord>>) countsField.get(runner.getProcessor());

        assertEquals(1, state.get(CATEGORY_ATTRIBUTE_NAME).size()); // only one threshold (2345) was checked

        final CompareRecord countsForValue_2345 = state.get(CATEGORY_ATTRIBUTE_NAME).get("2345");
        assertNotNull(countsForValue_2345);
        assertEquals(11, countsForValue_2345.getFileCount()); // would have been 1 if old state was not read from disk
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    @SuppressWarnings("unchecked")
    //@Ignore("For now...")
    @Test
    public void testFileCountsRecordedAsExpected() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_1234, 10); // only one will exceed
        runner.setAnnotationData(getFileContents(threshold_settings_allow_2_Default));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="320000" count="2">Default</noMatchRule>
        //    		<rule id="1234" size="1000000000" count="9"></rule>
        //    	</flowFileAttribute>
        // </configuration>
        runner.assertValid();
        runner.run(10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);
        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test

        final Field countsField = MonitorThreshold.class.getDeclaredField("counts");
        countsField.setAccessible(true);

        final ConcurrentHashMap<String, ConcurrentHashMap<String, CompareRecord>> state
                = (ConcurrentHashMap<String, ConcurrentHashMap<String, CompareRecord>>) countsField.get(runner.getProcessor());

        assertEquals(1, state.get(CATEGORY_ATTRIBUTE_NAME).size()); // only one threshold (1234) was checked

        final CompareRecord countsForValue_1234 = state.get(CATEGORY_ATTRIBUTE_NAME).get("1234");
        assertNotNull(countsForValue_1234);
        assertEquals(10, countsForValue_1234.getFileCount());

        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    @SuppressWarnings("unchecked")
    //@Ignore("For now...")
    @Test
    public void testByteCountsRecordedAsExpected() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_1234, 10); // only one will exceed
        runner.setAnnotationData(getFileContents(threshold_settings_allow_2_Default));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="320000" count="2">Default</noMatchRule>
        //    		<rule id="1234" size="1000000000" count="9"></rule>
        //    	</flowFileAttribute>
        // </configuration>
        runner.assertValid();
        runner.run(10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);
        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test

        final Field countsField = MonitorThreshold.class.getDeclaredField("counts");
        countsField.setAccessible(true);

        final ConcurrentHashMap<String, ConcurrentHashMap<String, CompareRecord>> state
                = (ConcurrentHashMap<String, ConcurrentHashMap<String, CompareRecord>>) countsField.get(runner.getProcessor());

        assertEquals(1, state.get(CATEGORY_ATTRIBUTE_NAME).size()); // only one threshold (1234) was checked

        final CompareRecord countsForValue_1234 = state.get(CATEGORY_ATTRIBUTE_NAME).get("1234");
        assertNotNull(countsForValue_1234);

        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
        assertEquals((PRIMARY_TEST_FILE_SIZE * 10), countsForValue_1234.getByteCount());

    }

    //@Ignore("For now...")
    @Test
    public void testOmitsAdditionOfExceededAttributesWhenRequested() throws IOException {

        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_2345, 2);  //size = 1,668 * 2 = 3,336 bytes

        runner.setAnnotationData(getFileContents(threshold_settings_allow_1669_bytes));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="320000" count="6">Default</noMatchRule>
        //    		<rule id="2345" size="1669" count="2"></rule>
        //    	</flowFileAttribute>
        //  </configuration>
        runner.setProperty(MonitorThreshold.OPT_ADD_ATTRIBUTES, MonitorThreshold.NEVER);

        runner.assertValid();
        runner.run(2);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 2);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        int success = 0;
        for (FlowFile flowFile : flowFiles) {
            success++;
            if (success == 1) {
                String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
                String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

                assertEquals(Integer.parseInt(numThresholdsExceeded), 0);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);

                // since a file threshold was not exceeded, additional 'file' attributes should not be present
                String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
                assertEquals(fileCount_category_2345, null);
                String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
                assertEquals(fileThreshold_category_2345, null);

                // since a byte threshold was not exceeded, 'byte' attributes should not be present
                String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
                assertEquals(byteCount_category_2345, null);
                String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
                assertEquals(byteThreshold_category_2345, null);
            } else {
                String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
                String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

                assertEquals(Integer.parseInt(numThresholdsExceeded), 1);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);

                // since a file threshold was not exceeded, additional 'file' attributes should not be present
                String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
                assertEquals(fileCount_category_2345, null);
                String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
                assertEquals(fileThreshold_category_2345, null);

                // even though a byte threshold was exceeded, 'byte' attributes should not be present since Add Attributes was set to 'Never'
                String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
                assertEquals(byteCount_category_2345, null);
                String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
                assertEquals(byteThreshold_category_2345, null);
            }
        }
        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    @Test
    public void testAlwaysAddsCountAndThresholdAttributesWhenRequested() throws IOException {

        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_2345, 2);  //size = 1,668 * 2 = 3,336 bytes

        runner.setAnnotationData(getFileContents(threshold_settings_allow_1669_bytes));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="320000" count="6">Default</noMatchRule>
        //    		<rule id="2345" size="1669" count="2"></rule>
        //    	</flowFileAttribute>
        //  </configuration>
        runner.setProperty(MonitorThreshold.OPT_ADD_ATTRIBUTES, MonitorThreshold.ALWAYS);

        runner.assertValid();
        runner.run(2);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 2);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        int success = 0;
        for (FlowFile flowFile : flowFiles) {
            success++;
            if (success == 1) {
                String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
                String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

                assertEquals(Integer.parseInt(numThresholdsExceeded), 0);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);

                // while a file threshold was not exceeded, additional 'file' attributes should be present since Add Attributes was set to 'Always'
                String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
                assertEquals(fileCount_category_2345, "1");
                String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
                assertEquals(fileThreshold_category_2345, "2");

                // while a byte threshold was not exceeded, 'byte' attributes should be present since Add Attributes was set to 'Always'
                String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
                assertEquals(byteCount_category_2345, "1668");
                String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
                assertEquals(byteThreshold_category_2345, "1669");
            } else {
                String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
                String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

                assertEquals(Integer.parseInt(numThresholdsExceeded), 1);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);

                // while a file threshold was not exceeded, additional 'file' attributes should be present since Add Attributes was set to 'Always'
                String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
                assertEquals(fileCount_category_2345, "2");
                String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
                assertEquals(fileThreshold_category_2345, "2");

                // a byte threshold was exceeded, 'byte' attributes should be present especially since Add Attributes was set to 'Always'
                String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
                assertEquals(byteCount_category_2345, "3336");
                String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
                assertEquals(byteThreshold_category_2345, "1669");
            }
        }
        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMaintainCountsWhenNoThresholdProvided() throws IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, InitializationException, NoSuchFieldException {
        // part 1 of test: Maintain separate counts when no threshold provided
        runner.setProperty(MonitorThreshold.OPT_AGGREGATE_COUNTS_WHEN_NO_THRESHOLD_PROVIDED, "false");

        //Send FlowFiles (to the processor) with values for which there are no thresholds
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_2345, 3); // should use default
        runner.setAnnotationData(getFileContents(threshold_settings_allow_3336_bytes_default));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="3336" count="1000">Default</noMatchRule>
        //    		<rule id="1234" size="1000000000" count="9"></rule>
        //    	</flowFileAttribute>
        // </configuration>
        runner.assertValid();
        runner.run(3);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 3);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        //Verify that counts are maintained, even though there was no threshold for the value
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        int success = 0;
        for (FlowFile flowFile : flowFiles) {
            success++;
            if (success == 3) {
                String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
                String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

                assertEquals(Integer.parseInt(numThresholdsExceeded), 1);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);

                // since a file threshold was not exceeded, additional 'file' attributes should not be present
                String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
                assertEquals(fileCount_category_2345, null);
                String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
                assertEquals(fileThreshold_category_2345, null);

                // since a byte threshold was exceeded, additional 'byte' attributes should be present
                String byteCount_category_2345_Value = flowFile.getAttribute(byteCount_category_2345_key);
                int byteCount_category_2345 = Integer.parseInt(byteCount_category_2345_Value);
                assertEquals(byteCount_category_2345, (success * PRIMARY_TEST_FILE_SIZE));
                String byteThreshold_category_2345_Value = flowFile.getAttribute(byteThreshold_category_2345_key);
                assertEquals(byteThreshold_category_2345_Value, "3336");
            } else {
                String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
                String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

                assertEquals(Integer.parseInt(numThresholdsExceeded), 0);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);

                // since a file threshold was not exceeded, additional 'file' attributes should not be present
                String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
                assertEquals(fileCount_category_2345, null);
                String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
                assertEquals(fileThreshold_category_2345, null);

                // since a byte threshold was not exceeded, additional 'byte' attributes should not be present
                String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
                assertEquals(byteCount_category_2345, null);
                String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
                assertEquals(byteThreshold_category_2345, null);
            }
        }
        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());

        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
        //==============================================================================================================================================
        tearDown();
        setUp();
        // part 2 of test: Maintain aggregate count when no threshold provided      
        runner.setProperty(MonitorThreshold.OPT_AGGREGATE_COUNTS_WHEN_NO_THRESHOLD_PROVIDED, "true");

        //Send FlowFiles (to the processor) with values for which there are no thresholds
        addTestFilesToInputQueue(primaryTestFile, attributes_with_category_2345, 3); // should use default
        runner.setAnnotationData(getFileContents(threshold_settings_allow_3336_bytes_default));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="3336" count="1000">Default</noMatchRule>
        //    		<rule id="1234" size="1000000000" count="9"></rule>
        //    	</flowFileAttribute>
        // </configuration>
        runner.assertValid();
        runner.run(3);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 3);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        //Verify that counts were maintained, despite there being no threshold for the value that was encountered
        flowFiles = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        success = 0;
        for (FlowFile flowFile : flowFiles) {
            success++;
            if (success == 3) {
                String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
                String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

                assertEquals(Integer.parseInt(numThresholdsExceeded), 1);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);

                // since a file threshold was not exceeded, additional 'file' attributes should not be present
                String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
                assertEquals(fileCount_category_2345, null);
                String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
                assertEquals(fileThreshold_category_2345, null);

                // since a byte threshold was exceeded, additional 'byte' attributes should be present
                String byteCount_category_2345_value = flowFile.getAttribute(byteCount_category_2345_key);
                int byteCount_category_2345 = Integer.parseInt(byteCount_category_2345_value);
                assertEquals(byteCount_category_2345, (success * PRIMARY_TEST_FILE_SIZE));
                String byteThreshold_category_2345_Value = flowFile.getAttribute(byteThreshold_category_2345_key);
                assertEquals(byteThreshold_category_2345_Value, "3336");

                // since we should be aggregating counts, verify that default counts were incremented 
                final Field countsField = MonitorThreshold.class.getDeclaredField("counts");
                countsField.setAccessible(true);

                final ConcurrentHashMap<String, ConcurrentHashMap<String, CompareRecord>> counts
                        = (ConcurrentHashMap<String, ConcurrentHashMap<String, CompareRecord>>) countsField.get(runner.getProcessor());

                assertEquals(1, counts.get(CATEGORY_ATTRIBUTE_NAME).size());

                final CompareRecord countsForValue_Default = counts.get(CATEGORY_ATTRIBUTE_NAME).get("Default");
                assertNotNull(countsForValue_Default);
                assertEquals(5004, countsForValue_Default.getByteCount());

            } else {
                String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
                String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

                assertEquals(Integer.parseInt(numThresholdsExceeded), 0);
                assertEquals(Integer.parseInt(numApplicableThresholds), 2);

                // since a file threshold was not exceeded, additional 'file' attributes should not be present
                String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
                assertEquals(fileCount_category_2345, null);
                String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
                assertEquals(fileThreshold_category_2345, null);

                // since a byte threshold was not exceeded, additional 'byte' attributes should not be present
                String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
                assertEquals(byteCount_category_2345, null);
                String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
                assertEquals(byteThreshold_category_2345, null);
            }
        }
        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());

        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    //@Ignore("For now...")
    @Test
    public void testMaximumAttributePairsToAddWhenMultipleThresholdsExceeded() throws IOException {
        runner.setProperty(MonitorThreshold.MAX_ATTRIBUTE_PAIRS_TO_ADD_PROPERTY, "1");

        for (int i = 0; i < 10; i++) {
            final Map<String, String> metadata = new HashMap<String, String>();
            metadata.put(PRIORITY_ATTRIBUTE_NAME, VALUE_1); //10*1,668 bytes = 16,680 bytes which exceeds the threshold of 2k bytes. 2nd-10th files should exceed.
            metadata.put(CATEGORY_ATTRIBUTE_NAME, VALUE_2345); //5*1,668 bytes = 8,340 bytes + 1,668 = 10,008 bytes which exceeds the threshold of 10k bytes.  6th through 10th files should exceed.
            FileInputStream testFileStream = new FileInputStream(primaryTestFile);
            runner.enqueue(testFileStream, metadata);
            testFileStream.close();
        }
        runner.setAnnotationData(getFileContents(threshold_settings_allow_10_KB_And_2_KB));
        // <configuration>
        //    	<flowFileAttribute attributeName="category" id="1">
        //    		<noMatchRule size="32000" count="6">Default</noMatchRule>
        //    		<rule id="2345" size="10000" count="1000000000"></rule>
        //    	</flowFileAttribute>
        //    	<flowFileAttribute attributeName="priority" id="2">
        //    		<noMatchRule size="100000000" count="10000000">Default</noMatchRule>
        //    		<rule id="0" size="4000" count="1000000000"></rule>
        //    		<rule id="1" size="2000" count="1000000000"></rule>
        //    	</flowFileAttribute>
        // </configuration>
        // 'priority' value of '1' at 2000 bytes will be the most limiting attribute, allowing only 1 file that does not exceed.
        //   however, the 'category' value of '2345' threshold is 10,000, which means 5 will not exceed, 
        //   before 'all' (both priority and category) thresholds are exceeded. 
        runner.assertValid();
        runner.run(10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_SUCCESS, 10);
        runner.assertTransferCount(MonitorThreshold.RELATIONSHIP_FAILURE, 0);

        // compare expected to what was produced
        List<MockFlowFile> flowFilesProduced = runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS);
        int fileNumber = 0;
        for (FlowFile flowFile : flowFilesProduced) {
            fileNumber++;
            int numThresholdExceededAttributePairsFound = 0;
            String numThresholdsExceeded = flowFile.getAttribute(MonitorThreshold.NUM_THRESHOLDS_EXCEEDED_KEY);
            String numApplicableThresholds = flowFile.getAttribute(MonitorThreshold.NUM_APPLICABLE_THRESHOLDS_KEY);

            if (fileNumber == 1) {
                assertEquals(Integer.parseInt(numThresholdsExceeded), 0);
                assertEquals(Integer.parseInt(numApplicableThresholds), 4);
                numThresholdExceededAttributePairsFound = countNumberThresholdExceededAttributePairsFound(flowFile);
                assertEquals(numThresholdExceededAttributePairsFound, 0);
            } else if ((fileNumber >= 2) & (fileNumber <= 5)) {
                assertEquals(Integer.parseInt(numThresholdsExceeded), 1);
                assertEquals(Integer.parseInt(numApplicableThresholds), 4);
                numThresholdExceededAttributePairsFound = countNumberThresholdExceededAttributePairsFound(flowFile);
                assertEquals(numThresholdExceededAttributePairsFound, 1);
            } else if (fileNumber > 5) {
                assertEquals(Integer.parseInt(numThresholdsExceeded), 2);
                assertEquals(Integer.parseInt(numApplicableThresholds), 4);
                numThresholdExceededAttributePairsFound = countNumberThresholdExceededAttributePairsFound(flowFile);
                assertEquals(numThresholdExceededAttributePairsFound, 1);
            }
        }
        assertEquals(primaryTestFile.length(), runner.getFlowFilesForRelationship(MonitorThreshold.RELATIONSHIP_SUCCESS).iterator().next().getSize());
        //dumpCounts(runner.getProcessContext()); // for manual verification of this test
    }

    private int countNumberThresholdExceededAttributePairsFound(FlowFile flowFile) {
        int numThresholdExceededAttributesFound = 0;
        String fileCount_category_2345 = flowFile.getAttribute(fileCount_category_2345_key);
        if (fileCount_category_2345 != null) {
            numThresholdExceededAttributesFound++;
        }

        String fileThreshold_category_2345 = flowFile.getAttribute(fileThreshold_category_2345_key);
        if (fileThreshold_category_2345 != null) {
            numThresholdExceededAttributesFound++;
        }

        String byteCount_category_2345 = flowFile.getAttribute(byteCount_category_2345_key);
        if (byteCount_category_2345 != null) {
            numThresholdExceededAttributesFound++;
        }

        String byteThreshold_category_2345 = flowFile.getAttribute(byteThreshold_category_2345_key);
        if (byteThreshold_category_2345 != null) {
            numThresholdExceededAttributesFound++;
        }

        String fileCount_category_3456 = flowFile.getAttribute(fileCount_category_3456_key);
        if (fileCount_category_3456 != null) {
            numThresholdExceededAttributesFound++;
        }

        String fileThreshold_category_3456 = flowFile.getAttribute(fileThreshold_category_3456_key);
        if (fileThreshold_category_3456 != null) {
            numThresholdExceededAttributesFound++;
        }

        String byteCount_category_3456 = flowFile.getAttribute(byteCount_category_3456_key);
        if (byteCount_category_3456 != null) {
            numThresholdExceededAttributesFound++;
        }

        String byteThreshold_category_3456 = flowFile.getAttribute(byteThreshold_category_3456_key);
        if (byteThreshold_category_3456 != null) {
            numThresholdExceededAttributesFound++;
        }

        String fileCount_priority_1 = flowFile.getAttribute(fileCount_priority_1_key);
        if (fileCount_priority_1 != null) {
            numThresholdExceededAttributesFound++;
        }

        String fileThreshold_priority_1 = flowFile.getAttribute(fileThreshold_priority_1_key);
        if (fileThreshold_priority_1 != null) {
            numThresholdExceededAttributesFound++;
        }

        String byteCount_priority_1 = flowFile.getAttribute(byteCount_priority_1_key);
        if (byteCount_priority_1 != null) {
            numThresholdExceededAttributesFound++;
        }

        String byteThreshold_priority_1 = flowFile.getAttribute(byteThreshold_priority_1_key);
        if (byteThreshold_priority_1 != null) {
            numThresholdExceededAttributesFound++;
        }
        return numThresholdExceededAttributesFound / 2;
    }

    // helper methods
    private String getFileContents(String filepath) throws IOException {
        String str;
        String result = new String();
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(filepath));
            while ((str = in.readLine()) != null) {
                result += str;
            }
        } catch (Exception ex) {
        } finally {
            if (in != null) {
                in.close();
            }
        }
        return result;
    }

    private void addTestFilesToInputQueue(File testFile, final Map<String, String> attributes, int number) throws FileNotFoundException,
            IOException {
        FileInputStream testFileStream;
        for (int i = 0; i < number; i++) {
            testFileStream = new FileInputStream(testFile);
            runner.enqueue(testFileStream, attributes);
            testFileStream.close();
        }
    }

    private void dumpCounts(ProcessContext processContext) {
        ((MonitorThreshold) runner.getProcessor()).logCounts(processContext, logger);
    }

    private void dumpAttributes(MockFlowFile flowFile) {
        Map<String, String> attrs = flowFile.getAttributes();
        Set<String> attributes = attrs.keySet();

        System.out.println("\n=============== Next File: ================");
        for (String attr : attributes) {
            String value = flowFile.getAttribute(attr);
            System.out.println("Attribute Name: " + attr + " Value: '" + value + "'");
        }
    }
}
