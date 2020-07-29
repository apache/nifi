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

import com.google.common.base.Splitter;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestAttributesToCSV {

    private static final String OUTPUT_NEW_ATTRIBUTE = "flowfile-attribute";
    private static final String OUTPUT_OVERWRITE_CONTENT = "flowfile-content";
    private static final String OUTPUT_ATTRIBUTE_NAME = "CSVData";
    private static final String OUTPUT_SEPARATOR = ",";
    private static final String OUTPUT_MIME_TYPE = "text/csv";
    private static final String SPLIT_REGEX = OUTPUT_SEPARATOR + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
    private static final String newline = System.getProperty("line.separator");

    @Test
    public void testAttrListNoCoreNullOffNewAttrToAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        final String NON_PRESENT_ATTRIBUTE_KEY = "beach-type";
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_LIST, NON_PRESENT_ATTRIBUTE_KEY);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0)
                .assertAttributeExists("CSVData");
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS)
                .get(0).assertAttributeEquals("CSVData","");
    }

    @Test
    public void testAttrListNoCoreNullOffNewAttrToContent() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        //set the destination of the csv string to be an attribute
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        //use only one attribute, which does not exists, as the list of attributes to convert to csv
        final String NON_PRESENT_ATTRIBUTE_KEY = "beach-type";
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_LIST, NON_PRESENT_ATTRIBUTE_KEY);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0)
                .assertAttributeExists("CSVData");
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS)
                .get(0).assertAttributeEquals("CSVData","");
    }

    @Test
    public void testAttrListNoCoreNullOffTwoNewAttrToAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        final String NON_PRESENT_ATTRIBUTE_KEY = "beach-type,beach-length";
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_LIST, NON_PRESENT_ATTRIBUTE_KEY);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0)
                .assertAttributeExists("CSVData");
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS)
                .get(0).assertAttributeEquals("CSVData",",");
    }

    @Test
    public void testAttrListNoCoreNullTwoNewAttrToAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "true");

        final String NON_PRESENT_ATTRIBUTE_KEY = "beach-type,beach-length";
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_LIST, NON_PRESENT_ATTRIBUTE_KEY);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0)
                .assertAttributeExists("CSVData");
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS)
                .get(0).assertAttributeEquals("CSVData","null,null");
    }

    @Test
    public void testNoAttrListNoCoreNullOffToAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        //set the destination of the csv string to be an attribute
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0)
                .assertAttributeExists("CSVData");
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS)
                .get(0).assertAttributeEquals("CSVData","");
    }

    @Test
    public void testNoAttrListNoCoreNullToAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "true");
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0)
                .assertAttributeExists("CSVData");
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS)
                .get(0).assertAttributeEquals("CSVData","");
    }


    @Test
    public void testNoAttrListCoreNullOffToContent() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_OVERWRITE_CONTENT);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "true");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        final Map<String, String> attrs = new HashMap<String, String>(){{
            put("beach-name", "Malibu Beach");
            put("beach-location", "California, US");
            put("beach-endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");
        }};

        testRunner.enqueue(new byte[0], attrs);
        testRunner.run();

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS);

        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);

        MockFlowFile flowFile = flowFilesForRelationship.get(0);

        assertEquals(OUTPUT_MIME_TYPE, flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        final byte[] contentData = testRunner.getContentAsByteArray(flowFile);

        final String contentDataString = new String(contentData, "UTF-8");

        Set<String> contentValues = new HashSet<>(getStrings(contentDataString));

        assertEquals(6, contentValues.size());

        assertTrue(contentValues.contains("Malibu Beach"));
        assertTrue(contentValues.contains("\"California, US\""));
        assertTrue(contentValues.contains("\"This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim\""));
        assertTrue(contentValues.contains(flowFile.getAttribute("filename")));
        assertTrue(contentValues.contains(flowFile.getAttribute("path")));
        assertTrue(contentValues.contains(flowFile.getAttribute("uuid")));
    }

    @Test
    public void testNoAttrListCoreNullOffToAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "true");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        Map<String, String> attrs = new HashMap<String, String>(){{
            put("beach-name", "Malibu Beach");
            put("beach-location", "California, US");
            put("beach-endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");
        }};

        testRunner.enqueue(new byte[0], attrs);
        testRunner.run();

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS);

        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);

        MockFlowFile flowFile = flowFilesForRelationship.get(0);

        assertNull(flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        final String attributeData = flowFile.getAttribute(OUTPUT_ATTRIBUTE_NAME);

        Set<String> csvAttributeValues = new HashSet<>(getStrings(attributeData));

        assertEquals(6, csvAttributeValues.size());

        assertTrue(csvAttributeValues.contains("Malibu Beach"));
        assertTrue(csvAttributeValues.contains("\"California, US\""));
        assertTrue(csvAttributeValues.contains("\"This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim\""));

        assertTrue(csvAttributeValues.contains(flowFile.getAttribute("filename")));
        assertTrue(csvAttributeValues.contains(flowFile.getAttribute("path")));
        assertTrue(csvAttributeValues.contains(flowFile.getAttribute("uuid")));
    }

    @Test
    public void testNoAttrListNoCoreNullOffToContent() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_OVERWRITE_CONTENT);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        Map<String, String> attrs = new HashMap<String, String>(){{
            put("beach-name", "Malibu Beach");
            put("beach-location", "California, US");
            put("beach-endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");
        }};

        testRunner.enqueue(new byte[0], attrs);
        testRunner.run();

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS);

        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);

        MockFlowFile flowFile = flowFilesForRelationship.get(0);

        assertEquals(OUTPUT_MIME_TYPE, flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        final byte[] contentData = testRunner.getContentAsByteArray(flowFile);

        final String contentDataString = new String(contentData, "UTF-8");
        Set<String> contentValues = new HashSet<>(getStrings(contentDataString));

        assertEquals(3, contentValues.size());

        assertTrue(contentValues.contains("Malibu Beach"));
        assertTrue(contentValues.contains("\"California, US\""));
        assertTrue(contentValues.contains("\"This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim\""));

    }


    @Test
    public void testAttrListNoCoreNullOffToAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_LIST, "beach-name,beach-location,beach-endorsement");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        Map<String, String> attrs = new HashMap<String, String>(){{
            put("beach-name", "Malibu Beach");
            put("beach-location", "California, US");
            put("beach-endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");
            put("attribute-should-be-eliminated", "This should not be in CSVAttribute!");
        }};

        testRunner.enqueue(new byte[0], attrs);
        testRunner.run();

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS);

        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);

        MockFlowFile flowFile = flowFilesForRelationship.get(0);

        assertNull(flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        final String attributeData = flowFile.getAttribute(OUTPUT_ATTRIBUTE_NAME);

        Set<String> CSVDataValues = new HashSet<>(getStrings(attributeData));

        assertEquals(3, CSVDataValues.size());

        assertTrue(CSVDataValues.contains("Malibu Beach"));
        assertTrue(CSVDataValues.contains("\"California, US\""));
        assertTrue(CSVDataValues.contains("\"This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim\""));

    }

    @Test
    public void testAttrListCoreNullOffToAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "true");
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_LIST, "beach-name,beach-location,beach-endorsement");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        Map<String, String> attrs = new HashMap<String, String>(){{
            put("beach-name", "Malibu Beach");
            put("beach-location", "California, US");
            put("beach-endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");
            put("attribute-should-be-eliminated", "This should not be in CSVData!");
        }};

        testRunner.enqueue(new byte[0], attrs);
        testRunner.run();

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS);

        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);

        MockFlowFile flowFile = flowFilesForRelationship.get(0);

        assertNull(flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        final String attributeData = flowFile.getAttribute(OUTPUT_ATTRIBUTE_NAME);

        Set<String> CSVDataValues = new HashSet<>(getStrings(attributeData));

        assertEquals(6, CSVDataValues.size());

        assertTrue(CSVDataValues.contains("Malibu Beach"));
        assertTrue(CSVDataValues.contains("\"California, US\""));
        assertTrue(CSVDataValues.contains("\"This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim\""));

        assertTrue(CSVDataValues.contains(flowFile.getAttribute("filename")));
        assertTrue(CSVDataValues.contains(flowFile.getAttribute("path")));
        assertTrue(CSVDataValues.contains(flowFile.getAttribute("uuid")));
    }

    @Test
    public void testAttrListNoCoreNullOffOverrideCoreByAttrListToAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_LIST, "beach-name,beach-location,beach-endorsement,uuid");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        Map<String, String> attrs = new HashMap<String, String>(){{
            put("beach-name", "Malibu Beach");
            put("beach-location", "California, US");
            put("beach-endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");
            put("attribute-should-be-eliminated", "This should not be in CSVData!");
        }};

        testRunner.enqueue(new byte[0], attrs);
        testRunner.run();

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS);

        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);

        MockFlowFile flowFile = flowFilesForRelationship.get(0);

        assertNull(flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        final String attributeData = flowFile.getAttribute(OUTPUT_ATTRIBUTE_NAME);

        Set<String> CSVDataValues = new HashSet<>(getStrings(attributeData));

        assertEquals(4, CSVDataValues.size());

        assertTrue(CSVDataValues.contains("Malibu Beach"));
        assertTrue(CSVDataValues.contains("\"California, US\""));
        assertTrue(CSVDataValues.contains("\"This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim\""));


        assertTrue(!CSVDataValues.contains(flowFile.getAttribute("filename")));
        assertTrue(!CSVDataValues.contains(flowFile.getAttribute("path")));
        assertTrue(CSVDataValues.contains(flowFile.getAttribute("uuid")));
    }

    @Test
    public void testAttrListFromExpCoreNullOffToAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "true");
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_LIST, "${myAttribs}");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        Map<String, String> attrs = new HashMap<String, String>(){{
            put("beach-name", "Malibu Beach");
            put("beach-location", "California, US");
            put("beach-endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");
            put("attribute-should-be-eliminated", "This should not be in CSVData!");
            put("myAttribs", "beach-name,beach-location,beach-endorsement");
        }};

        testRunner.enqueue(new byte[0], attrs);
        testRunner.run();

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS);

        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);

        //Test flow file 0 with ATTRIBUTE_LIST populated from expression language
        MockFlowFile flowFile = flowFilesForRelationship.get(0);

        assertNull(flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        String attributeData = flowFile.getAttribute(OUTPUT_ATTRIBUTE_NAME);

        Set<String> CSVDataValues = new HashSet<>(getStrings(attributeData));

        assertEquals(6, CSVDataValues.size());

        assertTrue(CSVDataValues.contains("Malibu Beach"));
        assertTrue(CSVDataValues.contains("\"California, US\""));
        assertTrue(CSVDataValues.contains("\"This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim\""));

        assertTrue(CSVDataValues.contains(flowFile.getAttribute("filename")));
        assertTrue(CSVDataValues.contains(flowFile.getAttribute("path")));
        assertTrue(CSVDataValues.contains(flowFile.getAttribute("uuid")));

        //Test flow file 1 with ATTRIBUTE_LIST populated from expression language containing commas (output should be he same)
        flowFile = flowFilesForRelationship.get(0);

        assertNull(flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        attributeData = flowFile.getAttribute(OUTPUT_ATTRIBUTE_NAME);

        CSVDataValues = new HashSet<>(getStrings(attributeData));

        assertEquals(6, CSVDataValues.size());

        assertTrue(CSVDataValues.contains("Malibu Beach"));
        assertTrue(CSVDataValues.contains("\"California, US\""));
        assertTrue(CSVDataValues.contains("\"This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim\""));

        assertTrue(CSVDataValues.contains(flowFile.getAttribute("filename")));
        assertTrue(CSVDataValues.contains(flowFile.getAttribute("path")));
        assertTrue(CSVDataValues.contains(flowFile.getAttribute("uuid")));

    }

    @Test
    public void testAttrListWithCommasInNameFromExpCoreNullOffToAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "true");
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_LIST, "${myAttribs}");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");


        Map<String, String> attrsCommaInName = new HashMap<String, String>(){{
            put("beach,name", "Malibu Beach");
            put("beach,location", "California, US");
            put("beach,endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");
            put("attribute-should-be-eliminated", "This should not be in CSVData!");
            put("myAttribs", "\"beach,name\",\"beach,location\",\"beach,endorsement\"");
        }};

        testRunner.enqueue(new byte[0], attrsCommaInName);
        testRunner.run();

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS);

        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);

        //Test flow file 0 with ATTRIBUTE_LIST populated from expression language
        MockFlowFile flowFile = flowFilesForRelationship.get(0);

        assertNull(flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        String attributeData = flowFile.getAttribute(OUTPUT_ATTRIBUTE_NAME);

        Set<String> CSVDataValues = new HashSet<>(getStrings(attributeData));

        assertEquals(6, CSVDataValues.size());

        assertTrue(CSVDataValues.contains("Malibu Beach"));
        assertTrue(CSVDataValues.contains("\"California, US\""));
        assertTrue(CSVDataValues.contains("\"This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim\""));

        assertTrue(CSVDataValues.contains(flowFile.getAttribute("filename")));
        assertTrue(CSVDataValues.contains(flowFile.getAttribute("path")));
        assertTrue(CSVDataValues.contains(flowFile.getAttribute("uuid")));

        //Test flow file 1 with ATTRIBUTE_LIST populated from expression language containing commas (output should be he same)
        flowFile = flowFilesForRelationship.get(0);

        assertNull(flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        attributeData = flowFile.getAttribute(OUTPUT_ATTRIBUTE_NAME);

        CSVDataValues = new HashSet<>(getStrings(attributeData));

        assertEquals(6, CSVDataValues.size());

        assertTrue(CSVDataValues.contains("Malibu Beach"));
        assertTrue(CSVDataValues.contains("\"California, US\""));
        assertTrue(CSVDataValues.contains("\"This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim\""));

        assertTrue(CSVDataValues.contains(flowFile.getAttribute("filename")));
        assertTrue(CSVDataValues.contains(flowFile.getAttribute("path")));
        assertTrue(CSVDataValues.contains(flowFile.getAttribute("uuid")));

    }


    @Test
    public void testAttrListFromExpNoCoreNullOffOverrideCoreByAttrListToAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_LIST, "${myAttribs}");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        Map<String, String> attrs = new HashMap<String, String>(){{
            put("beach-name", "Malibu Beach");
            put("beach-location", "California, US");
            put("beach-endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");
            put("attribute-should-be-eliminated", "This should not be in CSVData!");
            put("myAttribs", "beach-name,beach-location,beach-endorsement");
        }};

        testRunner.enqueue(new byte[0], attrs);
        testRunner.run();

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS);

        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);

        MockFlowFile flowFile = flowFilesForRelationship.get(0);

        assertNull(flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        final String attributeData = flowFile.getAttribute(OUTPUT_ATTRIBUTE_NAME);

        Set<String> CSVDataValues = new HashSet<>(getStrings(attributeData));

        assertEquals(3, CSVDataValues.size());

        assertTrue(CSVDataValues.contains("Malibu Beach"));
        assertTrue(CSVDataValues.contains("\"California, US\""));
        assertTrue(CSVDataValues.contains("\"This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim\""));


        assertTrue(!CSVDataValues.contains(flowFile.getAttribute("filename")));
        assertTrue(!CSVDataValues.contains(flowFile.getAttribute("path")));
        assertTrue(!CSVDataValues.contains(flowFile.getAttribute("uuid")));
    }

    @Test
    public void testAttributesRegex() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_REGEX, "${myRegEx}");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        Map<String, String> attrs = new HashMap<String, String>(){{
            put("beach-name", "Malibu Beach");
            put("beach-location", "California, US");
            put("beach-endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");
            put("attribute-should-be-eliminated", "This should not be in CSVData!");
            put("myRegEx", "beach-.*");
        }};

        testRunner.enqueue(new byte[0], attrs);
        testRunner.run();

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS);

        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);

        MockFlowFile flowFile = flowFilesForRelationship.get(0);

        assertNull(flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        final String attributeData = flowFile.getAttribute(OUTPUT_ATTRIBUTE_NAME);

        Set<String> CSVDataValues = new HashSet<>(getStrings(attributeData));

        assertEquals(3, CSVDataValues.size());

        assertTrue(CSVDataValues.contains("Malibu Beach"));
        assertTrue(CSVDataValues.contains("\"California, US\""));
        assertTrue(CSVDataValues.contains("\"This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim\""));


        assertTrue(!CSVDataValues.contains(flowFile.getAttribute("filename")));
        assertTrue(!CSVDataValues.contains(flowFile.getAttribute("path")));
        assertTrue(!CSVDataValues.contains(flowFile.getAttribute("uuid")));
    }

    @Test
    public void testAttributesRegexAndList() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_REGEX, "${myRegEx}");
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_LIST, "moreInfo1,moreInfo2");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        Map<String, String> attrs = new HashMap<String, String>(){{
            put("beach-name", "Malibu Beach");
            put("beach-location", "California, US");
            put("beach-endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");
            put("attribute-should-be-eliminated", "This should not be in CSVData!");
            put("myRegEx", "beach-.*");
            put("moreInfo1", "A+ Rating");
            put("moreInfo2", "Avg Temp: 61f");
        }};

        testRunner.enqueue(new byte[0], attrs);
        testRunner.run();

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS);

        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);

        MockFlowFile flowFile = flowFilesForRelationship.get(0);

        assertNull(flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        final String attributeData = flowFile.getAttribute(OUTPUT_ATTRIBUTE_NAME);

        Set<String> CSVDataValues = new HashSet<>(getStrings(attributeData));

        assertEquals(5, CSVDataValues.size());

        assertTrue(CSVDataValues.contains("Malibu Beach"));
        assertTrue(CSVDataValues.contains("\"California, US\""));
        assertTrue(CSVDataValues.contains("\"This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim\""));
        assertTrue(CSVDataValues.contains("A+ Rating"));
        assertTrue(CSVDataValues.contains("Avg Temp: 61f"));

        assertTrue(!CSVDataValues.contains(flowFile.getAttribute("filename")));
        assertTrue(!CSVDataValues.contains(flowFile.getAttribute("path")));
        assertTrue(!CSVDataValues.contains(flowFile.getAttribute("uuid")));
    }


    @Test
    public void testSchemaToAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");
        testRunner.setProperty(AttributesToCSV.INCLUDE_SCHEMA, "true");
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_REGEX, "beach-.*");

        Map<String, String> attrs = new HashMap<String, String>(){{
            put("beach-name", "Malibu Beach");
            put("beach-location", "California, US");
            put("attribute-should-be-eliminated", "This should not be in CSVData!");
        }};

        testRunner.enqueue(new byte[0], attrs);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0)
                .assertAttributeExists("CSVData");
        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0)
                .assertAttributeExists("CSVSchema");
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS)
                .get(0).assertAttributeEquals("CSVData","Malibu Beach,\"California, US\"");
        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS)
                .get(0).assertAttributeEquals("CSVSchema","beach-name,beach-location");
    }

    @Test
    public void testSchemaToContent() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        //set the destination of the csv string to be an attribute
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_OVERWRITE_CONTENT);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");
        testRunner.setProperty(AttributesToCSV.INCLUDE_SCHEMA, "true");
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_REGEX, "beach-.*");

        Map<String, String> attrs = new HashMap<String, String>(){{
            put("beach-name", "Malibu Beach");
            put("beach-location", "California, US");
            put("attribute-should-be-eliminated", "This should not be in CSVData!");
        }};

        testRunner.enqueue(new byte[0], attrs);
        testRunner.run();

        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0);
        flowFile.assertAttributeNotExists("CSVData");
        flowFile.assertAttributeNotExists("CSVSchema");

        final byte[] contentData = testRunner.getContentAsByteArray(flowFile);

        final String contentDataString = new String(contentData, "UTF-8");
        assertEquals(contentDataString.split(newline)[0], "beach-name,beach-location");
        assertEquals(contentDataString.split(newline)[1], "Malibu Beach,\"California, US\"");
    }


    @Test
    public void testSchemaWithCoreAttribuesToAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "true");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");
        testRunner.setProperty(AttributesToCSV.INCLUDE_SCHEMA, "true");
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_REGEX, "beach-.*");

        Map<String, String> attrs = new HashMap<String, String>(){{
            put("beach-name", "Malibu Beach");
            put("beach-location", "California, US");
            put("attribute-should-be-eliminated", "This should not be in CSVData!");
        }};

        testRunner.enqueue(new byte[0], attrs);
        testRunner.run();

        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0);
        flowFile.assertAttributeExists("CSVData");
        flowFile.assertAttributeExists("CSVSchema");

        final String path = flowFile.getAttribute("path");
        final String filename = flowFile.getAttribute("filename");
        final String uuid = flowFile.getAttribute("uuid");

        flowFile.assertAttributeEquals("CSVData", "Malibu Beach,\"California, US\"," + path + "," + filename + "," + uuid);
        flowFile.assertAttributeEquals("CSVSchema","beach-name,beach-location,path,filename,uuid");
    }

    @Test
    public void testSchemaWithCoreAttribuesToContent() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        //set the destination of the csv string to be an attribute
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_OVERWRITE_CONTENT);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "true");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");
        testRunner.setProperty(AttributesToCSV.INCLUDE_SCHEMA, "true");
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_REGEX, "beach-.*");

        Map<String, String> attrs = new HashMap<String, String>(){{
            put("beach-name", "Malibu Beach");
            put("beach-location", "California, US");
            put("attribute-should-be-eliminated", "This should not be in CSVData!");
        }};

        testRunner.enqueue(new byte[0], attrs);
        testRunner.run();

        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0);
        flowFile.assertAttributeNotExists("CSVData");
        flowFile.assertAttributeNotExists("CSVSchema");

        final String path = flowFile.getAttribute("path");
        final String filename = flowFile.getAttribute("filename");
        final String uuid = flowFile.getAttribute("uuid");

        final byte[] contentData = testRunner.getContentAsByteArray(flowFile);

        final String contentDataString = new String(contentData, "UTF-8");
        assertEquals(contentDataString.split(newline)[0], "beach-name,beach-location,path,filename,uuid");
        assertEquals(contentDataString.split(newline)[1], "Malibu Beach,\"California, US\"," + path + "," + filename + "," + uuid);
    }
    private List<String> getStrings(String sdata) {
        return Splitter.on(Pattern.compile(SPLIT_REGEX)).splitToList(sdata);
    }

}
