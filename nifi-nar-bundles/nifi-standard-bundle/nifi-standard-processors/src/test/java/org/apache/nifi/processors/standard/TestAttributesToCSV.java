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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import static org.junit.Assert.*;


public class TestAttributesToCSV {

    private static final String OUTPUT_NEW_ATTRIBUTE = "flowfile-attribute";
    private static final String OUTPUT_OVERWRITE_CONTENT = "flowfile-content";
    private static final String OUTPUT_ATTRIBUTE_NAME = "CSVAttributes";
    private static final String OUTPUT_SEPARATOR = ",";
    private static final String OUTPUT_MIME_TYPE = "text/csv";
    private static final String SPLIT_REGEX = OUTPUT_SEPARATOR + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    @Test
    public void testAttrListNoCoreNullOffNewAttrToAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        final String NON_PRESENT_ATTRIBUTE_KEY = "beach-type";
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_LIST, NON_PRESENT_ATTRIBUTE_KEY);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0)
                .assertAttributeExists("CSVAttributes");
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS)
                .get(0).assertAttributeEquals("CSVAttributes","");
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

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0)
                .assertAttributeExists("CSVAttributes");
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS)
                .get(0).assertAttributeEquals("CSVAttributes","");
    }

    @Test
    public void testAttrListNoCoreNullOffTwoNewAttrToAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        final String NON_PRESENT_ATTRIBUTE_KEY = "beach-type,beach-length";
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_LIST, NON_PRESENT_ATTRIBUTE_KEY);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0)
                .assertAttributeExists("CSVAttributes");
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS)
                .get(0).assertAttributeEquals("CSVAttributes",",");
    }

    @Test
    public void testAttrListNoCoreNullTwoNewAttrToAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "true");

        final String NON_PRESENT_ATTRIBUTE_KEY = "beach-type,beach-length";
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_LIST, NON_PRESENT_ATTRIBUTE_KEY);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0)
                .assertAttributeExists("CSVAttributes");
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS)
                .get(0).assertAttributeEquals("CSVAttributes","null,null");
    }

    @Test
    public void testNoAttrListNoCoreNullOffToAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        //set the destination of the csv string to be an attribute
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");


        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0)
                .assertAttributeExists("CSVAttributes");
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS)
                .get(0).assertAttributeEquals("CSVAttributes","");
    }

    @Test
    public void testNoAttrListNoCoreNullToAttribute() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "true");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS).get(0)
                .assertAttributeExists("CSVAttributes");
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);
        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);

        testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS)
                .get(0).assertAttributeEquals("CSVAttributes","");
    }


    @Test
    public void testNoAttrListCoreNullOffToContent() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_OVERWRITE_CONTENT);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "true");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        ff = session.putAttribute(ff, "beach-name", "Malibu Beach");
        ff = session.putAttribute(ff, "beach-location", "California, US");
        ff = session.putAttribute(ff, "beach-endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");

        testRunner.enqueue(ff);
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

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        ff = session.putAttribute(ff, "beach-name", "Malibu Beach");
        ff = session.putAttribute(ff, "beach-location", "California, US");
        ff = session.putAttribute(ff, "beach-endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");

        testRunner.enqueue(ff);
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

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        ff = session.putAttribute(ff, "beach-name", "Malibu Beach");
        ff = session.putAttribute(ff, "beach-location", "California, US");
        ff = session.putAttribute(ff, "beach-endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");

        testRunner.enqueue(ff);
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

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        ff = session.putAttribute(ff, "beach-name", "Malibu Beach");
        ff = session.putAttribute(ff, "beach-location", "California, US");
        ff = session.putAttribute(ff, "beach-endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");
        ff = session.putAttribute(ff, "attribute-should-be-eliminated", "This should not be in CSVAttribute!");

        testRunner.enqueue(ff);
        testRunner.run();

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS);

        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);

        MockFlowFile flowFile = flowFilesForRelationship.get(0);

        assertNull(flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        final String attributeData = flowFile.getAttribute(OUTPUT_ATTRIBUTE_NAME);

        Set<String> csvAttributesValues = new HashSet<>(getStrings(attributeData));

        assertEquals(3, csvAttributesValues.size());

        assertTrue(csvAttributesValues.contains("Malibu Beach"));
        assertTrue(csvAttributesValues.contains("\"California, US\""));
        assertTrue(csvAttributesValues.contains("\"This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim\""));

    }

    @Test
    public void testAttrListCoreNullOffToAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "true");
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_LIST, "beach-name,beach-location,beach-endorsement");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");


        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        ff = session.putAttribute(ff, "beach-name", "Malibu Beach");
        ff = session.putAttribute(ff, "beach-location", "California, US");
        ff = session.putAttribute(ff, "beach-endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");
        ff = session.putAttribute(ff, "attribute-should-be-eliminated", "This should not be in CSVAttribute!");

        testRunner.enqueue(ff);
        testRunner.run();

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS);

        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);

        MockFlowFile flowFile = flowFilesForRelationship.get(0);

        assertNull(flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        final String attributeData = flowFile.getAttribute(OUTPUT_ATTRIBUTE_NAME);

        Set<String> csvAttributesValues = new HashSet<>(getStrings(attributeData));

        assertEquals(6, csvAttributesValues.size());

        assertTrue(csvAttributesValues.contains("Malibu Beach"));
        assertTrue(csvAttributesValues.contains("\"California, US\""));
        assertTrue(csvAttributesValues.contains("\"This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim\""));

        assertTrue(csvAttributesValues.contains(flowFile.getAttribute("filename")));
        assertTrue(csvAttributesValues.contains(flowFile.getAttribute("path")));
        assertTrue(csvAttributesValues.contains(flowFile.getAttribute("uuid")));
    }

    @Test
    public void testAttrListNoCoreNullOffOverrideCoreByAttrListToAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new AttributesToCSV());
        testRunner.setProperty(AttributesToCSV.DESTINATION, OUTPUT_NEW_ATTRIBUTE);
        testRunner.setProperty(AttributesToCSV.INCLUDE_CORE_ATTRIBUTES, "false");
        testRunner.setProperty(AttributesToCSV.ATTRIBUTES_LIST, "beach-name,beach-location,beach-endorsement,uuid");
        testRunner.setProperty(AttributesToCSV.NULL_VALUE_FOR_EMPTY_STRING, "false");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        ff = session.putAttribute(ff, "beach-name", "Malibu Beach");
        ff = session.putAttribute(ff, "beach-location", "California, US");
        ff = session.putAttribute(ff, "beach-endorsement", "This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim");
        ff = session.putAttribute(ff, "attribute-should-be-eliminated", "This should not be in CSVAttribute!");

        testRunner.enqueue(ff);
        testRunner.run();

        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(AttributesToCSV.REL_SUCCESS);

        testRunner.assertTransferCount(AttributesToCSV.REL_FAILURE, 0);
        testRunner.assertTransferCount(AttributesToCSV.REL_SUCCESS, 1);

        MockFlowFile flowFile = flowFilesForRelationship.get(0);

        assertNull(flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        final String attributeData = flowFile.getAttribute(OUTPUT_ATTRIBUTE_NAME);

        Set<String> csvAttributesValues = new HashSet<>(getStrings(attributeData));

        assertEquals(4, csvAttributesValues.size());

        assertTrue(csvAttributesValues.contains("Malibu Beach"));
        assertTrue(csvAttributesValues.contains("\"California, US\""));
        assertTrue(csvAttributesValues.contains("\"This is our family's favorite beach. We highly recommend it. \n\nThanks, Jim\""));


        assertTrue(!csvAttributesValues.contains(flowFile.getAttribute("filename")));
        assertTrue(!csvAttributesValues.contains(flowFile.getAttribute("path")));
        assertTrue(csvAttributesValues.contains(flowFile.getAttribute("uuid")));
    }

    private List<String> getStrings(String sdata) {
        return Splitter.on(Pattern.compile(SPLIT_REGEX)).splitToList(sdata);
    }

}
